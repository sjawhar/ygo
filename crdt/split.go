package crdt

import (
	"cmp"
	"maps"
	"math"
	"slices"

	"github.com/reearth/ygo/encoding"
)

// This file cuts one V1 update into the series of smaller V1 updates a
// receiver applies in its place, for a transport that must not hand a
// receiver one large update at once (a browser applies one update in one
// task). It works at the struct level, on decodeStructsV1 and
// encodeStructStoreV1 as MergeUpdatesV1 and DiffUpdateV1 do, because the cut
// has to read what each struct names: a split by byte length would leave a
// receiver holding structs that wait on structs still to come.

// splitHeaderBytes is what an update spends outside its structs and
// deletions: the struct group count and the delete set's client count. Each is
// a VarUint of a count the splitter bounds by the bytes it packs, so five
// bytes each is never short.
const splitHeaderBytes = 10

// splitCountBytes is what a count inside an update costs at most: a struct
// group's entry count, and a delete-set client's range count, bounded the same
// way as splitHeaderBytes.
const splitCountBytes = 5

// SplitUpdateV1 returns the series of V1 updates a receiver whose state vector
// is sv applies, in order, to reach what update alone would leave it holding.
//
// Every update but the last integrates on its own: once it is applied the
// receiver holds every struct it carries, waiting for none, and its deletions
// name clocks the receiver holds. The last update carries what no update can
// integrate on its own — the structs update itself leaves waiting for a struct
// it does not carry, which a sender holds parked, and the deletions of clocks
// no struct in the series delivers — so the series leaves the receiver exactly
// what update would have. It is always there, empty when there is nothing
// left, so a caller can send it as the one message that ends a reply.
//
// Each update is at most maxBytes long, with two exceptions: one carrying a
// single struct that is longer on its own, which a V1 update cannot cut
// (see cuttableContent), and the last, whose parked structs cannot be split
// across updates by definition. maxBytes must leave room for one struct;
// below that, each update carries one struct.
func SplitUpdateV1(update []byte, sv StateVector, maxBytes int) ([][]byte, error) {
	perClient, ds, err := buildMergeStore([][]byte{update}, decodeStructsV1)
	if err != nil {
		return nil, err
	}
	s := newUpdateSplitter(perClient, ds, sv, maxBytes)
	s.run()
	return s.out, nil
}

// updateSplitter packs the structs and deletions of one decoded update into
// the updates SplitUpdateV1 returns.
type updateSplitter struct {
	items     map[ClientID][]*Item // each client's structs still to place, clock-ascending
	clients   []ClientID           // the clients of items, descending as the encoder writes them
	ds        DeleteSet
	dsClients []ClientID          // the clients of ds, descending
	dsIdx     map[ClientID]int    // the first range of a client the series has not sent whole
	dsFrom    map[ClientID]uint64 // the clock in that range the series has sent up to
	held      StateVector         // what the receiver holds once every update so far is applied
	maxBytes  int

	piece   map[ClientID][]*Item // the structs of the update being packed
	pieceDS DeleteSet            // its deletions
	base    StateVector          // held when the update began, for the clients in it
	bytes   int                  // what it encodes to, never counted short
	scratch *encoding.Encoder    // reused to measure one struct's encoding

	out [][]byte
}

func newUpdateSplitter(perClient map[ClientID][]*Item, ds DeleteSet, sv StateVector, maxBytes int) *updateSplitter {
	s := &updateSplitter{
		items:    perClient,
		ds:       ds,
		dsIdx:    make(map[ClientID]int, len(ds.clients)),
		dsFrom:   make(map[ClientID]uint64, len(ds.clients)),
		held:     make(StateVector, len(sv)),
		maxBytes: maxBytes,
		scratch:  encoding.NewEncoder(),
	}
	maps.Copy(s.held, sv)
	for c, items := range perClient {
		// A struct the receiver holds whole is not sent, as encodeStructStoreV1
		// leaves it out; dropping it here keeps an update's entry count right
		// and keeps it out of the readiness walk.
		known := s.held.Clock(c)
		for len(items) > 0 && items[0].ID.Clock+items[0].Content.Len() <= known {
			items = items[1:]
		}
		if len(items) == 0 {
			delete(s.items, c)
			continue
		}
		s.items[c] = items
		s.clients = append(s.clients, c)
	}
	for c := range ds.clients {
		s.dsClients = append(s.dsClients, c)
	}
	// Yjs writes clients in descending order, and encodeStructStoreV1 and
	// encodeDeleteSet sort them that way too.
	descending := func(a, b ClientID) int { return cmp.Compare(b, a) }
	slices.SortFunc(s.clients, descending)
	slices.SortFunc(s.dsClients, descending)
	s.newPiece()
	return s
}

// run packs every struct that can integrate on its own into updates, then the
// deletions no update carried with its structs, and ends the series with the
// update that carries what is left.
func (s *updateSplitter) run() {
	// A struct placed in this pass can be what another client's struct waits
	// for, so the walk repeats until a pass places nothing.
	for progress := true; progress; {
		progress = false
		for _, c := range s.clients {
			for s.placeReady(c) {
				progress = true
			}
		}
	}
	s.flush()
	s.emitRemainingDeletes()
	s.out = append(s.out, s.tail())
}

// placeReady places the next struct of c when the receiver can integrate it,
// and reports whether it did.
func (s *updateSplitter) placeReady(c ClientID) bool {
	items := s.items[c]
	if len(items) == 0 || !s.ready(items[0]) {
		return false
	}
	s.place(c)
	return true
}

// ready reports whether it integrates into a receiver holding s.held without
// waiting for a struct that has not arrived: its own client's clocks reach it,
// and the structs its origin, right origin and parent name have arrived. These
// are the dependencies the apply defers an item for — a clock gap and
// missingDep in decodeStructs and integrateGroup, the right origin in
// integrateGroup, and all four again in itemFutureDep, which is what parks it.
func (s *updateSplitter) ready(it *Item) bool {
	if it.ID.Clock > s.held.Clock(it.ID.Client) {
		return false
	}
	if o := it.Origin; o != nil && o.Clock >= s.held.Clock(o.Client) {
		return false
	}
	if o := it.OriginRight; o != nil && o.Clock >= s.held.Clock(o.Client) {
		return false
	}
	// A struct names a parent only when it has neither origin; with one it
	// inherits the parent of the struct its origin names (decodeItem).
	if p := it.parentID; p != nil && it.Parent == nil && p.Clock >= s.held.Clock(p.Client) {
		return false
	}
	return true
}

// place adds the next struct of c to the update being packed, starting a new
// update when it does not fit this one and cutting it when it does not fit an
// update of its own.
func (s *updateSplitter) place(c ClientID) {
	it := s.items[c][0]
	if !s.fits(c, it) {
		if !s.pieceEmpty() {
			s.flush()
		}
		if !s.fits(c, it) {
			s.cut(c, it)
			it = s.items[c][0]
		}
	}
	s.add(c, it)
	s.items[c] = s.items[c][1:]
}

// fits reports whether it still leaves the update being packed within maxBytes.
func (s *updateSplitter) fits(c ClientID, it *Item) bool {
	return s.bytes+s.structCost(c, it) <= s.maxBytes
}

// structCost is what it adds to the update being packed: the struct, and the
// group header when it is the first struct of c in that update.
func (s *updateSplitter) structCost(c ClientID, it *Item) int {
	offset := s.offsetIn(it)
	n := s.structBytes(it, offset)
	if len(s.piece[c]) == 0 {
		n += splitCountBytes + varUintLen(uint64(c)) + varUintLen(it.ID.Clock+offset)
	}
	return n
}

// offsetIn is the clocks of it the receiver already holds, which the update
// leaves out, as encodeStructStoreV1 cuts the first struct of a group at the
// receiver's clock. It is zero for every struct but the first of a client.
func (s *updateSplitter) offsetIn(it *Item) uint64 {
	if held := s.held.Clock(it.ID.Client); held > it.ID.Clock {
		return held - it.ID.Clock
	}
	return 0
}

// structBytes is what it encodes to past offset.
func (s *updateSplitter) structBytes(it *Item, offset uint64) int {
	s.scratch.Reset()
	encodeItem(s.scratch, it, offset)
	return len(s.scratch.Bytes())
}

// add puts it in the update being packed, advances what the receiver holds,
// and takes the deletions of the clocks it delivers, so the update that brings
// a struct brings its deletion: a receiver never renders deleted content it is
// about to be told is gone.
func (s *updateSplitter) add(c ClientID, it *Item) {
	if len(s.piece[c]) == 0 {
		s.base[c] = s.held.Clock(c)
	}
	s.bytes += s.structCost(c, it)
	s.piece[c] = append(s.piece[c], it)
	s.held[c] = it.ID.Clock + it.Content.Len()
	s.takeDeletes(c, s.held[c])
}

// cut shortens the struct at the head of c's queue to what fits an update of
// its own, leaving the rest of it in the queue behind the part that fits. A
// struct a V1 update cannot cut is left whole, and the update that carries it
// passes maxBytes.
func (s *updateSplitter) cut(c ClientID, it *Item) {
	offset := s.offsetIn(it)
	budget := s.maxBytes - s.bytes - splitCountBytes - varUintLen(uint64(c)) - varUintLen(it.ID.Clock+offset)
	clocks := s.fittingClocks(it, offset, budget)
	if clocks == 0 {
		return
	}
	s.items[c] = slices.Insert(s.items[c], 1, splitItemAt(it, offset+clocks))
}

// fittingClocks returns how many of the clocks of it past offset encode within
// budget, or 0 when it cannot be cut that small: content a V1 update cannot
// cut, one clock left, or not one clock small enough.
//
// The search is on the encoding's length alone, which grows with the clocks
// cut, and the answer is then moved back to where a character starts. Testing
// both at once would not work: where a cut falls inside a character the test
// fails whatever its length, and the halving walks through such lengths, so
// the search would settle far below the longest that fits or at nothing.
func (s *updateSplitter) fittingClocks(it *Item, offset uint64, budget int) uint64 {
	if budget <= 0 || !cuttableContent(it.Content) {
		return 0
	}
	n := it.Content.Len() - offset
	if n <= 1 {
		return 0
	}
	lo, hi := uint64(0), n-1
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if s.cutBytes(it, offset, mid) <= budget {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	// A shorter cut encodes no longer, so moving back keeps the fit.
	for lo > 0 && !cutsCleanly(it.Content, offset+lo) {
		lo--
	}
	return lo
}

// cutBytes is what it encodes to when cut to clocks clocks past offset.
func (s *updateSplitter) cutBytes(it *Item, offset, clocks uint64) int {
	head := *it
	head.Content = it.Content.Copy()
	_ = head.Content.Splice(offset + clocks)
	return s.structBytes(&head, offset)
}

// takeDeletes adds to the update being packed the delete ranges of c the
// series has not sent that name a clock below end, cutting a range at end and
// stopping at the first that does not fit what the update has left, and
// returns how many it took. It takes one whatever the room left when the
// update is empty, so a caller that flushes on nothing taken makes progress.
func (s *updateSplitter) takeDeletes(c ClientID, end uint64) int {
	ranges := s.ds.clients[c]
	idx, from := s.dsIdx[c], s.dsFrom[c]
	group := 0
	if len(s.pieceDS.clients[c]) == 0 {
		group = splitCountBytes + varUintLen(uint64(c))
	}
	taken, cost := 0, 0
	for ; idx < len(ranges); idx++ {
		r := ranges[idx]
		rangeEnd := r.Clock + r.Len
		// from names a clock inside ranges[idx] alone; a later range starts at
		// its own clock.
		start := max(r.Clock, from)
		if start >= rangeEnd {
			from = 0
			continue
		}
		if start >= end {
			break
		}
		stop := min(rangeEnd, end)
		n := varUintLen(start) + varUintLen(stop-start)
		if s.bytes+group+cost+n > s.maxBytes && (taken != 0 || !s.pieceEmpty()) {
			break
		}
		cost += n
		taken++
		s.pieceDS.clients[c] = append(s.pieceDS.clients[c], DeleteRange{Clock: start, Len: stop - start})
		if stop < rangeEnd {
			from = stop
			break
		}
		from = 0
	}
	if taken > 0 {
		s.bytes += group + cost
	}
	s.dsIdx[c], s.dsFrom[c] = idx, from
	return taken
}

// emitRemainingDeletes packs the delete ranges no update carried with its
// structs into updates of their own, after every struct the series sends: the
// ranges of a client whose structs the receiver already held have no update
// to ride in, and neither have those naming a clock past a client's structs,
// which park on the receiver as they are parked on the sender. What is left
// over stays in the update being packed, which the series ends with.
func (s *updateSplitter) emitRemainingDeletes() {
	for _, c := range s.dsClients {
		for s.dsIdx[c] < len(s.ds.clients[c]) {
			if s.takeDeletes(c, math.MaxUint64) == 0 {
				s.flush()
			}
		}
	}
}

// flush ends the update being packed, adding it to the series unless it is
// empty, and begins the next one.
func (s *updateSplitter) flush() {
	if s.pieceEmpty() {
		return
	}
	s.out = append(s.out, encodeStructStoreV1(s.piece, s.pieceDS, s.base))
	s.newPiece()
}

// newPiece begins an update with nothing in it.
func (s *updateSplitter) newPiece() {
	s.piece = make(map[ClientID][]*Item)
	s.pieceDS = newDeleteSet()
	s.base = make(StateVector)
	s.bytes = splitHeaderBytes
}

// pieceEmpty reports whether the update being packed carries nothing.
func (s *updateSplitter) pieceEmpty() bool {
	return len(s.piece) == 0 && len(s.pieceDS.clients) == 0
}

// tail is the update the series ends with: the structs no update could carry
// on its own, which the receiver parks as the sender does, and the deletions
// left over. Its structs are above what the receiver holds, so it needs no
// state vector to cut them at.
func (s *updateSplitter) tail() []byte {
	var left map[ClientID][]*Item
	for _, c := range s.clients {
		if rest := s.items[c]; len(rest) > 0 {
			if left == nil {
				left = make(map[ClientID][]*Item)
			}
			left[c] = rest
		}
	}
	return encodeStructStoreV1(left, s.pieceDS, nil)
}

// splitItemAt cuts it at its offset'th clock: it keeps the clocks before
// offset and the returned struct holds the rest, anchored on it, which is the
// pair Yjs splitItem leaves in a store and encodeStateAsUpdate then writes.
func splitItemAt(it *Item, offset uint64) *Item {
	return &Item{
		ID:          ID{Client: it.ID.Client, Clock: it.ID.Clock + offset},
		Origin:      &ID{Client: it.ID.Client, Clock: it.ID.Clock + offset - 1},
		OriginRight: it.OriginRight,
		Parent:      it.Parent,
		ParentSub:   it.ParentSub,
		parentID:    it.parentID,
		Deleted:     it.Deleted,
		Content:     it.Content.Splice(offset),
	}
}

// cuttableContent reports whether a V1 update can carry part of the clocks of
// c as a struct of its own. Splice panics on every content kind that occupies
// one clock (ContentBinary, ContentEmbed, ContentFormat, ContentType,
// ContentDoc, ContentMove), which is nothing to cut anyway, and a deleted run
// encodes as its length alone, which no frame is too small for.
func cuttableContent(c Content) bool {
	switch c.(type) {
	case *ContentString, *ContentAny, *ContentJSON:
		return true
	}
	return false
}

// cutsCleanly reports whether cutting c at offset leaves the two parts holding
// between them exactly what c holds. Splice cuts UTF-16 code units, as
// JavaScript slices a string, so an offset inside a supplementary character
// leaves a replacement character on either side of the cut, each one unit long
// (splitUTF16) — a cut that changes the text, which a transport must not make.
// The cut is clean when offset is where a character starts, which is why this
// counts the units of each character rather than asking splitUTF16, whose
// halves count to offset either way.
func cutsCleanly(c Content, offset uint64) bool {
	s, ok := c.(*ContentString)
	if !ok {
		return true
	}
	units := uint64(0)
	for _, r := range s.Str {
		if units == offset {
			return true
		}
		if r > 0xFFFF {
			units += 2
		} else {
			units++
		}
		if units > offset {
			return false
		}
	}
	return units == offset
}

// varUintLen is how many bytes Encoder.WriteVarUint writes for v: seven bits
// of it per byte.
func varUintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
