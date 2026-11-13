package crdt

import (
	"cmp"
	"math"
	"slices"
	"sort"
	"unicode/utf8"

	"github.com/reearth/ygo/encoding"
)

// This file cuts one V1 update into the series of smaller V1 updates a
// receiver applies in its place, for a transport that must not hand a
// receiver one large update at once (a browser applies one update in one
// task). It works at the struct level, because the cut has to read what each
// struct names: a split by byte length would leave a receiver holding structs
// that wait on structs still to come. SplitUpdateV1 reads the structs of an
// encoded update with decodeStructsV1, as MergeUpdatesV1 and DiffUpdateV1 do;
// SplitStateAsUpdateV1 reads a document's own, so that splitting a state costs
// what encoding it does.

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
	queues := make([]splitQueue, 0, len(perClient))
	for c, items := range perClient {
		queues = appendUnsent(queues, c, items, sv)
	}
	s := newUpdateSplitter(queues, ds, sv, maxBytes, false)
	s.run()
	return s.out, nil
}

// SplitStateAsUpdateV1 returns what SplitUpdateV1 returns for
// EncodeStateAsUpdateV1(doc, sv), byte for byte, without encoding the state
// and decoding it back: it reads doc's own structs and writes each one as the
// decode would have read it. It copies a struct only to cut it, to write a
// subdocument, or when the decode would read one of its values back as
// another Go type (see rereadsSame), which no value Yjs or ygo writes in an
// update is. What it allocates then grows with the updates it returns and
// doc's clients, not with doc's structs. It holds doc's lock until it
// returns.
//
// A document holding parked structs or deletions is cut from its encoded
// state, by SplitUpdateV1: these reach the state through a merge of their own
// (see withParked), which the walk over doc's structs does not repeat.
func SplitStateAsUpdateV1(doc *Doc, sv StateVector, maxBytes int) ([][]byte, error) {
	doc.mu.Lock()
	store := doc.store
	if store.pending != nil || len(store.pendingDs.clients) > 0 {
		state := withParked(doc, sv, encodeV1Locked(doc, sv), encodeStructStoreV1, MergeUpdatesV1)
		doc.mu.Unlock()
		return SplitUpdateV1(state, sv, maxBytes)
	}
	defer doc.mu.Unlock()
	queues := make([]splitQueue, 0, len(store.clients))
	for c, items := range store.clients {
		queues = appendUnsent(queues, c, items, sv)
	}
	s := newUpdateSplitter(queues, buildDeleteSet(store), sv, maxBytes, true)
	s.run()
	if s.err != nil {
		return nil, s.err
	}
	return s.out, nil
}

// appendUnsent appends to queues those of the clock-ordered structs of c that
// a receiver whose state vector is sv does not hold whole, if there are any. A
// struct the receiver holds whole is not sent, as encodeStructStoreV1 and
// EncodeStateAsUpdateV1 leave it out.
func appendUnsent(queues []splitQueue, c ClientID, items []*Item, sv StateVector) []splitQueue {
	known := sv.Clock(c)
	from := sort.Search(len(items), func(i int) bool {
		return items[i].ID.Clock+items[i].Content.Len() > known
	})
	if from == len(items) {
		return queues
	}
	return append(queues, splitQueue{client: c, held: known, items: items[from:]})
}

// splitQueue is the structs of one client still to place, clock-ascending,
// and what the series has sent of that client.
type splitQueue struct {
	client ClientID
	held   uint64  // the clocks of client the receiver holds once every update so far is applied
	cut    *Item   // the rest of a struct the series has cut, ahead of items; never a store's
	items  []*Item // read, never written: they can be a document's own
	packed int     // the update being packed holds a struct of client when this is updateSplitter.pieces
	dels   *clientDeletes
}

// next is the struct the queue places next, or nil when it is empty.
func (q *splitQueue) next() *Item {
	if q.cut != nil {
		return q.cut
	}
	if len(q.items) > 0 {
		return q.items[0]
	}
	return nil
}

// pop drops the struct next returns.
func (q *splitQueue) pop() {
	if q.cut != nil {
		q.cut = nil
		return
	}
	q.items = q.items[1:]
}

// offsetIn is the clocks of it, a struct of q, that the receiver already
// holds, which the update leaves out, as encodeStructStoreV1 cuts the first
// struct of a group at the receiver's clock. It is zero for every struct but
// the first of a client.
func (q *splitQueue) offsetIn(it *Item) uint64 {
	if q.held > it.ID.Clock {
		return q.held - it.ID.Clock
	}
	return 0
}

// clientDeletes is the delete ranges of one client, and how far the series
// has sent them.
type clientDeletes struct {
	client ClientID
	ranges []DeleteRange
	idx    int    // the first range the series has not sent whole
	from   uint64 // the clock in that range the series has sent up to
}

// pieceSpan is where one struct of the update being packed lies in
// updateSplitter.arena.
type pieceSpan struct {
	client     ClientID
	start, end int
}

// updateSplitter packs the structs and deletions of one update, or one
// document's state, into the updates SplitUpdateV1 and SplitStateAsUpdateV1
// return.
type updateSplitter struct {
	queues   []splitQueue // descending by client, as the encoder writes them
	queueOf  map[ClientID]*splitQueue
	sv       StateVector     // what the receiver holds of a client with no queue
	dels     []clientDeletes // descending by client
	maxBytes int

	// stored is set when the queues' items are a document's own structs
	// rather than structs decoded from an update. The splitter then writes
	// them as the decode would have read them (encodeOwn), cuts only a copy of
	// one (materialize), and writes each struct once, into arena.
	stored bool
	err    error // a struct of the document's that does not decode

	pieces  int         // the updates begun, the last of them the one being packed
	pieceDS DeleteSet   // its deletions
	base    StateVector // what the receiver held when it began, for the clients with structs in it
	bytes   int         // what it encodes to, never counted short
	// Its structs: by client when they are decoded, which encodeStructStoreV1
	// writes, and as written, in arena, when stored.
	piece map[ClientID][]*Item
	arena []byte
	spans []pieceSpan

	scratch    *encoding.Encoder // holds the encoding of measured, past measuredAt
	measured   *Item
	measuredAt uint64
	reader     *Doc // what materialize decodes into, made when it is first needed

	out [][]byte
}

func newUpdateSplitter(queues []splitQueue, ds DeleteSet, sv StateVector, maxBytes int, stored bool) *updateSplitter {
	s := &updateSplitter{
		queues:   queues,
		queueOf:  make(map[ClientID]*splitQueue, len(queues)),
		sv:       sv,
		dels:     make([]clientDeletes, 0, len(ds.clients)),
		maxBytes: maxBytes,
		stored:   stored,
		scratch:  encoding.NewEncoder(),
	}
	for c, ranges := range ds.clients {
		s.dels = append(s.dels, clientDeletes{client: c, ranges: ranges})
	}
	// Yjs writes clients in descending order, and encodeStructStoreV1 and
	// encodeDeleteSet sort them that way too.
	slices.SortFunc(s.queues, func(a, b splitQueue) int { return cmp.Compare(b.client, a.client) })
	slices.SortFunc(s.dels, func(a, b clientDeletes) int { return cmp.Compare(b.client, a.client) })
	for i := range s.queues {
		s.queueOf[s.queues[i].client] = &s.queues[i]
	}
	for i := range s.dels {
		if q := s.queueOf[s.dels[i].client]; q != nil {
			q.dels = &s.dels[i]
		}
	}
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
		for i := range s.queues {
			for s.placeReady(&s.queues[i]) {
				progress = true
			}
		}
	}
	s.flush()
	s.emitRemainingDeletes()
	s.out = append(s.out, s.tail())
}

// placeReady places the next struct of q when the receiver can integrate it,
// and reports whether it did.
func (s *updateSplitter) placeReady(q *splitQueue) bool {
	it := q.next()
	if it == nil || !s.ready(q, it) {
		return false
	}
	s.place(q)
	return true
}

// ready reports whether it, a struct of q, integrates into the receiver
// without waiting for a struct that has not arrived: its own client's clocks
// reach it, and the structs its encoding names as origin, right origin and
// parent have arrived. These are the dependencies the apply defers an item
// for — a clock gap and missingDep in decodeStructs and integrateGroup, the
// right origin in integrateGroup, and all four again in itemFutureDep, which
// is what parks it.
func (s *updateSplitter) ready(q *splitQueue, it *Item) bool {
	if it.ID.Clock > q.held {
		return false
	}
	switch {
	case it.isGCOrphan():
		// Written as a GC struct, which names nothing.
		return true
	case it.ID.Clock < q.held:
		// Cut at the receiver's clock, it names as origin the clock before the
		// cut, which the receiver holds, and no parent.
		return s.holds(q, it.OriginRight)
	case it.Origin != nil || it.OriginRight != nil:
		// With an origin it names no parent: the receiver takes the parent of
		// the struct its origin names (decodeItem).
		return s.holds(q, it.Origin) && s.holds(q, it.OriginRight)
	}
	return s.holds(q, namedParent(it))
}

// holds reports whether the receiver holds the struct id names, or id is nil,
// once every update so far is applied; q is the queue of the struct naming it.
func (s *updateSplitter) holds(q *splitQueue, id *ID) bool {
	if id == nil {
		return true
	}
	if id.Client == q.client {
		return id.Clock < q.held
	}
	if other := s.queueOf[id.Client]; other != nil {
		return id.Clock < other.held
	}
	return id.Clock < s.sv.Clock(id.Client)
}

// namedParent is the container item that it is written as a child of when it
// has no origin, or nil when it is written as a child of a root type. A
// document's struct holds its container as Parent, a decoded one as parentID.
func namedParent(it *Item) *ID {
	if it.Parent != nil && it.Parent.item != nil {
		return &it.Parent.item.ID
	}
	return it.parentID
}

// place adds the next struct of q to the update being packed, starting a new
// update when it does not fit this one and cutting it when it does not fit an
// update of its own.
func (s *updateSplitter) place(q *splitQueue) {
	it := q.next()
	own := s.stored && q.cut == nil
	var rest *Item
	if !s.fits(q, it, own) {
		if !s.pieceEmpty() {
			s.flush()
		}
		if !s.fits(q, it, own) {
			if own && cuttableContent(it.Content) {
				// The cut changes the struct it cuts, so it cuts the splitter's
				// own copy of the document's.
				if read := s.materialize(it, q.offsetIn(it)); read != nil {
					it, own = read, false
				}
			}
			if !own {
				rest = s.cut(q, it)
			}
		}
	}
	q.pop()
	s.add(q, it, own)
	if rest != nil {
		q.cut = rest
	}
}

// fits reports whether it, a struct of q, still leaves the update being
// packed within maxBytes.
func (s *updateSplitter) fits(q *splitQueue, it *Item, own bool) bool {
	return s.bytes+s.structCost(q, it, own) <= s.maxBytes
}

// structCost is what it, a struct of q, adds to the update being packed: the
// struct, and the group header when it is the first struct of q's client in
// that update.
func (s *updateSplitter) structCost(q *splitQueue, it *Item, own bool) int {
	offset := q.offsetIn(it)
	n := s.structBytes(it, offset, own)
	if q.packed != s.pieces {
		n += splitCountBytes + varUintLen(uint64(q.client)) + varUintLen(it.ID.Clock+offset)
	}
	return n
}

// structBytes is what it encodes to past offset, which it leaves in scratch.
// A struct is measured to see whether it fits and again to add it, so the
// last one measured is not written again.
func (s *updateSplitter) structBytes(it *Item, offset uint64, own bool) int {
	if it != s.measured || offset != s.measuredAt {
		s.scratch.Reset()
		if own {
			s.encodeOwn(s.scratch, it, offset)
		} else {
			encodeItem(s.scratch, it, offset)
		}
		s.measured, s.measuredAt = it, offset
	}
	return len(s.scratch.Bytes())
}

// encodeOwn writes a document's struct past offset as encodeItem writes the
// struct decodeStructsV1 reads for it from EncodeStateAsUpdateV1. The decode
// leaves no parentSub on a struct written with an origin, which encodeItem
// would otherwise flag, and reads values back as the Go types it decodes to,
// which encodeItem can write in other bytes than it wrote (see rereadsSame).
func (s *updateSplitter) encodeOwn(enc *encoding.Encoder, it *Item, offset uint64) {
	if !rereadsSame(it.Content) {
		if read := s.materialize(it, offset); read != nil {
			encodeItem(enc, read, 0)
			return
		}
	}
	if it.ParentSub != nil && (offset > 0 || it.Origin != nil || it.OriginRight != nil) {
		read := *it
		read.ParentSub = nil
		encodeItem(enc, &read, offset)
		return
	}
	encodeItem(enc, it, offset)
}

// materialize returns a document's struct as decodeStructsV1 reads it from
// EncodeStateAsUpdateV1, which cuts it at offset: a struct of the splitter's
// own, which a cut may change. It returns nil, and the split fails, when the
// struct does not decode, as the decode of the whole state would have failed.
func (s *updateSplitter) materialize(it *Item, offset uint64) *Item {
	written := encoding.EncodeBytes(func(enc *encoding.Encoder) { encodeItem(enc, it, offset) })
	if s.reader == nil {
		s.reader = New()
	}
	read, err := decodeItem(encoding.NewDecoder(written), s.reader, it.ID.Client, it.ID.Clock+offset)
	if err != nil {
		if s.err == nil {
			s.err = wrapUpdateErr(err)
		}
		return nil
	}
	return read
}

// add puts it, a struct of q, in the update being packed, advances what the
// receiver holds, and takes the deletions of the clocks it delivers, so the
// update that brings a struct brings its deletion: a receiver never renders
// deleted content it is about to be told is gone.
func (s *updateSplitter) add(q *splitQueue, it *Item, own bool) {
	s.bytes += s.structCost(q, it, own)
	if q.packed != s.pieces {
		q.packed = s.pieces
		s.base[q.client] = q.held
	}
	if s.stored {
		written := s.scratch.Bytes()
		s.spans = append(s.spans, pieceSpan{client: q.client, start: len(s.arena), end: len(s.arena) + len(written)})
		s.arena = append(s.arena, written...)
	} else {
		s.piece[q.client] = append(s.piece[q.client], it)
	}
	q.held = it.ID.Clock + it.Content.Len()
	if q.dels != nil {
		s.takeDeletes(q.dels, q.held)
	}
}

// cut shortens it, a struct of q and the splitter's own, to what fits an
// update of its own and returns the rest of it, to be placed after it, or nil
// when it cannot be cut that small. A struct a V1 update cannot cut is left
// whole, and the update that carries it passes maxBytes.
func (s *updateSplitter) cut(q *splitQueue, it *Item) *Item {
	offset := q.offsetIn(it)
	budget := s.maxBytes - s.bytes - splitCountBytes - varUintLen(uint64(q.client)) - varUintLen(it.ID.Clock+offset)
	clocks := s.fittingClocks(it, offset, budget)
	if clocks == 0 {
		return nil
	}
	return splitItemAt(it, offset+clocks)
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
	s.measured = nil
	s.scratch.Reset()
	encodeItem(s.scratch, &head, offset)
	return len(s.scratch.Bytes())
}

// takeDeletes adds to the update being packed the ranges of d the series has
// not sent that name a clock below end, cutting a range at end and stopping
// at the first that does not fit what the update has left, and returns how
// many it took. It takes one whatever the room left when the update is empty,
// so a caller that flushes on nothing taken makes progress.
func (s *updateSplitter) takeDeletes(d *clientDeletes, end uint64) int {
	if d.idx == len(d.ranges) {
		return 0 // every range of d has gone out
	}
	c := d.client
	idx, from := d.idx, d.from
	group := 0
	if len(s.pieceDS.clients[c]) == 0 {
		group = splitCountBytes + varUintLen(uint64(c))
	}
	taken, cost := 0, 0
	for ; idx < len(d.ranges); idx++ {
		r := d.ranges[idx]
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
	d.idx, d.from = idx, from
	return taken
}

// emitRemainingDeletes packs the delete ranges no update carried with its
// structs into updates of their own, after every struct the series sends: the
// ranges of a client whose structs the receiver already held have no update
// to ride in, and neither have those naming a clock past a client's structs,
// which park on the receiver as they are parked on the sender. What is left
// over stays in the update being packed, which the series ends with.
func (s *updateSplitter) emitRemainingDeletes() {
	for i := range s.dels {
		d := &s.dels[i]
		for d.idx < len(d.ranges) {
			if s.takeDeletes(d, math.MaxUint64) == 0 {
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
	if s.stored {
		s.out = append(s.out, s.encodePiece())
	} else {
		s.out = append(s.out, encodeStructStoreV1(s.piece, s.pieceDS, s.base))
	}
	s.newPiece()
}

// encodePiece writes the update being packed from the structs add wrote into
// arena, as encodeStructStoreV1 writes it: a group for each client, ascending,
// starting at the clock the receiver held when the update began, and then the
// update's deletions.
func (s *updateSplitter) encodePiece() []byte {
	if len(s.base) > 1 {
		slices.SortStableFunc(s.spans, func(a, b pieceSpan) int { return cmp.Compare(a.client, b.client) })
	}
	return encoding.EncodeBytes(func(enc *encoding.Encoder) {
		enc.WriteVarUint(uint64(len(s.base)))
		for spans := s.spans; len(spans) > 0; {
			c := spans[0].client
			n := 1
			for n < len(spans) && spans[n].client == c {
				n++
			}
			enc.WriteVarUint(uint64(n))
			enc.WriteVarUint(uint64(c))
			enc.WriteVarUint(s.base[c])
			for _, span := range spans[:n] {
				enc.WriteRaw(s.arena[span.start:span.end])
			}
			spans = spans[n:]
		}
		encodeDeleteSet(enc, s.pieceDS)
	})
}

// newPiece begins an update with nothing in it.
func (s *updateSplitter) newPiece() {
	if s.stored {
		s.arena, s.spans = s.arena[:0], s.spans[:0]
	} else {
		s.piece = make(map[ClientID][]*Item)
	}
	s.pieces++
	s.pieceDS = newDeleteSet()
	s.base = make(StateVector)
	s.bytes = splitHeaderBytes
}

// pieceEmpty reports whether the update being packed carries nothing.
func (s *updateSplitter) pieceEmpty() bool {
	return len(s.base) == 0 && len(s.pieceDS.clients) == 0
}

// tail is the update the series ends with: the structs no update could carry
// on its own, which the receiver parks as the sender does, and the deletions
// left over. Its structs are above what the receiver holds, so it needs no
// state vector to cut them at; a document's own are first read as the decode
// reads them, cut at what the receiver holds.
func (s *updateSplitter) tail() []byte {
	var left map[ClientID][]*Item
	for i := range s.queues {
		q := &s.queues[i]
		if q.cut == nil && len(q.items) == 0 {
			continue
		}
		rest := q.items
		if q.cut != nil || s.stored {
			rest = make([]*Item, 0, len(q.items)+1)
			if q.cut != nil {
				rest = append(rest, q.cut)
			}
			for _, it := range q.items {
				if s.stored {
					if read := s.materialize(it, q.offsetIn(it)); read != nil {
						it = read
					}
				}
				rest = append(rest, it)
			}
		}
		if left == nil {
			left = make(map[ClientID][]*Item)
		}
		left[q.client] = rest
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

// rereadsSame reports whether encodeContent writes c in the same bytes as the
// content decodeContent reads back from them. The decode reads each value back
// as the Go type it decodes to — a JSON number as a float64, a lib0 integer as
// an int64, or past int32 as a float64, which WriteAny then writes as a
// float32 when one holds it exactly — and a value of those types is written
// as it was read. A document filled by updates holds no other, so its content
// passes. A value written through the Go API in another type passes when it
// is an integer the decode's type writes in the same bytes, and otherwise
// does not.
func rereadsSame(c Content) bool {
	switch ct := c.(type) {
	case *ContentAny:
		return allRereadSame(ct.Vals, anyRereadsSame)
	case *ContentJSON:
		return allRereadSame(ct.Vals, jsonRereadsSame)
	case *ContentEmbed:
		return jsonRereadsSame(ct.Val)
	case *ContentFormat:
		return jsonRereadsSame(ct.Val)
	case *ContentDoc:
		return false // the decode rebuilds its options into a new document
	}
	return true
}

// allRereadSame reports whether rereads holds for each of vals.
func allRereadSame(vals []any, rereads func(any) bool) bool {
	for _, v := range vals {
		if !rereads(v) {
			return false
		}
	}
	return true
}

// anyRereadsSame reports it for a value WriteAny writes: one of the types
// ReadAny returns, or an integer within int32, which both write as a lib0
// integer.
func anyRereadsSame(v any) bool {
	switch x := v.(type) {
	case nil, bool, string, []byte, float32, float64, encoding.BigInt,
		int8, int16, int32, uint8, uint16:
		return true
	case int:
		return int64(x) >= math.MinInt32 && int64(x) <= math.MaxInt32
	case int64:
		return x >= math.MinInt32 && x <= math.MaxInt32
	case uint:
		return uint64(x) <= math.MaxInt32
	case uint32:
		return x <= math.MaxInt32
	case uint64:
		return x <= math.MaxInt32
	case []any:
		return allRereadSame(x, anyRereadsSame)
	case map[string]any:
		for _, e := range x {
			if !anyRereadsSame(e) {
				return false
			}
		}
		return true
	}
	return false
}

// jsonRereadsSame reports it for a value fmtValToJSON writes: one of the types
// json.Unmarshal returns, with no string that is not UTF-8, which the
// encoding replaces, or an integer a float64 holds, which both write in the
// same digits.
func jsonRereadsSame(v any) bool {
	switch x := v.(type) {
	case nil, bool, float64:
		return true
	case int64:
		return x >= -1<<53 && x <= 1<<53
	case string:
		return utf8.ValidString(x)
	case []any:
		return allRereadSame(x, jsonRereadsSame)
	case map[string]any:
		for k, e := range x {
			if !utf8.ValidString(k) || !jsonRereadsSame(e) {
				return false
			}
		}
		return true
	}
	return false
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
