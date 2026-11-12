package crdt

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"sort"
)

// SetDiscardRefusedDrain (off by default) changes what a document with a
// scan bound, budget or apply check does with a drain of its pending queue
// that it takes back (drainParked): the apply check refused what the drain
// landed (WithApplyCheck), or the drain's scans passed their allowance, or
// its splits the split budget. Off, the drain is taken back and the backlog
// is parked again whole, so the next update that fills the same gap runs
// into the same refusal. On, the drain is taken back and everything the fill
// released is discarded instead: every parked struct the store now holds the
// dependencies of, or that another such struct supplies - what a drain run
// to its end would have integrated, read from the clocks alone - and every
// pending delete range, or part of one, below the clocks the store and those
// structs then reach. What the fill did not release stays parked.
//
// The update that filled the gap stands, as it does with the option off,
// and the apply returns a *DrainDiscardedError that names what was discarded
// and the origins of the applies that parked it, which the document records
// at each commit that parks something while the option is on. A document
// with neither a scan bound, a budget nor an apply check takes no drain
// back, and the option changes nothing for it. Turning it off forgets the
// origins recorded, and ForgetParkedOrigin forgets one of them.
func (d *Doc) SetDiscardRefusedDrain(on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.discardRefusedDrain = on
	if !on {
		d.parkedBy, d.parkedDsBy, d.parkedDsByRanges = nil, nil, 0
	}
}

// ForgetParkedOrigin drops origin from the record a document that discards
// refused drains keeps of the apply that parked each struct and delete range
// (SetDiscardRefusedDrain), so the document holds no reference to origin and
// a later DrainDiscardedError does not name it. What origin parked stays
// parked. A server calls it once the sender whose applies carried origin has
// gone: the record would otherwise keep that sender, and all it holds, for as
// long as its parked structs and ranges stay parked, and a sender that has
// gone has nothing left to close. A nil origin, or one Go cannot compare, is
// not matched. It costs a pass over the record, which the queue bounds.
func (d *Doc) ForgetParkedOrigin(origin any) {
	if origin == nil || !reflect.ValueOf(origin).Comparable() {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for it, by := range d.parkedBy {
		if by == origin {
			delete(d.parkedBy, it)
		}
	}
	kept := d.parkedDsBy[:0]
	for _, by := range d.parkedDsBy {
		if by.origin == origin {
			d.parkedDsByRanges -= rangeCount(by.ds)
			continue
		}
		kept = append(kept, by)
	}
	clear(d.parkedDsBy[len(kept):])
	d.parkedDsBy = kept
}

// DrainDiscardedError is the error of a remote apply whose update stands and
// whose drain of the pending queue was taken back and what its fill released
// discarded (SetDiscardRefusedDrain). It wraps ErrApplyCheckDrain, so a
// caller that takes an apply refused only in its drain as applied - the
// websocket and HTTP providers answer and relay such an update as one -
// still does, and Cause, the reason the drain was taken back.
type DrainDiscardedError struct {
	// Structs and Ranges are the parked structs and pending delete ranges
	// discarded; a range of which only a part was below the released clocks
	// counts once.
	Structs, Ranges int
	// Scanned is what the discarded drain's scans passed.
	Scanned int
	// Senders are the origins of the applies that parked what was
	// discarded: each comparable origin once, in no order. An origin the
	// document has no record of - a park made before the option was set,
	// one whose struct a later park's merge replaced in the queue, one of
	// several that parked parts of one pending delete range, when the
	// record keeps that range under another of them (pruneParkedDeletes),
	// or one the document was told to forget (ForgetParkedOrigin) - is
	// missing, and a nil origin is left out.
	Senders []any
	// Cause is why the drain was taken back: the apply check's refusal
	// (which wraps ErrApplyCheckDrain itself), or the scan allowance's or
	// split budget's error.
	Cause error
}

func (e *DrainDiscardedError) Error() string {
	return fmt.Sprintf("crdt: the drain of the pending queue was taken back and what it released discarded: %d structs and %d delete ranges, from %d senders, its scans having passed %d: %v",
		e.Structs, e.Ranges, len(e.Senders), e.Scanned, e.Cause)
}

// Unwrap is ErrApplyCheckDrain and Cause.
func (e *DrainDiscardedError) Unwrap() []error {
	if e.Cause == nil {
		return []error{ErrApplyCheckDrain}
	}
	return []error{ErrApplyCheckDrain, e.Cause}
}

// parkedDeletesBy is the delete ranges the applies of one origin parked,
// as far as the record of who parked them still holds them, and that origin.
type parkedDeletesBy struct {
	ds     DeleteSet
	origin any
}

// recordParkedOrigins keeps, on a document that discards refused drains,
// the origin of txn for every struct and delete range txn parked and the
// queue still holds at its commit, and forgets every struct txn took out of
// the queue for good (transactionParked.gone), so the record of structs
// holds none the queue does not hold. It forgets the records of delete
// ranges the queue no longer holds once they outnumber it: after every
// commit the record of delete ranges holds at most 2*pendingDsRanges+16
// ranges, whatever frames parked them (pruneParkedDeletes). The caller holds
// d.mu.
func recordParkedOrigins(d *Doc, txn *Transaction) {
	if !d.discardRefusedDrain {
		return
	}
	s := d.store
	if rec := s.parked; rec != nil {
		if len(rec.items) > 0 && s.pending != nil {
			left := make(map[*Item]struct{}, len(rec.left))
			for _, it := range rec.left {
				left[it] = struct{}{}
			}
			for _, it := range rec.items {
				if _, gone := left[it]; it == nil || gone {
					continue
				}
				if d.parkedBy == nil {
					d.parkedBy = make(map[*Item]any)
				}
				d.parkedBy[it] = txn.Origin
			}
		}
		// Only what the apply newly parked is its own. A nil origin is
		// named to no one (DrainDiscardedError.Senders), so it is not kept.
		if txn.Origin != nil {
			for _, ds := range rec.fresh {
				recordParkedDeletes(d, ds, txn.Origin)
			}
		}
		for _, it := range rec.gone {
			delete(d.parkedBy, it)
		}
	}
	if s.pending.heldStructs() == 0 {
		d.parkedBy = nil
	}
	switch {
	case len(s.pendingDs.clients) == 0:
		d.parkedDsBy, d.parkedDsByRanges = nil, 0
	case d.parkedDsByRanges > 2*s.pendingDsRanges+16:
		pruneParkedDeletes(d)
	}
}

// recordParkedDeletes adds ds, the delete ranges an apply of origin newly
// parked, to the record: into the one set of origin's ranges when origin is
// comparable, compacted as the pending deletes are, so frames of one sender
// that grow or bridge the ranges it parked add none, and as a set of its own
// otherwise. It keeps parkedDsByRanges.
func recordParkedDeletes(d *Doc, ds DeleteSet, origin any) {
	var by *parkedDeletesBy
	if reflect.ValueOf(origin).Comparable() {
		for i := range d.parkedDsBy {
			if d.parkedDsBy[i].origin == origin {
				by = &d.parkedDsBy[i]
				break
			}
		}
	}
	if by == nil {
		d.parkedDsBy = append(d.parkedDsBy, parkedDeletesBy{ds: newDeleteSet(), origin: origin})
		by = &d.parkedDsBy[len(d.parkedDsBy)-1]
	}
	for client := range ds.clients {
		d.parkedDsByRanges -= len(by.ds.clients[client])
	}
	by.ds.Merge(ds)
	for client := range ds.clients {
		d.parkedDsByRanges += len(by.ds.clients[client])
	}
}

// pruneParkedDeletes cuts the record of who parked each delete range to the
// clocks the pending deletes still hold. The sets of several origins can
// still cut one pending range into more pieces than the bound allows - one
// sender's frames parking the clocks between another's - so when more than
// 2*pendingDsRanges+16 ranges are left, each pending range the record touches
// is kept whole under one origin, that of the first set that touches it
// (onePerPendingRange). The record then holds at most pendingDsRanges ranges
// and still names an origin for every pending range it named one for. Each
// range the record holds is found in the pending deletes by a binary search,
// and the prune runs only once the record holds more than twice the pending
// ranges, so its work is what the record holds.
func pruneParkedDeletes(d *Doc) {
	s := d.store
	kept, n := d.parkedDsBy[:0], 0
	for _, by := range d.parkedDsBy {
		by.ds = heldPart(by.ds, s.pendingDs)
		if r := rangeCount(by.ds); r > 0 {
			kept = append(kept, by)
			n += r
		}
	}
	clear(d.parkedDsBy[len(kept):])
	d.parkedDsBy, d.parkedDsByRanges = kept, n
	if n > 2*s.pendingDsRanges+16 {
		d.parkedDsBy, d.parkedDsByRanges = onePerPendingRange(kept, s.pendingDs)
	}
}

// heldPart is the part of ds that held also holds: each range of ds cut to
// the clocks the ranges of held of the same client cover, which are sorted
// and apart, as the pending deletes keep them.
func heldPart(ds, held DeleteSet) DeleteSet {
	out := newDeleteSet()
	for client, ranges := range ds.clients {
		h := held.clients[client]
		var kept []DeleteRange
		for _, r := range ranges {
			end := r.Clock + r.Len
			i := sort.Search(len(h), func(i int) bool { return h[i].Clock+h[i].Len > r.Clock })
			for ; i < len(h) && h[i].Clock < end; i++ {
				start := max(r.Clock, h[i].Clock)
				kept = append(kept, DeleteRange{Clock: start, Len: min(end, h[i].Clock+h[i].Len) - start})
			}
		}
		if len(kept) > 0 {
			out.clients[client] = kept
		}
	}
	return out
}

// onePerPendingRange is the record by, each of whose ranges lies within one
// range of pending (heldPart), with each pending range it touches kept whole
// under the first set of by that touches it, and the ranges it then holds.
func onePerPendingRange(by []parkedDeletesBy, pending DeleteSet) ([]parkedDeletesBy, int) {
	taken := make(map[ClientID][]bool)
	out, n := by[:0], 0
	for _, b := range by {
		ds := newDeleteSet()
		for client, ranges := range b.ds.clients {
			h := pending.clients[client]
			took, ok := taken[client]
			if !ok {
				took = make([]bool, len(h))
				taken[client] = took
			}
			var whole []DeleteRange
			for _, r := range ranges {
				i := sort.Search(len(h), func(i int) bool { return h[i].Clock+h[i].Len > r.Clock })
				if !took[i] {
					took[i] = true
					whole = append(whole, h[i])
				}
			}
			if len(whole) > 0 {
				ds.clients[client] = whole
				n += len(whole)
			}
		}
		if len(ds.clients) > 0 {
			out = append(out, parkedDeletesBy{ds: ds, origin: b.origin})
		}
	}
	clear(by[len(out):])
	return out, n
}

// rangeCount is how many ranges ds holds over all its clients.
func rangeCount(ds DeleteSet) int {
	n := 0
	for _, ranges := range ds.clients {
		n += len(ranges)
	}
	return n
}

// unparkedPart is the part of ds that parked does not hold: each range of ds
// less the clocks parked's ranges of the same client cover.
func unparkedPart(ds, parked DeleteSet) DeleteSet {
	out := newDeleteSet()
	for client, ranges := range ds.clients {
		held := slices.Clone(parked.clients[client])
		slices.SortFunc(held, func(a, b DeleteRange) int { return cmp.Compare(a.Clock, b.Clock) })
		var kept []DeleteRange
		for _, r := range ranges {
			start, end := r.Clock, r.Clock+r.Len
			for _, h := range held {
				if h.Clock+h.Len <= start {
					continue
				}
				if h.Clock >= end {
					break
				}
				if h.Clock > start {
					kept = append(kept, DeleteRange{Clock: start, Len: h.Clock - start})
				}
				start = h.Clock + h.Len
				if start >= end {
					break
				}
			}
			if start < end {
				kept = append(kept, DeleteRange{Clock: start, Len: end - start})
			}
		}
		if len(kept) > 0 {
			out.clients[client] = kept
		}
	}
	return out
}

// releasedByFill is every struct of items, the pending queue as a drain
// found it, that the store s now holds the dependencies of or that another
// such struct supplies: what a drain run to its end would integrate, read
// from the clocks alone, with no integration and no scan. It returns too
// each client's clock as those structs would leave it, for the clients it
// read.
func releasedByFill(s *StructStore, items []*Item) (map[*Item]struct{}, map[ClientID]uint64) {
	reach := make(map[ClientID]uint64)
	clock := func(c ClientID) uint64 {
		if v, ok := reach[c]; ok {
			return v
		}
		v := s.NextClock(c)
		reach[c] = v
		return v
	}
	sorted := slices.Clone(items)
	slices.SortStableFunc(sorted, func(a, b *Item) int { return cmp.Compare(a.ID.Clock, b.ID.Clock) })
	released := make(map[*Item]struct{})
	for changed := true; changed; {
		changed = false
		for _, it := range sorted {
			if _, done := released[it]; done || it == nil {
				continue
			}
			c := it.ID.Client
			if it.ID.Clock > clock(c) ||
				it.Origin != nil && it.Origin.Clock >= clock(it.Origin.Client) ||
				it.OriginRight != nil && it.OriginRight.Clock >= clock(it.OriginRight.Client) ||
				it.Parent == nil && it.parentID != nil && it.parentID.Clock >= clock(it.parentID.Client) {
				continue
			}
			released[it] = struct{}{}
			reach[c] = max(clock(c), it.ID.Clock+it.Content.Len())
			changed = true
		}
	}
	return released, reach
}

// parkAgainExcept is parkAgain without the structs in gone, or nil when
// nothing is left to park.
func parkAgainExcept(s *StructStore, found *pendingUpdate, gone map[*Item]struct{}) *pendingUpdate {
	if found == nil {
		return nil
	}
	var kept []*Item
	for _, item := range found.heldItems() {
		if _, out := gone[item]; out {
			continue
		}
		item.Left, item.Right, item.MovedBy = nil, nil, nil
		kept = append(kept, item)
	}
	if len(kept) == 0 {
		return nil
	}
	missing := make(StateVector, len(found.missing))
	for client, clock := range found.missing {
		missing[client] = max(clock, s.NextClock(client))
	}
	return &pendingUpdate{items: kept, missing: missing, bytes: itemsBytes(kept)}
}

// discardReleased is the take-back of a refused drain on a document that
// discards refused drains, run once rollBackTo has left the store as the
// drain found it: the queue as the drain found it (found) is parked again
// without what the fill released, and the pending deletes as the drain found
// them (foundDs) keep only the ranges, and the parts of ranges, above the
// clocks the store and the released structs reach. It records the discard on
// txn's bounded apply state: scanned is what the drain's scans passed, and
// cause why it was taken back.
func discardReleased(txn *Transaction, found *pendingUpdate, foundDs parkedDeletesAt, scanned int, cause error) {
	d := txn.doc
	s := d.store
	var items []*Item
	if found != nil {
		items = found.heldItems()
	}
	released, reach := releasedByFill(s, items)
	s.pending = parkAgainExcept(s, found, released)

	clock := func(c ClientID) uint64 {
		if v, ok := reach[c]; ok {
			return v
		}
		return s.NextClock(c)
	}
	kept := DeleteSet{clients: make(map[ClientID][]DeleteRange, len(foundDs.ds.clients))}
	dropped := DeleteSet{clients: make(map[ClientID][]DeleteRange)}
	keptRanges, droppedRanges := 0, 0
	for _, client := range deleteSetClients(foundDs.ds) {
		below := clock(client)
		var keep, drop []DeleteRange
		for _, r := range foundDs.ds.clients[client] {
			switch {
			case r.Clock+r.Len <= below:
				drop = append(drop, r)
			case r.Clock < below:
				drop = append(drop, DeleteRange{Clock: r.Clock, Len: below - r.Clock})
				keep = append(keep, DeleteRange{Clock: below, Len: r.Clock + r.Len - below})
			default:
				keep = append(keep, r)
			}
		}
		if len(keep) > 0 {
			kept.clients[client] = keep
			if foundDs.ds.order != nil {
				kept.order = append(kept.order, client)
			}
			keptRanges += len(keep)
		}
		if len(drop) > 0 {
			dropped.clients[client] = drop
			droppedRanges += len(drop)
		}
	}
	s.pendingDs, s.pendingDsRanges, s.pendingDsMoved = kept, keptRanges, foundDs.moved

	state := txn.boundedApply
	state.drainTakenBack = true
	state.discardedStructs += len(released)
	state.discardedRanges += droppedRanges
	state.drainScanned += scanned
	if state.drainRefused == nil {
		state.drainRefused = cause
	}
	// This transaction's own parks have no record yet: it is made at the
	// commit (recordParkedOrigins).
	ownParks := make(map[*Item]struct{})
	var ownDeletes []DeleteSet
	if rec := s.parked; rec != nil {
		for _, it := range rec.items {
			ownParks[it] = struct{}{}
		}
		ownDeletes = rec.deletes
	}
	seen := make(map[any]struct{})
	add := func(origin any) {
		if origin == nil {
			return
		}
		if reflect.TypeOf(origin).Comparable() {
			if _, dup := seen[origin]; dup {
				return
			}
			seen[origin] = struct{}{}
		}
		state.discardedSenders = append(state.discardedSenders, origin)
	}
	for it := range released {
		if origin, ok := d.parkedBy[it]; ok {
			add(origin)
		} else if _, own := ownParks[it]; own {
			add(txn.Origin)
		}
		txn.noteGone(it)
	}
	if droppedRanges > 0 {
		for _, by := range d.parkedDsBy {
			if deletesOverlap(by.ds, dropped) {
				add(by.origin)
			}
		}
		for _, ds := range ownDeletes {
			if deletesOverlap(ds, dropped) {
				add(txn.Origin)
			}
		}
	}
}

// deleteSetClients is ds's clients: those its order lists, in that order,
// then any other, ascending.
func deleteSetClients(ds DeleteSet) []ClientID {
	clients := make([]ClientID, 0, len(ds.clients))
	listed := make(map[ClientID]struct{}, len(ds.order))
	for _, c := range ds.order {
		if _, ok := ds.clients[c]; ok {
			if _, dup := listed[c]; !dup {
				listed[c] = struct{}{}
				clients = append(clients, c)
			}
		}
	}
	rest := len(clients)
	for c := range ds.clients {
		if _, ok := listed[c]; !ok {
			clients = append(clients, c)
		}
	}
	slices.Sort(clients[rest:])
	return clients
}

// deletesOverlap is whether a and b share a deleted clock.
func deletesOverlap(a, b DeleteSet) bool {
	for client, ra := range a.clients {
		for _, x := range ra {
			for _, y := range b.clients[client] {
				if x.Clock < y.Clock+y.Len && y.Clock < x.Clock+x.Len {
					return true
				}
			}
		}
	}
	return false
}
