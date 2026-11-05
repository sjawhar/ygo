package crdt

import (
	"fmt"
	"maps"
	"slices"
	"sort"
)

// A document with a scan bound (WithMaxIntegrateScan, SetMaxIntegrateScan) or
// a scan budget (WithIntegrateScanBudget, SetIntegrateScanBudget) is bounded:
// it applies a remote update whole or not at all. Integration is not a pure
// function of the update: each item is placed by a scan of the list as the
// items before it in the same update left it, so whether an update's scans
// pass the bound is known only by integrating it. An update refused partway -
// by the bound, or for any other reason its apply returns an error - is
// therefore integrated as far as it got and then taken out again
// (applyJournal.rollBack) before the transaction commits, so the document is
// left as it was and the transaction fires no observer
// (boundedApplyState.rolledBack):
// nothing the update carried is persisted, relayed or seen by anyone. The
// apply returns its error as a RefusedUpdateError, which carries what the
// apply's scans passed, since no observer reports it (endRemoteApply).
//
// The pending queue is not the update's. Items an earlier update parked behind
// a gap in its clocks integrate in the apply of the update that fills the gap
// (drainPending), and were a failed drain to refuse that update, an earlier
// sender could make every later update that fills its gap be refused. So the
// drain is a phase that fails on its own: a drain that fails is taken back to
// where it began (applyPoint) and the queue is parked again as the drain found
// it (drainParked), while the update that filled the gap stands. Under a bound
// the drain has an allowance of its own, the bound again (the scan count starts
// again from the drain); under a budget it draws on what the apply's allowance
// has left, so one apply's scans, drain included, never pass what the budget
// gave it. The scans of a drain taken back stay counted
// (Transaction.IntegrateScanned): the work was done.
//
// What a rollback has to undo is what integrating and deleting change:
//   - every struct added since the point it rolls back to, which is every
//     struct at or past the point's state vector, unlinked from its parent's
//     list, with the parent's length, and dropped from the store;
//   - the previous value of each map key a new item took (applyPoint.mapPrev);
//   - every item deleted since the point, which the transaction's delete set
//     lists beyond the point's copy of it;
//   - root types named since the point;
//   - the transaction's record of what changed;
//   - for the whole apply, the pending queue, pending deletes and pending moves
//     as they stood, and which move holds each target (movedBefore). The
//     parked items, the clocks the queue waits on (its missing map) and the
//     pending deletes are kept by reference, not copied, so an apply costs
//     nothing in proportion to what earlier updates parked. The apply only
//     appends to the parked items' slice, past the length the journal keeps,
//     or replaces it with a slice of its own (drainPending); it writes the
//     missing map only to park an item (resolveWithinUpdatePending), and its
//     first such write copies the map (parkedMissing); and it writes into the
//     pending deletes only to merge a delete range it parks (parkDeletes) or
//     to take the ranges a retry applies (retryPendingDs), and each such
//     write goes to a copy of their map and of the ranges of the clients it
//     names (ownPendingDs). The count of parked ranges and the clients
//     moved since the last retry, which the store keeps with them, are kept
//     as values (parkedDeletesAt). A park also adds to the queue's cover of
//     clocks (pendingUpdate.addCover), in place, so a rollback drops the
//     cover and the next park that asks for it builds it from the items
//     kept. The parked items themselves are not touched before the drain,
//     the apply's last step, so the whole apply's rollback needs no record of
//     their fields; a drain taken back gives back what it changed of them
//     (drainParked).
//
// Splits of items the document already held stay: a split leaves the same
// content, and the transaction's commit merges each split back
// (tryMergeWithLefts) once the items between the halves are gone. A remote
// update that would integrate a subdocument or a move is refused before
// anything is integrated, since registering either reaches outside the
// document's lists (refuseOnBoundedDoc).

// applyJournal is what one remote apply on a bounded document records so it
// can leave the document as it found it.
type applyJournal struct {
	start        applyPoint
	pending      *pendingUpdate
	pendingItems []*Item
	// pendingDead is pending's count of emptied slots as the apply found it.
	pendingDead int
	// pendingItemsOwn is whether pendingItems is the journal's own copy of
	// the queue's slots, made before a park's merge first emptied or moved
	// them in place (keepQueueFor).
	pendingItemsOwn bool
	// queuedAs is each queued struct a park's merge cut or grew in place, as
	// the apply found it, its content copied (keepQueueFor).
	queuedAs map[*Item]Item
	missing  StateVector
	// missingCopied is whether pending's missing map is the apply's own copy
	// (parkedMissing).
	missingCopied bool
	// pendingTail and pendingTailValid are the queue's tail record
	// (pendingUpdate.noteTail) as the apply found it; a park moves it in place.
	pendingTail      uint64
	pendingTailValid bool
	pendingReady     bool
	pendingDs        parkedDeletesAt
	pendingMoves     pendingMovesAt
	// drain is the point the drain began at, once it has; at is the point a
	// map key's previous value is recorded on.
	drain *applyPoint
	at    *applyPoint
	// parkedAs is each parked item the drain changed in place, as the drain
	// found it, for a drain taken back (keepParked).
	parkedAs map[*Item]Item
	// drainOwnAllowance is whether the drain's scans have an allowance of
	// their own (a bound) rather than drawing on the apply's (a budget).
	drainOwnAllowance bool
	// integrating is the item integrate has linked into its parent's list and
	// not yet appended to the store, for a failure between the two.
	integrating *Item
	// tally is what the apply decoded and added, and the allowance it may do
	// so within (WithApplyBudget, WithAllowedRoots). It lives in the journal
	// because an apply that keeps one is exactly an apply that can be
	// refused; the document points at it for the decode (Doc.applying).
	tally applyTally
}

// applyPoint is a point of an apply a rollback can return the document to.
type applyPoint struct {
	sv       StateVector
	deleted  DeleteSet
	changed  map[*abstractType]map[string]struct{}
	newItems int
	roots    map[string]struct{}
	mapPrev  map[mapSlot]*Item
	// fmtPrev is the formatting flags of each type the apply changed them
	// on, as they were at this point (tookFormatting).
	fmtPrev map[*abstractType]fmtFlags
}

// mapSlot names one key of one map-like type.
type mapSlot struct {
	parent *abstractType
	key    string
}

// fmtFlags is what integrating or deleting a ContentFormat changes on the
// type it hangs under: the two flags that move a text off the plain path and
// the counter the format cache reads (abstractType).
type fmtFlags struct {
	has   bool
	clean bool
	gen   uint64
}

// pointNow is txn's apply as it stands, with the store's state vector sv.
func pointNow(txn *Transaction, sv StateVector) applyPoint {
	point := applyPoint{
		sv:       sv,
		deleted:  copyDeleteSet(txn.deleteSet),
		changed:  make(map[*abstractType]map[string]struct{}, len(txn.changed)),
		newItems: len(txn.newItems),
		roots:    make(map[string]struct{}, len(txn.doc.share)),
	}
	for t, keys := range txn.changed {
		point.changed[t] = maps.Clone(keys)
	}
	for name := range txn.doc.share {
		point.roots[name] = struct{}{}
	}
	return point
}

// beginRemoteApply starts txn's journal when its document is bounded, and sets
// the most its scans may pass from the document's budget for txn's origin or
// its bound; before is the store's state vector as the apply begins. The
// parked items, their missing map and the pending deletes are kept as they
// are, by reference (see the top of this file).
func beginRemoteApply(txn *Transaction, before StateVector) {
	d := txn.doc
	if !d.scanBounded() {
		return
	}
	s := d.store
	j := &applyJournal{
		start:             pointNow(txn, before),
		drainOwnAllowance: d.integrateScanBudget == nil,
		tally:             applyTally{budget: noApplyBudget},
	}
	j.at = &j.start
	if s.pending != nil {
		j.pending = s.pending
		j.pendingItems = s.pending.items
		j.pendingDead = s.pending.dead
		j.missing = s.pending.missing
		j.pendingReady = s.pending.ready
		j.pendingTail, j.pendingTailValid = s.pending.tail, s.pending.tailValid
	}
	j.pendingDs = parkedDeletesOf(s)
	j.pendingMoves = pendingMovesOf(s)
	state := txn.beginBoundedApply()
	state.journal = j
	// The document points at the journal's tally for the whole apply: the
	// decoders count against it through the document, since they are shared
	// with the merge and snapshot decoders, which have no apply
	// (applyTally). endRemoteApply takes the pointer back.
	d.applying = &j.tally
	// The callbacks run after the journal, so its rollback refuses an apply
	// whose callback panics, and their answers are read once, here, for the
	// whole apply.
	state.scanLimit = d.applyScanLimit(txn.Origin)
	if d.applyBudget != nil {
		j.tally.budget = d.callApplyBudget(txn.Origin)
	}
	if d.allowedRoots != nil {
		j.tally.roots = d.callAllowedRoots(txn.Origin)
		// The names the document holds are resolved to their types once,
		// here, so each item the apply integrates or deletes is checked by
		// walking up to its root and looking it up (checkItemRoot). A name
		// the document does not hold yet stays allowed by name: the apply
		// may be what creates it (decodeItem's refuseRootOnBoundedDoc).
		if len(j.tally.roots) > 0 {
			j.tally.allowedTypes = make(map[*abstractType]bool, len(j.tally.roots))
			for _, name := range j.tally.roots {
				if shared, held := d.share[name]; held {
					j.tally.allowedTypes[shared.baseType()] = true
				}
			}
		}
	}
}

// pendingMovesAt is the store's pending moves as a point of an apply found
// them: each client's heap copied, since resolvePendingMoves pops it in place,
// with the counts the store keeps of them.
type pendingMovesAt struct {
	moves                  map[ClientID]pendingMoveHeap
	count, deleted, mapCap int
}

func pendingMovesOf(s *StructStore) pendingMovesAt {
	p := pendingMovesAt{count: s.pendingMoveCount, deleted: s.deletedPendingMoves, mapCap: s.pendingMoveMapCap}
	if s.pendingMoves != nil {
		p.moves = make(map[ClientID]pendingMoveHeap, len(s.pendingMoves))
		for c, h := range s.pendingMoves {
			p.moves[c] = append(pendingMoveHeap(nil), h...)
		}
	}
	return p
}

// restore puts the store's pending moves back as p found them.
func (p pendingMovesAt) restore(s *StructStore) {
	s.pendingMoves, s.pendingMoveCount, s.deletedPendingMoves, s.pendingMoveMapCap = p.moves, p.count, p.deleted, p.mapCap
}

// parkedMissing is the pending queue's missing map, for a write that parks an
// item. On a document with a scan bound the journal shares the map the apply
// found, so the apply's first write goes to a copy of it.
func parkedMissing(txn *Transaction) StateVector {
	p := txn.doc.store.pending
	if j := txn.applyJournal(); j != nil && p == j.pending && !j.missingCopied {
		p.missing = maps.Clone(p.missing)
		j.missingCopied = true
	}
	return p.missing
}

// parkedDeletesAt is the store's pending deletes as a point of an apply found
// them: the set, kept by reference (see the top of this file), the count of
// its ranges (StructStore.pendingDsRanges) and the clients Append gave a
// struct since the last retry (StructStore.pendingDsMoved), copied, since
// Append and retryPendingDs write that set in place.
type parkedDeletesAt struct {
	ds     DeleteSet
	ranges int
	moved  map[ClientID]struct{}
}

func parkedDeletesOf(s *StructStore) parkedDeletesAt {
	return parkedDeletesAt{ds: s.pendingDs, ranges: s.pendingDsRanges, moved: maps.Clone(s.pendingDsMoved)}
}

// restore puts the store's pending deletes back as p found them.
func (p parkedDeletesAt) restore(s *StructStore) {
	s.pendingDs, s.pendingDsRanges, s.pendingDsMoved = p.ds, p.ranges, p.moved
}

// ownPendingDs gives the store's pending deletes a map of their own, and
// copies of the ranges of the clients in names, before a write into them
// during txn's apply on a bounded document: the journal, and the drain, keep
// the set they found by reference, and a merge (parkDeletes) appends to a
// client's ranges and sorts them in place, and a retry takes a client's
// ranges out of the map (StructStore.takeReadyDeletes). It does nothing when
// txn keeps no journal.
func ownPendingDs(txn *Transaction, names map[ClientID][]DeleteRange) {
	if txn.applyJournal() == nil {
		return
	}
	s := txn.doc.store
	shared := s.pendingDs
	s.pendingDs = DeleteSet{clients: maps.Clone(shared.clients)}
	if s.pendingDs.clients == nil {
		s.pendingDs.clients = make(map[ClientID][]DeleteRange)
	}
	if shared.order != nil {
		s.pendingDs.order = append([]ClientID{}, shared.order...)
	}
	for c := range names {
		if ranges, ok := shared.clients[c]; ok {
			s.pendingDs.clients[c] = append([]DeleteRange(nil), ranges...)
		}
	}
}

// parkDeletes merges ds, delete ranges whose targets the store does not hold
// yet, into the store's pending deletes (StructStore.parkDeletes, which keeps
// their count), on a bounded document into a set of its own (ownPendingDs).
// Today no rollback of the whole apply can follow this merge: it is the last
// step before drainParked, which takes back on its own what it does. The copy
// guards only a step that can fail being added after it; keep it for that, and
// do not count on it as more.
func parkDeletes(txn *Transaction, ds DeleteSet) {
	ownPendingDs(txn, ds.clients)
	txn.doc.store.parkDeletes(ds)
}

// refuseRemoteApply is the error of a remote apply that failed with err
// before it changed anything: an update it could not read at its header or at
// a later group it scans first, or one the pending cap refused. Its
// transaction is refused (Transaction.refused), so its commit encodes and
// fires nothing (buildPhase2), on any document. On a bounded document err
// is returned as a RefusedUpdateError, as for an apply the journal took back,
// with no scans to report.
func refuseRemoteApply(txn *Transaction, err error) error {
	if err == nil || !txn.doc.scanBounded() {
		return err
	}
	return &RefusedUpdateError{Err: err}
}

// endRemoteApply ends txn's journal and returns the apply's error err. When a
// bounded document's apply failed it rolls the whole apply back and returns
// err as a RefusedUpdateError carrying what the apply's scans passed and what
// it decoded and added, which no observer reports for a rolled-back
// transaction (buildPhase2).
//
// It raises no refusal of its own. The struct and byte budget is read where
// the apply's struct phase ends (refuseOverBudget), before the delete set
// and before the drain, because a rollback after either cannot give back
// what that step changed.
func endRemoteApply(txn *Transaction, err error) error {
	state := txn.boundedApply
	if state == nil {
		return err
	}
	j := state.journal
	state.journal = nil
	// The apply's counting ends here, before the commit that follows it
	// merges the split halves and before the next transaction begins.
	txn.doc.applying = nil
	if j == nil {
		return err
	}
	if err == nil {
		return err
	}
	j.rollBack(txn)
	return &RefusedUpdateError{
		Err:     err,
		Scanned: txn.scanned,
		Decoded: j.tally.decoded,
		Structs: j.tally.structs,
		Bytes:   j.tally.bytes,
		Split:   j.tally.split,
		Root:    j.tally.root,
	}
}

// drainParked runs drain, the apply's drain of the pending queue and pending
// deletes. On a bounded document it is a phase that can fail on its own: under
// a bound it has an allowance of its own of the bound's size, and under a
// budget it draws on what the apply's allowance has left. One that fails is
// taken back to where it began, the pending queue, pending deletes and pending
// moves are put back as it found them (the queue's clocks raised, parkAgain),
// and the apply goes on as if the drain had not run; the scans it did stay
// counted.
//
// The queue is put back rather than dropped: a parked item is an edit whose
// sender was told it was delivered and which the store's state vector does not
// show, so a dropped one would come back only when its sender next connects,
// and taking the drain back already keeps the drain's scans from refusing the
// update. The drain writes into neither the queue it found nor that queue's
// slice and map (drainPending replaces the queue), and its retry of the
// pending deletes writes into a copy of them (ownPendingDs), so they are kept
// by reference, with their count and moved clients (parkedDeletesAt);
// it resolves pending moves in place, so those are copied. It does change the
// parked items it integrates, and the take-back gives each back what it
// changed: rollBackTo unlinks it and undoes the drain's deletion of it, a
// parked item the drain integrated, trimmed or split gets back the fields the
// drain found it with (keepParked), and parkAgain clears the neighbours and
// the move the drain gave it.
func drainParked(txn *Transaction, drain func()) {
	j := txn.applyJournal()
	if j == nil {
		drain()
		return
	}
	s := txn.doc.store
	point := pointNow(txn, s.StateVector())
	pending, pendingDs, pendingMoves := s.pending, parkedDeletesOf(s), pendingMovesOf(s)
	// The structs a drain takes out of the queue are listed in the park
	// record (transactionParked.left); a drain taken back parks them again,
	// so the list goes back to its length as the drain began.
	leftLen := 0
	if s.parked != nil {
		leftLen = len(s.parked.left)
	}
	j.drain, j.at = &point, &point
	if j.drainOwnAllowance {
		txn.boundedApply.scanFrom = txn.scanned
	}
	defer func() {
		r := recover()
		j.at = &j.start
		if r == nil {
			j.parkedAs = nil
			return
		}
		j.rollBackTo(txn, &point)
		for item, as := range j.parkedAs {
			*item = as
		}
		j.parkedAs = nil
		s.pending = parkAgain(s, pending)
		pendingDs.restore(s)
		pendingMoves.restore(s)
		if s.parked != nil {
			clear(s.parked.left[leftLen:])
			s.parked.left = s.parked.left[:leftLen]
		}
	}()
	drain()
}

// keepQueueFor records, before a park merges arrivals into the queue the
// apply found (pendingUpdate.apply), what that merge changes in place, so a
// rollback gives it back: the queue's slots, which the merge empties and
// compacts in the slice the journal holds by reference, so the journal takes
// its own copy of them once; and each queued struct the merge cuts or grows,
// its content copied. It does nothing for a queue the apply made itself (a
// drain's), or for a plan that changes no queued struct.
func (j *applyJournal) keepQueueFor(p *pendingUpdate, pl mergePlan) {
	if p != j.pending {
		return
	}
	for i, q := range pl.held {
		k := pl.keepHeld[i]
		if k == (mergeKeep{start: q.ID.Clock, end: q.ID.Clock + q.Content.Len()}) {
			continue
		}
		if !j.pendingItemsOwn {
			j.pendingItems, j.pendingItemsOwn = slices.Clone(j.pendingItems), true
		}
		if !k.kept() {
			continue
		}
		if _, kept := j.queuedAs[q.Item]; kept {
			continue
		}
		if j.queuedAs == nil {
			j.queuedAs = make(map[*Item]Item)
		}
		as := *q.Item
		as.Content = q.Content.Copy()
		j.queuedAs[q.Item] = as
	}
}

// parkAgain is found, the pending queue as a drain found it, for after the
// drain was taken back: the same items in the same order, without the
// neighbours and move the drain gave them, waiting on each client found waits
// on at the later of the clock found holds for it and the store's next clock.
//
// The clocks are raised so that a drain the apply's allowance could not hold
// is not paid again by every later apply. A drain tries the queue's items only
// while the store's next clock for a client the queue waits on has passed the
// clock held for it. Taken back, a drain leaves the store at the clocks it
// began at, which have passed found's - the update that filled the gap passed
// them - so found's clocks would let every later apply try the items again and
// pay again for a drain that integrates nothing. Raised, the items are tried
// again only by an apply that brings one of the queue's clients past the clock
// the store held for it at the take-back, the rule every drain runs under.
// Such an apply pays for the drain from what its own allowance leaves under a
// budget, or from the drain's own under a bound, so no apply scans past its
// limit; and it completes the drain when that allowance holds it, or when the
// apply carries the parked items itself, as their sender does when it
// connects again: the apply integrates them under its own allowance and the
// drain skips each as already integrated (tryIntegrate).
func parkAgain(s *StructStore, found *pendingUpdate) *pendingUpdate {
	if found == nil {
		return nil
	}
	items := found.heldItems()
	for _, item := range items {
		item.Left, item.Right, item.MovedBy = nil, nil, nil
	}
	missing := make(StateVector, len(found.missing))
	for client, clock := range found.missing {
		missing[client] = max(clock, s.NextClock(client))
	}
	return &pendingUpdate{items: items, missing: missing}
}

// keepParked records item, a parked item or a struct the running drain added,
// as the drain found it, its content copied, before the drain changes it in
// place, so a drain taken back can give it back (drainParked). It does nothing
// outside a drain or for an item it recorded already. A drain changes a parked
// item in two places: tryIntegrate, which resolves, trims and integrates it,
// and a split of an item it integrated, which leaves the item's first part in
// it (splitItem, drained).
func (j *applyJournal) keepParked(item *Item) {
	if j.drain == nil || j.at != j.drain {
		return
	}
	if _, kept := j.parkedAs[item]; kept {
		return
	}
	if j.parkedAs == nil {
		j.parkedAs = make(map[*Item]Item)
	}
	as := *item
	as.Content = item.Content.Copy()
	j.parkedAs[item] = as
}

// inDrain is whether the apply is running its drain of the pending queue, the
// phase whose structs are not the update's: they are what earlier updates
// parked, and a drain that fails is taken back on its own while the update
// stands (drainParked). A nil journal is no apply at all and no drain.
func (j *applyJournal) inDrain() bool {
	return j != nil && j.drain != nil && j.at == j.drain
}

// drained is whether item is a struct the running drain added to the store: a
// parked item it integrated, or a part split from one. A struct the store held
// before the drain keeps a split the drain made (see the top of this file).
func (j *applyJournal) drained(item *Item) bool {
	return j.inDrain() && item.ID.Clock >= j.drain.sv.Clock(item.ID.Client)
}

// tookMapKey records that a new item is about to replace the value of key in
// parent, the first time since the current point that key changes.
func (j *applyJournal) tookMapKey(parent *abstractType, key string) {
	slot := mapSlot{parent, key}
	if j.at.mapPrev == nil {
		j.at.mapPrev = make(map[mapSlot]*Item)
	}
	if _, seen := j.at.mapPrev[slot]; !seen {
		j.at.mapPrev[slot] = parent.itemMap[key]
	}
}

// tookFormatting records parent's formatting flags as they stand, the first
// time since the current point that the apply changes them: a ContentFormat
// it integrates (Item.integrate) or deletes (Item.delete) raises them, and
// nothing lowers them again, so a refused formatting update would otherwise
// leave a plain text on the formatting-aware path for the rest of the
// document's life.
func (j *applyJournal) tookFormatting(parent *abstractType) {
	if j.at.fmtPrev == nil {
		j.at.fmtPrev = make(map[*abstractType]fmtFlags)
	}
	if _, seen := j.at.fmtPrev[parent]; !seen {
		j.at.fmtPrev[parent] = fmtFlags{parent.hasFormatting, parent.cleanFormatting, parent.fmtGen}
	}
}

// noteFormatting records parent's formatting flags with txn's apply, where
// the apply is about to change them (tookFormatting). It does nothing
// outside an apply that can be rolled back.
func noteFormatting(txn *Transaction, parent *abstractType) {
	if j := txn.applyJournal(); j != nil {
		j.tookFormatting(parent)
	}
}

// rollBack leaves txn's document as it was when the journal began, and makes
// the transaction commit as one that changed nothing. It runs under the
// document lock, before the transaction commits.
func (j *applyJournal) rollBack(txn *Transaction) {
	if j.drain != nil {
		// The drain's own map keys and formatting flags go back first, to
		// what they were when the drain began, which is what the start's
		// record then takes back further.
		j.restoreMapKeys(j.drain, map[*abstractType]struct{}{})
		j.drain.mapPrev = nil
		j.restoreFormatting(j.drain)
		j.drain.fmtPrev = nil
	}
	j.rollBackTo(txn, &j.start)

	s := txn.doc.store
	for target, mover := range txn.movedBefore {
		target.MovedBy = mover
		if target.Parent != nil {
			target.Parent.clearMarkers()
		}
	}
	if j.pending != nil {
		// What the apply appended past the length kept is dropped, and its
		// slots cleared so the backing array holds none of the refused items.
		clear(j.pendingItems[len(j.pendingItems):cap(j.pendingItems)])
		j.pending.items, j.pending.dead = j.pendingItems, j.pendingDead
		for item, as := range j.queuedAs {
			*item = as
		}
		j.pending.missing = j.missing
		j.pending.ready = j.pendingReady
		// The apply's parks added their clocks to the queue's cover in place
		// (addCover), and it holds none of them now.
		j.pending.cover = nil
		j.pending.tail, j.pending.tailValid = j.pendingTail, j.pendingTailValid
	}
	s.pending = j.pending
	j.pendingDs.restore(s)
	j.pendingMoves.restore(s)

	txn.subdocsAdded, txn.subdocsRemoved, txn.subdocsLoaded = nil, nil, nil
	txn.rearbitrate, txn.movedBefore = nil, nil
	txn.boundedApply.rolledBack = true
}

// rollBackTo returns txn's document to point: what was added, deleted and
// named since it is taken out, and the transaction's record of what changed is
// what it was there.
func (j *applyJournal) rollBackTo(txn *Transaction, point *applyPoint) {
	d := txn.doc
	s := d.store
	touched := make(map[*abstractType]struct{})

	// The struct integrate linked and did not get to append.
	if x := j.integrating; x != nil {
		if x.Parent != nil && x.ID.Clock >= point.sv.Clock(x.ID.Client) {
			unlinkItem(x, touched)
		}
		j.integrating = nil
	}

	// Every struct added since point: unlinked, and dropped from the store.
	for client, items := range s.clients {
		start := point.sv.Clock(client)
		cut := sort.Search(len(items), func(i int) bool { return items[i].ID.Clock >= start })
		for _, x := range items[cut:] {
			if x.Parent != nil {
				unlinkItem(x, touched)
			}
			// An item the pending queue gets back is parked again as it was
			// taken: not deleted, its neighbours to be found on integration.
			if x.Parent != nil && x.Deleted && !point.deleted.IsDeleted(x.ID) && txn.deleteSet.IsDeleted(x.ID) {
				x.Deleted = false
			}
		}
		clear(items[cut:])
		if cut == 0 {
			delete(s.clients, client)
		} else {
			s.clients[client] = items[:cut]
		}
	}

	j.restoreMapKeys(point, touched)

	// Items held at point that were deleted since come back.
	for client, ranges := range txn.deleteSet.clients {
		start := point.sv.Clock(client)
		items := s.clients[client]
		for _, r := range ranges {
			end := min(r.Clock+r.Len, start)
			if r.Clock >= end {
				continue
			}
			i := sort.Search(len(items), func(i int) bool {
				it := items[i]
				return it.ID.Clock+it.Content.Len() > r.Clock
			})
			for ; i < len(items) && items[i].ID.Clock < end; i++ {
				x := items[i]
				if !x.Deleted || point.deleted.IsDeleted(x.ID) {
					continue
				}
				x.Deleted = false
				// A deleted item carrying a subdocument detached it
				// (Item.delete); alive again, it holds it again, or the
				// subdocument would stay attached to nothing while the
				// document still shows it (Doc.reattachSubdoc).
				if cd, ok := x.Content.(*ContentDoc); ok && cd.Doc != nil {
					cd.Doc.reattachSubdoc(x, d)
				}
				if x.Parent != nil {
					if x.Content.IsCountable() {
						x.Parent.length += nativeContentLen(x.Content)
					}
					touched[x.Parent] = struct{}{}
				}
			}
		}
	}

	// Root types named since point.
	for name := range d.share {
		if _, held := point.roots[name]; !held {
			delete(d.share, name)
		}
	}

	j.restoreFormatting(point)

	for parent := range touched {
		parent.clearMarkers()
		parent.invalidateFirstLiveCache()
	}

	merge := txn.mergeStructs[:0]
	for _, half := range txn.mergeStructs {
		if half.ID.Clock < point.sv.Clock(half.ID.Client) {
			merge = append(merge, half)
		}
	}
	txn.mergeStructs = merge
	txn.changed = point.changed
	clear(txn.newItems[point.newItems:])
	txn.newItems = txn.newItems[:point.newItems]
	txn.deleteSet = point.deleted
}

// restoreMapKeys gives each map key a new item took since point its value
// there back.
func (j *applyJournal) restoreMapKeys(point *applyPoint, touched map[*abstractType]struct{}) {
	for slot, prev := range point.mapPrev {
		if prev == nil {
			delete(slot.parent.itemMap, slot.key)
		} else {
			slot.parent.itemMap[slot.key] = prev
		}
		touched[slot.parent] = struct{}{}
	}
}

// restoreFormatting gives each type whose formatting flags the apply raised
// since point the flags it had there (tookFormatting).
func (j *applyJournal) restoreFormatting(point *applyPoint) {
	for t, as := range point.fmtPrev {
		t.hasFormatting, t.cleanFormatting, t.fmtGen = as.has, as.clean, as.gen
	}
}

// unlinkItem takes x out of its parent's list, with what it added to the
// parent's length, and records the parent as touched.
func unlinkItem(x *Item, touched map[*abstractType]struct{}) {
	p := x.Parent
	if x.Left != nil {
		x.Left.Right = x.Right
	} else if p.start == x {
		p.start = x.Right
	}
	if x.Right != nil {
		x.Right.Left = x.Left
	}
	if !x.Deleted && x.Content.IsCountable() {
		p.length -= nativeContentLen(x.Content)
	}
	x.Left, x.Right = nil, nil
	touched[p] = struct{}{}
}

// copyDeleteSet copies ds as it is, ranges and order included (cloneDeleteSet
// also sorts and compacts it).
func copyDeleteSet(ds DeleteSet) DeleteSet {
	out := DeleteSet{clients: make(map[ClientID][]DeleteRange, len(ds.clients))}
	for c, ranges := range ds.clients {
		out.clients[c] = append([]DeleteRange(nil), ranges...)
	}
	if ds.order != nil {
		out.order = append([]ClientID{}, ds.order...)
	}
	return out
}

// errUnboundableContent refuses, on a document with a scan bound, a remote
// update carrying a subdocument or a move, whose integration reaches outside
// the document's lists where a rollback cannot follow (applyJournal).
var errUnboundableContent = fmt.Errorf("%w: a document with a scan bound takes no subdocument or move from a remote update", ErrInvalidUpdate)

// refuseOnBoundedDoc is errUnboundableContent for content a remote update
// decoded into doc carries, when doc is bounded; nil otherwise.
func refuseOnBoundedDoc(doc *Doc, content Content) error {
	if !doc.scanBounded() {
		return nil
	}
	switch content.(type) {
	case *ContentDoc, *ContentMove:
		return errUnboundableContent
	}
	return nil
}

// ErrApplyBudget is why a document with an apply budget (WithApplyBudget)
// refuses a remote update that decodes or integrates more than the apply's
// budget allows. It wraps ErrInvalidUpdate, and the apply returns it inside a
// RefusedUpdateError reporting what the refused apply reached: like every
// remote update a bounded document refuses, the update leaves the document as
// it was.
var ErrApplyBudget = fmt.Errorf("%w: it decodes or adds more than the document allows", ErrInvalidUpdate)

// ErrRootNotAllowed is why a document with an allowed-roots list
// (WithAllowedRoots) refuses a remote update naming a root outside it. It
// wraps ErrInvalidUpdate, and the apply returns it inside a
// RefusedUpdateError whose Root is the name the update named.
var ErrRootNotAllowed = fmt.Errorf("%w: it names a root the document does not allow", ErrInvalidUpdate)

// applyTally is what one remote apply has decoded and added, and the
// allowance it may do so within: the budget WithApplyBudget answered for the
// apply's origin and the roots WithAllowedRoots allowed it, both read once as
// the apply began (beginRemoteApply).
//
// The counts are what RefusedUpdateError reports for a refused apply, since
// no observer reports a rolled-back transaction, and a document with any
// bound keeps them whether or not a budget bounds them: the counting is an
// add and a compare where the apply was already decoding or linking a struct.
//
// Every method tolerates a nil receiver, which is an apply with no tally -
// one on a document with no bound, or a decode with no apply at all, such as
// the merge decoders' - so that the decoders count without a guard around
// each value.
type applyTally struct {
	// budget is what the apply may decode and add, noApplyBudget when the
	// document has no budget callback: a field of the budget below zero is no
	// limit, and zero allows nothing (WithApplyBudget).
	budget ApplyBudget
	// roots is what the update may name, or nil or empty for every root.
	// allowedTypes are those of roots the document holds, resolved as the
	// apply began, and the ones it names during the apply that the list
	// allows; rootOf memoises the root above each type an item is parented
	// to, and one walk fills it for every type the walk passed
	// (checkItemRoot).
	roots        []string
	allowedTypes map[*abstractType]bool
	rootOf       map[*abstractType]*abstractType
	// decoded, structs, bytes and split are the counts of WithApplyBudget;
	// root is the root a refused apply reached that roots does not allow,
	// empty when the refused item hangs under no root of the document.
	decoded, structs, bytes, split int
	root                           string
}

// noApplyBudget is the budget of an apply on a document with no budget
// callback: no limit in any measure, so the counts are kept and reported and
// nothing is refused for them (WithApplyBudget).
var noApplyBudget = ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1}

// tally is the running tally of the remote apply on d, or nil when no apply
// is running on it, when d bounds nothing, or when d itself is nil, which a
// decode outside an apply passes (decodeContent).
func (d *Doc) tally() *applyTally {
	if d == nil {
		return nil
	}
	return d.applying
}

// decode counts n values the apply decoded, and refuses it once they pass the
// budget, by the panic the apply recovers as ErrApplyBudget
// (recoveredApplyErr), since the count is checked inside the decode loop,
// where the allocation it bounds happens: a check after the decode would run
// once the memory was already taken.
func (t *applyTally) decode(n int) {
	if t == nil {
		return
	}
	t.decoded += n
	if t.budget.Decoded >= 0 && t.decoded > t.budget.Decoded {
		panic(applyBudgetExceeded{})
	}
}

// value counts v, one decoded lib0 Any or JSON value, and everything inside
// it: the value itself, each element of an array and each entry of a map, at
// any depth, so a frame whose values nest deeply is counted by what it holds
// rather than by the one value it holds it in. The recursion is bounded by
// the depth the decoders accept (anycodec.MaxDepth).
//
// It walks a value the decode has already built, so it is used only where
// the declared length that sized that value was charged first
// (decodeContent); a lib0 Any is counted off the frame instead, before
// anything is allocated for it (countAnyRun).
func (t *applyTally) value(v any) {
	if t == nil {
		return
	}
	t.decode(1)
	t.nested(v)
}

// nested counts what v holds and not v itself, for a value whose own slot was
// charged with the declared length of the list it arrived in.
func (t *applyTally) nested(v any) {
	switch v := v.(type) {
	case []any:
		for _, e := range v {
			t.value(e)
		}
	case map[string]any:
		for _, e := range v {
			t.value(e)
		}
	}
}

// nestedAll counts what each of vals holds, the values of one content whose
// own slots the declared length already charged.
func (t *applyTally) nestedAll(vals []any) {
	if t == nil {
		return
	}
	for _, v := range vals {
		t.nested(v)
	}
}

// integrated counts item, a struct of the update the apply has just added to
// the store, with the bytes of content it carries.
func (t *applyTally) integrated(item *Item) {
	if t == nil {
		return
	}
	t.structs++
	t.bytes += contentBytes(item.Content)
}

// integratedErr is ErrApplyBudget when the apply integrated more structs or
// more bytes than its budget allows, and nil otherwise.
func (t *applyTally) integratedErr() error {
	if t == nil {
		return nil
	}
	if t.budget.Structs >= 0 && t.structs > t.budget.Structs {
		return ErrApplyBudget
	}
	if t.budget.Bytes >= 0 && t.bytes > t.budget.Bytes {
		return ErrApplyBudget
	}
	return nil
}

// refuseOverBudget is ErrApplyBudget when txn's apply has integrated more
// structs or more bytes than its budget allows, and nil otherwise. The
// applies call it where their struct phase ends, before the delete set is
// decoded and before the pending queue drains (applyV1Txn, applyV2Txn).
//
// The counts are final there: the delete set integrates no struct of its
// own, and the structs it makes by splitting items the document already held
// are reported as Split, which no budget bounds. Reading the budget there
// and nowhere later is what keeps two whole classes of damage out of a
// refusal. A refusal after the delete set had run would have to give back a
// subdocument the delete detached from its parent, and a refusal after the
// drain cannot give back what the drain changed in the parked items it
// integrated - a parked "BC" it trimmed to "C" stays trimmed - so this node
// would keep a document every peer disagrees with. Neither is reachable
// through ErrApplyBudget when the budget is read before both
// (TestUnit_ApplyBudget_ARefusalAfterTheDrainKeepsWhatTheDocumentHeld).
func refuseOverBudget(txn *Transaction) error {
	j := txn.applyJournal()
	if j == nil {
		return nil
	}
	return j.tally.integratedErr()
}

// noteIntegrated records item with txn's apply, where the apply adds a struct
// of its update to the store (Item.integrate, integrateGroup, appendTrimmed):
// it refuses the apply when the item lands under a root the document's
// allowed-roots list does not name (checkItemRoot), and counts the struct and
// its bytes against the budget.
//
// A struct the drain of the pending queue adds is not the update's and is not
// counted: refusing the update for what an earlier update parked would let
// one sender have every later update that fills its gap refused
// (drainParked). The root check does run for it, since what the list keeps
// out of a root must stay out of it however it arrives; a drain the check
// refuses is taken back on its own and its items stay parked.
func noteIntegrated(txn *Transaction, item *Item) {
	t := txn.doc.tally()
	if t == nil {
		return
	}
	t.checkItemRoot(txn.doc, item.Parent)
	if !txn.applyJournal().inDrain() {
		t.integrated(item)
	}
}

// noteDeleted refuses txn's apply when it deletes an item under a root the
// document's allowed-roots list does not name (Item.delete). Deleting is not
// counted against the budget - it adds nothing to the document but the splits
// the range's edges make, which countSplit reports - and it is bounded by the
// delete ranges the apply decoded (countDecodedRanges).
func noteDeleted(txn *Transaction, item *Item) {
	if t := txn.doc.tally(); t != nil {
		t.checkItemRoot(txn.doc, item.Parent)
	}
}

// checkItemRoot refuses the apply, by the panic it recovers as
// ErrRootNotAllowed (recoveredApplyErr), when parent - the type an item the
// apply integrates or deletes hangs under - is under a root the apply's list
// does not name.
//
// The list is a containment rule over the whole update, not a rule about the
// names the update writes on the wire: an item written beside items the
// document already holds carries an origin and names no root at all, and a
// delete range names nothing either, so a check only where the decode
// resolves a name (refuseRootOnBoundedDoc) would let a sender overwrite and
// delete inside a root it may not touch. That check stays: it refuses earlier
// and names the root exactly.
//
// The walk up the parents is memoised per type, and one walk answers a whole
// chain: it climbs only to the first ancestor the apply has already answered
// and records the root it reached for every type it passed, so an update of
// n items nested n deep costs one walk rather than one for each item. A type
// whose parents do not reach a root the document holds - the content of a
// container the GC collected, say - is refused too: the owner cannot have
// allowed what is under no root of theirs.
func (t *applyTally) checkItemRoot(d *Doc, parent *abstractType) {
	// A GC struct hangs under no type at all: it is a placeholder for clocks
	// whose content is gone, and it adds nothing to any root.
	if len(t.roots) == 0 || parent == nil {
		return
	}
	root, known := t.rootOf[parent]
	if !known {
		var chain []*abstractType
		root = parent
		for {
			if above, seen := t.rootOf[root]; seen {
				root = above
				break
			}
			chain = append(chain, root)
			if root.item == nil || root.item.Parent == nil {
				break
			}
			root = root.item.Parent
		}
		if t.rootOf == nil {
			t.rootOf = make(map[*abstractType]*abstractType, len(chain))
		}
		for _, passed := range chain {
			t.rootOf[passed] = root
		}
	}
	if t.allowedTypes[root] {
		return
	}
	name, isRoot := rootName(d, root)
	if root.item == nil && isRoot && slices.Contains(t.roots, name) {
		// A root the document did not hold when the apply began, which this
		// apply named and the name check let through.
		t.allowedTypes[root] = true
		return
	}
	t.root = name
	panic(rootNotAllowed{})
}

// rootName is the name d holds root under, and false for a type that is no
// root of d. A root carries its own name once it has been named, and the
// reverse lookup covers one that has not; the empty name is a name like any
// other, which is why the lookup says whether it found one rather than
// answering "" for both. It runs only where an apply is refused or a root the
// apply itself named is reached.
func rootName(d *Doc, root *abstractType) (string, bool) {
	if root.name != "" {
		return root.name, true
	}
	for name, shared := range d.share {
		if shared.baseType() == root {
			return name, true
		}
	}
	return "", false
}

// countSplit counts the struct a split added, where the apply splits an item
// (splitItem). A split is reported and never bounded (WithApplyBudget), and
// like an integrated struct it is counted only outside the drain.
func countSplit(txn *Transaction) {
	if t := txn.doc.tally(); t != nil && !txn.applyJournal().inDrain() {
		t.split++
	}
}

// countDecodedRanges counts n, the delete ranges the apply read, each a value
// it decoded, and so refuses an update that carries more of them than its
// budget allows before one of them deletes or splits anything.
//
// n is the ranges the decode read off the frame, not the ranges the set it
// returned holds: the decode sorts each client's ranges and merges the ones
// that touch, so a frame of a hundred thousand ranges that compact to one
// would otherwise be counted as one. The decode counts them in its first
// pass, before it allocates a list for any of them (decodeDeleteSet).
//
// What the count bounds is the work the ranges make the document do: each
// range can split a struct the document holds, and a split adds a struct the
// Structs budget does not count.
func countDecodedRanges(txn *Transaction, n int) {
	txn.doc.tally().decode(n)
}

// refuseRootOnBoundedDoc is ErrRootNotAllowed for name, a root type a remote
// apply's decode is about to resolve on doc, when doc's allowed roots do not
// list it; nil otherwise, which includes every decode outside an apply and
// every apply whose list is empty (WithAllowedRoots). It records the name, so
// the refusal reports which root the update named
// (RefusedUpdateError.Root).
func refuseRootOnBoundedDoc(doc *Doc, name string) error {
	t := doc.tally()
	if t == nil || len(t.roots) == 0 || slices.Contains(t.roots, name) {
		return nil
	}
	t.root = name
	return ErrRootNotAllowed
}

// anyScalarBytes is what a decoded lib0 Any scalar - a number, a boolean,
// null, or the slot an element of an array or an entry of a map takes - is
// counted as: the two words of an interface value, which is what it costs the
// document to hold it.
const anyScalarBytes = 16

// contentBytes is the bytes of content a struct carries: its text, its binary
// and the values it decoded, not the item around them, whose cost the Structs
// count bounds. A deleted range carries none (it is a length). A type carries
// only the name an XML element or hook keeps - it is otherwise an empty
// container the structs under it fill, each counted as it integrates - and
// that name is as long as its sender made it. A subdocument and a move carry
// none either: a bounded document takes neither from a remote update
// (refuseOnBoundedDoc).
func contentBytes(c Content) int {
	switch c := c.(type) {
	case *ContentString:
		return len(c.Str)
	case *ContentBinary:
		return len(c.Data)
	case *ContentAny:
		return valsBytes(c.Vals)
	case *ContentJSON:
		return valsBytes(c.Vals)
	case *ContentEmbed:
		return anyBytes(c.Val)
	case *ContentFormat:
		return len(c.Key) + anyBytes(c.Val)
	case *ContentType:
		return typeNameBytes(c.Type)
	}
	return 0
}

// typeNameBytes is the bytes of the name a type carries of its own: the node
// name of an XML element and the name of an XML hook, both of them written on
// the wire by their sender and kept by the document for as long as it holds
// the type. Every other type carries no name.
func typeNameBytes(t *abstractType) int {
	if t == nil {
		return 0
	}
	switch owner := t.owner.(type) {
	case *YXmlElement:
		return len(owner.NodeName)
	case *yxmlHook:
		return len(owner.hookName)
	}
	return 0
}

// valsBytes is the bytes of the values one content holds.
func valsBytes(vals []any) int {
	n := 0
	for _, v := range vals {
		n += anyBytes(v)
	}
	return n
}

// anyBytes is the bytes a decoded value holds: text and binary as they are, a
// scalar as the words it takes, and a container as the sum of its parts with
// a slot for each. It measures what the value costs the document to hold, not
// what it took on the wire, which the frame's own length bounds.
func anyBytes(v any) int {
	switch v := v.(type) {
	case string:
		return len(v)
	case []byte:
		return len(v)
	case []any:
		n := 0
		for _, e := range v {
			n += anyScalarBytes + anyBytes(e)
		}
		return n
	case map[string]any:
		n := 0
		for k, e := range v {
			n += anyScalarBytes + len(k) + anyBytes(e)
		}
		return n
	}
	return anyScalarBytes
}
