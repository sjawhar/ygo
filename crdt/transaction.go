package crdt

import (
	"cmp"
	"context"
	"slices"
	"sort"
	"strings"
)

// Transaction batches a set of insertions and deletions into a single atomic
// operation. Observers fire once per transaction, not once per operation,
// which keeps event handler overhead proportional to transactions not edits.
type Transaction struct {
	doc    *Doc
	Origin any  // user-supplied tag forwarded to update observers
	Local  bool // true when the change originated on this peer; false while applying a remote update
	// done is set (under d.mu, just before the commit releases it) once this
	// transaction has committed. The transaction-scoped root accessors check
	// it and fail loudly instead of silently mutating d.share without the
	// lock: the realistic misuses — an OnAfterTransaction observer (which
	// receives the *Transaction after unlock) or code retaining the txn past
	// its callback — run on the same goroutine that committed, so program
	// order guarantees they observe done == true. A transaction smuggled to
	// another goroutine while still live is unaffected (that is already
	// undefined for every txn method); for one used cross-goroutine after
	// commit the check is best-effort. It sits beside Local so the two share
	// a word.
	done        bool
	deleteSet   DeleteSet
	beforeState StateVector
	afterState  StateVector
	// changed tracks which types were modified, and what about each. Add
	// entries only through addChanged, which records the order too.
	changed map[*abstractType]changedType
	// newItems collects ContentString items integrated during this transaction.
	// Used by squashRuns to merge adjacent same-client runs after observers fire.
	newItems []*Item
	// mergeStructs collects right-halves produced by splitItem during this
	// transaction. tryMergeWithLefts walks the slice at commit and re-merges
	// each entry with its left neighbour when the split turned out to be
	// transient (no item was inserted between them). Mirrors Yjs JS's
	// `_mergeStructs` and powers gap #78 H2.
	mergeStructs []*Item
	// scanned counts the items the conflict scans of this transaction's
	// integrates have passed (IntegrateScanned), for every transaction of
	// every document, so it is a field of its own: a bounded apply's limit is
	// checked against it (boundedApplyState.scanLimit, scanFrom).
	scanned int
	// boundedApply holds a bounded remote apply's scan and rollback state, so
	// other transactions do not carry it.
	boundedApply *boundedApplyState
	// rearbitrate queues, per parent, move targets whose winning ContentMove
	// was tombstoned; rearbitrateMoves resolves them in one pass at commit.
	rearbitrate map[*abstractType]map[*Item]struct{}
	// movedBefore holds each move target's MovedBy as it was before this
	// transaction first changed it, so YArray deltas can diff the old render.
	movedBefore map[*Item]*Item
	// subdocsAdded/subdocsRemoved/subdocsLoaded track subdocument lifecycle
	// changes made during this transaction (#63). Populated by Item.integrate
	// and Item.delete when the item's Content is a *ContentDoc. Reconciled
	// into Doc.subdocs and turned into a SubdocsEvent by buildPhase2 at commit.
	// An add followed by a delete of the same doc within the same transaction
	// cancels out (removed from subdocsAdded/subdocsLoaded, never added to
	// subdocsRemoved) so no event fires for a net no-op.
	subdocsAdded   map[*Doc]struct{}
	subdocsRemoved map[*Doc]struct{}
	subdocsLoaded  map[*Doc]struct{}
	// ctx is the context associated with this transaction. Set to
	// context.Background() by Transact and to the caller's ctx by
	// TransactContext. Exposed via the Ctx() method so fn can poll for
	// cancellation.
	ctx context.Context
	// cleanup is a remote transaction's format cleanup decision, which
	// afterRemoteObservers reads once the observers have fired;
	// cleanupAfterTypeObservers defers it until the type observers
	// (typeObserved: there are some) have run, where Yjs makes it.
	// cleanupSkipped holds the changed types that take no part in it: those
	// deleted at commit, and texts whose own observers panicked.
	cleanup        cleanupDecision
	typeObserved   bool
	cleanupSkipped map[*abstractType]struct{}
	// landed, when set, receives ApplyUpdateV1Landed's update of what this
	// failed apply integrated, encoded after the GC as OnUpdate's update is.
	landed *[]byte
	// deletedFormats holds, per client and sorted, the clocks of the format
	// markers this remote transaction deleted, recorded for the format
	// cleanup before the GC.
	deletedFormats map[ClientID][]uint64
	// refused marks a remote apply that failed before integrating, so the
	// commit fires nothing.
	refused bool
}

// transactionParked is what one transaction parked, which a failed apply's
// landed update carries (encodeTxnV1Locked); each document keeps one, since
// its transactions run one at a time.
type transactionParked struct {
	items   []*Item     // the first run is a view of the pending queue
	deletes []DeleteSet // sharing ranges that nothing changes
	// left is what a drain took out of the queue after the park, so the
	// structs still parked are found without a pass over the queue; a merge
	// runs before the transaction's one park and cannot take them.
	left []*Item
}

// parkRecord allocates the document's record only when a transaction parks.
func (t *Transaction) parkRecord() *transactionParked {
	s := t.doc.store
	if s.parked == nil {
		s.parked = &transactionParked{}
	}
	return s.parked
}

// noteParked keeps the first appended run as a queue view to avoid copying it.
func (t *Transaction) noteParked(run []*Item) {
	p := t.parkRecord()
	if p.items == nil {
		p.items = run
		return
	}
	p.items = append(p.items, run...)
}

// noteParkedDeletes records ds, which this transaction parked.
func (t *Transaction) noteParkedDeletes(ds DeleteSet) {
	p := t.parkRecord()
	p.deletes = append(p.deletes, ds)
}

// changesDoc reports whether t deleted something or added structs, the
// condition under which Yjs emits a transaction's update. It reads the
// store, so it holds inside the transaction as well as at commit.
func (t *Transaction) changesDoc() bool {
	if len(t.deleteSet.clients) > 0 {
		return true
	}
	for client := range t.doc.store.clients {
		if t.doc.store.NextClock(client) != t.beforeState.Clock(client) {
			return true
		}
	}
	return false
}

// Ctx returns the context associated with this transaction. Transactions
// started via Transact return context.Background(); transactions started
// via TransactContext return the caller's ctx. fn can poll Ctx().Err()
// or <-Ctx().Done() to detect cancellation and return early.
//
// Returning early from fn commits whatever mutations have been made so
// far — there is no rollback. Callers needing atomicity should recover
// and reconcile via sync or recreate the doc from persistence.
func (t *Transaction) Ctx() context.Context {
	return t.ctx
}

// IntegrateScanned is the number of items the conflict scans of the
// transaction's integrates passed (Item.integrate), the drain of the pending
// queue included, also when a bounded document took the drain back: what
// WithMaxIntegrateScan and WithIntegrateScanBudget bound for a remote update,
// counted for every transaction of every document, bounded or not. Read from
// an OnAfterTransaction observer, which runs after the document's lock is
// released and before ApplyUpdate returns, it is the scan work an applied
// remote update cost: under a budget at most what the budget gave the apply,
// and under a bound at most the bound for the update and the bound again for
// its drain. A remote update a bounded document refuses commits no
// transaction an observer sees; RefusedUpdateError.Scanned reports its scans.
func (t *Transaction) IntegrateScanned() int {
	return t.scanned
}

// ChangedTypes returns the shared types the transaction changed: every type an
// item was integrated into or deleted from, as that item's direct parent, once
// per type and in no particular order. A change under a nested type names that
// type, not the root above it; walk Parent from an entry to find the
// containers it sits in.
//
// It is not the key set of Yjs `transaction.changed`, which leaves out every
// type the transaction created and every type whose own item is deleted.
// ChangedTypes names those too: a type the transaction created, whose own
// content integrates into it; a type it deleted, whose content the delete
// cascades into; and a type deleted before the transaction that a concurrent
// remote change still writes into, while the GC has not collected it (once it
// has, the write integrates as a GC struct, as in Yjs). A caller that wants only live types that
// existed before the transaction filters these entries out: a type whose own
// item is deleted no longer appears among its Parent's children or values.
//
// It never names an internal placeholder, a root nobody has opened by name
// (Doc.GetXmlFragment, Doc.GetMap, …): a change to it is left out, and once
// the root is opened, later transactions name the type its Get* returned. A
// Yjs YXmlHook is named as the value YXmlFragment.Children lists for it.
//
// ChangedTypes does not take the document lock. OnAfterTransaction observers
// run after the transaction released the lock, and reading ChangedTypes there
// is safe: it reads only the committed transaction. Walking Parent from an
// entry there, or anywhere outside a Transact callback, races any other
// goroutine writing the document; it is safe inside a Transact callback, or in
// an observer of a document no other goroutine writes (for example a copy one
// goroutine applies updates to). Each call returns a new slice.
func (t *Transaction) ChangedTypes() []SharedType {
	types := make([]SharedType, 0, len(t.changed))
	for at := range t.changed {
		switch at.owner.(type) {
		case nil, *rawType:
			continue
		}
		types = append(types, at.owner)
	}
	return types
}

// Transaction-scoped root accessors (issue #138).
//
// The Doc-level GetText/GetMap/GetArray/GetXmlFragment take the document
// lock, which Transact already holds for the whole callback — calling them
// inside fn self-deadlocks a non-reentrant lock. These methods resolve (and
// create-on-miss) the same root types WITHOUT re-locking, reusing the lock
// the transaction holds — so the natural in-transaction call simply works,
// mirroring yrs, where root handles are resolved through the transaction.
//
// They are valid ONLY inside the Transact callback that received this
// Transaction (the same lifetime rule as passing txn to Insert/Set/Delete).
// After the transaction commits — in an observer, or via a retained txn —
// they PANIC instead of mutating d.share without the lock.

// assertLive panics when the transaction has already committed. See the
// Transaction.done field for why this is deterministic for the realistic
// (same-goroutine) misuses.
func (t *Transaction) assertLive(method string) {
	if t.done {
		panic("crdt: Transaction." + method + " used after commit (inside an observer or " +
			"retained past the Transact callback) — resolve root types via doc." + method +
			" there (issue #138)")
	}
}

// GetText returns the named root YText, creating it if it does not exist.
// It panics if that name already belongs to a different root type. Safe to
// call inside the Transact callback; see the section comment above.
func (t *Transaction) GetText(name string) *YText {
	checkUTF8("Transaction.GetText", "name", name)
	t.assertLive("GetText")
	return t.doc.getTextLocked(name)
}

// GetMap returns the named root YMap, creating it if it does not exist.
// It panics if that name already belongs to a different root type. Safe to
// call inside the Transact callback; see the section comment above.
func (t *Transaction) GetMap(name string) *YMap {
	checkUTF8("Transaction.GetMap", "name", name)
	t.assertLive("GetMap")
	return t.doc.getMapLocked(name)
}

// GetArray returns the named root YArray, creating it if it does not exist.
// It panics if that name already belongs to a different root type. Safe to
// call inside the Transact callback; see the section comment above.
func (t *Transaction) GetArray(name string) *YArray {
	checkUTF8("Transaction.GetArray", "name", name)
	t.assertLive("GetArray")
	return t.doc.getArrayLocked(name)
}

// GetXmlFragment returns the named root YXmlFragment, creating it if it does
// not exist. It panics if that name already belongs to a different root type.
// Safe to call inside the Transact callback; see the section comment above.
func (t *Transaction) GetXmlFragment(name string) *YXmlFragment {
	checkUTF8("Transaction.GetXmlFragment", "name", name)
	t.assertLive("GetXmlFragment")
	return t.doc.getXmlFragmentLocked(name)
}

// squashRuns merges adjacent ContentString items that were both created in this
// transaction and form a contiguous clock run from the same client.
//
// Safety: only items with ID.Clock >= beforeState.Clock(client) are eligible,
// ensuring pre-existing items (which snapshot clock boundaries reference) are
// never modified.
//
// A merged item is encoded as one struct carrying the left item's Origin and
// OriginRight, so two items are merged only when that struct still places
// every character where it was: the right item's Origin is the last character
// of the run and both items share an OriginRight. These are Yjs's
// Item.mergeWith conditions. Merging items with different right origins makes
// every later encoding move the right item's characters on decode, and makes
// a later insert whose origin is inside the run split it with the wrong
// right origin.
//
// squashRuns runs for remote applies too, so a peer's per-keystroke history
// loads as one item per run, as Yjs's transaction cleanup does.
//
// Performance: uses a two-pointer (run) approach with strings.Builder so that
// string concatenation is O(total_run_length) rather than O(n²), and tracks
// the expected next-clock without calling left.Content.Len() on the growing
// merged string. Store compaction is a single O(n) filter pass per client.
func squashRuns(txn *Transaction) {
	if len(txn.newItems) == 0 {
		return
	}

	// Group new ContentString items by client.
	byClient := make(map[ClientID][]*Item, 4)
	for _, item := range txn.newItems {
		if !item.Deleted {
			byClient[item.ID.Client] = append(byClient[item.ID.Client], item)
		}
	}

	store := txn.doc.store

	// removedByClient collects items squashed into their left neighbour.
	// Items are appended in clock order (squashRuns processes them that way),
	// so the compaction pass can use a two-pointer merge instead of a hash
	// lookup — avoiding 182k map-insert operations on the hot decode path.
	var removedByClient map[ClientID][]*Item

	for client, items := range byClient {
		if len(items) < 2 {
			continue
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].ID.Clock < items[j].ID.Clock
		})
		beforeClock := txn.beforeState.Clock(client)

		i := 0
		for i < len(items) {
			left := items[i]

			// Skip ineligible run starts.
			if left.Deleted || left.ID.Clock < beforeClock {
				i++
				continue
			}

			// Walk j forward to find all items that can be squashed into left.
			// expectedClock tracks the clock boundary at the right edge of the
			// current merged item, updated with each absorbed right item's
			// original Len() — avoiding a call to left.Content.Len() (which is
			// O(string length) and would make the loop O(n²)).
			expectedClock := left.ID.Clock + left.Content.Len()
			var sb strings.Builder
			sb.WriteString(left.Content.(*ContentString).Str)

			j := i + 1
			for j < len(items) {
				right := items[j]
				if right.Deleted || right.ID.Clock < beforeClock {
					break
				}
				if expectedClock != right.ID.Clock {
					break
				}
				if left.Right != right {
					break
				}
				// right must continue the run: inserted directly after the
				// run's last character, toward the same right origin.
				if right.Origin == nil || right.Origin.Client != client || right.Origin.Clock != expectedClock-1 {
					break
				}
				if !originIDEquals(left.OriginRight, right.OriginRight) {
					break
				}
				// right is directly adjacent and clock-contiguous: absorb it.
				rightLen := right.Content.Len() // O(1) for single-char items
				expectedClock = right.ID.Clock + rightLen

				// Rewire linked list: splice right out.
				left.Right = right.Right
				if right.Right != nil {
					right.Right.Left = left
				}

				// Collect right's string into the builder.
				sb.WriteString(right.Content.(*ContentString).Str)

				// Schedule for store removal (appended in clock order).
				if removedByClient == nil {
					removedByClient = make(map[ClientID][]*Item, 1)
				}
				removedByClient[client] = append(removedByClient[client], right)

				j++
			}

			if j > i+1 {
				// At least one item was absorbed: commit the merged string and
				// invalidate the position cache once for the whole run.
				cs := left.Content.(*ContentString)
				cs.Str = sb.String()
				cs.utf16Len = utf16Len(cs.Str)
				// A run squash absorbs the right items into left and splices them
				// out of the linked list; a marker still pointing at an absorbed
				// (now off-list) item would walk from a dangling node. Rendered
				// positions are unchanged, but relocating each marker is Task 5's
				// mergeWith concern — here we clear all markers (always safe).
				if left.Parent != nil {
					left.Parent.clearMarkers()
				}
				// Compact items slice: skip over all absorbed entries.
				items = append(items[:i+1], items[j:]...)
			}
			i++
		}
	}

	// Single O(n) compaction pass per client using a two-pointer merge.
	// removed is already in clock order (squashRuns processes items that way),
	// matching the clock-sorted order of storeItems — no hash lookup needed.
	for client, removed := range removedByClient {
		storeItems := store.clients[client]
		n, ri := 0, 0
		for _, item := range storeItems {
			if ri < len(removed) && item == removed[ri] {
				ri++ // skip this squashed item
			} else {
				storeItems[n] = item
				n++
			}
		}
		// Zero out the tail to release GC references.
		for k := n; k < len(storeItems); k++ {
			storeItems[k] = nil
		}
		store.clients[client] = storeItems[:n]
	}
}

// changedType is one entry of a transaction's changed map: the map keys the
// transaction changed in that type, and the type's place in the order the
// transaction first changed each of its types, which is the order Yjs fires
// their observers in. An entry is never removed, so the orders of one
// transaction's entries are 0 to len(changed)-1, which buildPhase2 uses as
// indices.
type changedType struct {
	keys  map[string]struct{}
	order int
}

// addChanged records that a type was modified, optionally under a specific key.
// It is the only writer of changed, which keeps the orders dense.
func (txn *Transaction) addChanged(t *abstractType, key string) {
	c, ok := txn.changed[t]
	if !ok {
		c = changedType{keys: make(map[string]struct{}), order: len(txn.changed)}
		txn.changed[t] = c
	}
	c.keys[key] = struct{}{}
}

// addSubdocAdded records that d was newly embedded (integrated) during this
// transaction (#63).
func (t *Transaction) addSubdocAdded(d *Doc) {
	if t.subdocsAdded == nil {
		t.subdocsAdded = map[*Doc]struct{}{}
	}
	t.subdocsAdded[d] = struct{}{}
}

// addSubdocLoaded records that d should be reported as loaded in this
// transaction's SubdocsEvent (#63).
func (t *Transaction) addSubdocLoaded(d *Doc) {
	if t.subdocsLoaded == nil {
		t.subdocsLoaded = map[*Doc]struct{}{}
	}
	t.subdocsLoaded[d] = struct{}{}
}

// addSubdocRemoved records that d was detached (deleted) during this
// transaction (#63).
func (t *Transaction) addSubdocRemoved(d *Doc) {
	if t.subdocsRemoved == nil {
		t.subdocsRemoved = map[*Doc]struct{}{}
	}
	t.subdocsRemoved[d] = struct{}{}
}

// tryMergeWithLefts walks every right-half produced by splitItem during this
// transaction and re-merges it with its left neighbour when the split turned
// out to be unnecessary (#78 H2). A split is unnecessary when, by the time the
// transaction commits, no item has been inserted between the two halves and
// no other invariant (move arbitration, parent, ParentSub, deleted state,
// origin pointers) blocks reunification.
//
// Re-merging shortens the linked list, which compounds with squashRuns (new
// runs) and gcTxnDeleteSet (deleted runs after auto-GC) to keep documents from
// fragmenting over long edit sessions. Mirrors Yjs JS's `tryToMergeWithLeft`.
//
// Caller must hold doc.mu.
func tryMergeWithLefts(txn *Transaction) {
	if len(txn.mergeStructs) == 0 {
		return
	}
	// In client and clock order each half merges into the run left of it, so a
	// run split n times is copied once rather than n times.
	slices.SortFunc(txn.mergeStructs, func(a, b *Item) int {
		if c := cmp.Compare(a.ID.Client, b.ID.Client); c != 0 {
			return c
		}
		return cmp.Compare(a.ID.Clock, b.ID.Clock)
	})
	merging := mergeRuns{store: txn.doc.store}
	for _, item := range txn.mergeStructs {
		tryMergeWithLeft(item, &merging)
	}
	merging.finish()
}

// mergeRuns is the state of tryMergeWithLefts's merges, which take the halves
// in client and clock order.
type mergeRuns struct {
	store *StructStore

	// text is the run the last string merge extended, and joined its text from
	// the run's second merge on, written back when another run starts.
	text   *ContentString
	joined strings.Builder

	// items is client's store slice while merged halves leave it: items[:kept]
	// is compacted and items[next:] not yet reached.
	client     ClientID
	items      []*Item
	kept, next int
}

// appendString adds right's text to left's run without copying the whole run
// at every merge.
func (m *mergeRuns) appendString(left, right *ContentString) {
	switch {
	case left != m.text:
		m.flushText()
		m.text = left
		left.Str += right.Str
	case m.joined.Len() == 0:
		m.joined.Grow(len(left.Str) + len(right.Str))
		m.joined.WriteString(left.Str)
		m.joined.WriteString(right.Str)
	default:
		m.joined.WriteString(right.Str)
	}
	left.utf16Len += right.utf16Len
}

// flushText writes the joined text back to its run.
func (m *mergeRuns) flushText() {
	if m.joined.Len() > 0 {
		m.text.Str = m.joined.String()
		m.joined.Reset()
	}
}

// remove takes item, merged into its left neighbour, out of its client's store
// slice, moving each item that stays at most once, as a single removal did.
func (m *mergeRuns) remove(item *Item) {
	if m.items != nil && item.ID.Client == m.client {
		// Later halves come in clock order, so this scan passes only items the
		// compaction moves anyway.
		for i := m.next; i < len(m.items) && m.items[i].ID.Clock <= item.ID.Clock; i++ {
			if m.items[i] == item {
				m.kept += copy(m.items[m.kept:], m.items[m.next:i])
				m.next = i + 1
				return
			}
		}
		return
	}
	m.flushItems()
	items := m.store.clients[item.ID.Client]
	if i, ok := indexOfItem(items, item); ok {
		m.client, m.items, m.kept, m.next = item.ID.Client, items, i, i+1
	}
}

// flushItems closes the gap the merged halves left in the current client's
// store slice.
func (m *mergeRuns) flushItems() {
	if m.items == nil {
		return
	}
	n := m.kept + copy(m.items[m.kept:], m.items[m.next:])
	clear(m.items[n:])
	m.store.clients[m.client] = m.items[:n]
	m.items = nil
}

// finish writes back the last run's text and the last client's store slice.
func (m *mergeRuns) finish() {
	m.flushText()
	m.flushItems()
}

// indexOfItem finds item in items, a store slice in clock order.
func indexOfItem(items []*Item, item *Item) (int, bool) {
	i, found := slices.BinarySearchFunc(items, item.ID.Clock, func(it *Item, clock uint64) int {
		return cmp.Compare(it.ID.Clock, clock)
	})
	return i, found && items[i] == item
}

// tryMergeWithLeft attempts to absorb item into item.Left. Returns true when
// the merge succeeded. Conditions, all required:
//   - both items share the same client, parent, ParentSub, MovedBy, and Deleted state
//   - left.Right == item (still directly adjacent in the linked list)
//   - clocks are contiguous: left.ID.Clock + left.Content.Len() == item.ID.Clock
//   - item.Origin points to the last clock of left (Yjs origin invariant)
//   - both items share an OriginRight (Yjs right-origin invariant)
//   - content types match and support merging (ContentString, ContentAny,
//     ContentJSON, ContentDeleted)
//
// On success: left's content is extended in place, the linked list is spliced
// past item, and item is removed from the store.
func tryMergeWithLeft(item *Item, merging *mergeRuns) bool {
	left := item.Left
	if left == nil {
		return false
	}
	if left.ID.Client != item.ID.Client {
		return false
	}
	if left.Right != item {
		return false
	}
	if left.ID.Clock+left.Content.Len() != item.ID.Clock {
		return false
	}
	if left.Deleted != item.Deleted {
		return false
	}
	if !parentSubEqual(left.ParentSub, item.ParentSub) {
		return false
	}
	if left.Parent != item.Parent {
		return false
	}
	if left.MovedBy != item.MovedBy {
		return false
	}
	// A merge would drop the right half's redone link (Yjs mergeWith parity).
	if left.redone != nil || item.redone != nil {
		return false
	}
	// item.Origin must reference the last clock of left for the split to be
	// reversible. (splitItem always sets Origin this way; foreign updates may
	// set Origin differently, in which case we leave the items split.)
	if item.Origin == nil {
		return false
	}
	expectedLast := left.ID.Clock + left.Content.Len() - 1
	if item.Origin.Client != left.ID.Client || item.Origin.Clock != expectedLast {
		return false
	}
	// The merged item keeps left's OriginRight. splitItem copies it to the
	// right half, so a split being reversed always matches; the check keeps
	// this merge to Yjs's Item.mergeWith conditions.
	if !originIDEquals(left.OriginRight, item.OriginRight) {
		return false
	}

	// Content-type match + in-place extension.
	switch lc := left.Content.(type) {
	case *ContentString:
		rc, ok := item.Content.(*ContentString)
		if !ok {
			return false
		}
		merging.appendString(lc, rc)
	case *ContentAny:
		rc, ok := item.Content.(*ContentAny)
		if !ok {
			return false
		}
		lc.Vals = append(lc.Vals, rc.Vals...)
	case *ContentJSON:
		rc, ok := item.Content.(*ContentJSON)
		if !ok {
			return false
		}
		lc.Vals = append(lc.Vals, rc.Vals...)
	case *ContentDeleted:
		rc, ok := item.Content.(*ContentDeleted)
		if !ok {
			return false
		}
		lc.length += rc.length
	default:
		// ContentEmbed, ContentType, ContentFormat, ContentBinary, ContentDoc,
		// and ContentMove are single-value or not splittable — never appear
		// in mergeStructs in mergeable form.
		return false
	}

	// Splice item out of the linked list; a key entry moves to the merged item.
	if item.ParentSub != nil && item.Parent != nil && item.Parent.itemMap[*item.ParentSub] == item {
		item.Parent.itemMap[*item.ParentSub] = left
	}
	left.Right = item.Right
	if item.Right != nil {
		item.Right.Left = left
	}
	// item is absorbed into left and removed from the list. A marker still
	// pointing at item would be left dangling; rendered positions don't change,
	// but marker relocation on merge is Task 5's mergeWith concern, so clear all
	// markers here (always safe).
	if left.Parent != nil {
		left.Parent.clearMarkers()
	}

	// Remove item from its client's store slice.
	merging.remove(item)
	return true
}

// scanMark is a conflict scan's mark on an item it passed: which scan of the
// transaction, and at which step of it (Item.integrate).
type scanMark struct{ scan, step uint32 }

// boundedApplyState is the scan and rollback state of one remote apply on a
// document with a scan bound.
type boundedApplyState struct {
	// scanLimit is the most a phase of the apply may add to the
	// transaction's scanned count, set as the apply begins
	// (beginRemoteApply), and 0 for no limit; scanFrom is scanned as the
	// phase began: the whole apply is one phase under a budget, and under a
	// bound the drain of the pending queue is a phase with an allowance of
	// its own (drainParked).
	scanLimit, scanFrom int
	scanMarks           map[*Item]scanMark
	scanSerial          uint32
	journal             *applyJournal
	rolledBack          bool
	// report is what the apply decoded and added, kept at the end of an
	// apply that will commit, and reportDue is whether the commit has a
	// report to make of it: the totals it also carries are read at the end
	// of the transaction, after the merges that decide them
	// (noteApplyReport, reportApply).
	report    ApplyReport
	reportDue bool
}

// beginBoundedApply returns txn's bounded apply state, made on first use.
func (txn *Transaction) beginBoundedApply() *boundedApplyState {
	if txn.boundedApply == nil {
		txn.boundedApply = &boundedApplyState{}
	}
	return txn.boundedApply
}

// applyJournal returns the journal of txn's bounded apply, or nil.
func (txn *Transaction) applyJournal() *applyJournal {
	if txn.boundedApply == nil {
		return nil
	}
	return txn.boundedApply.journal
}

// scanMarkMap returns the bounded apply's scan marks, made on first use.
func (state *boundedApplyState) scanMarkMap() map[*Item]scanMark {
	if state.scanMarks == nil {
		state.scanMarks = make(map[*Item]scanMark)
	}
	return state.scanMarks
}
