package crdt

import (
	"container/heap"
	"math"
)

// A pendingWatch makes one waiter wait until a client's clock reaches a value.
// Each client's watches form a min-heap on that value, so advancing a client's
// clock wakes exactly the waiters it satisfies.
type pendingWatch struct {
	clock       uint64
	node, index int
	owner       *pendingWatchHeap
}

type pendingWatchHeap struct {
	client ClientID
	items  []*pendingWatch
}

func (h *pendingWatchHeap) Len() int           { return len(h.items) }
func (h *pendingWatchHeap) Less(i, j int) bool { return h.items[i].clock < h.items[j].clock }
func (h *pendingWatchHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}
func (h *pendingWatchHeap) Push(value any) {
	w := value.(*pendingWatch)
	w.owner = h
	w.index = len(h.items)
	h.items = append(h.items, w)
}
func (h *pendingWatchHeap) Pop() any {
	last := len(h.items) - 1
	w := h.items[last]
	h.items[last] = nil
	h.items = h.items[:last]
	w.owner = nil
	return w
}

// pendingWatches holds a fixed set of watch slots; each slot waits on at most
// one (client, clock) at a time and names the waiter (node) it wakes.
type pendingWatches struct {
	waits    []pendingWatch
	byClient map[ClientID]*pendingWatchHeap
}

func newPendingWatches(slots, clients int) pendingWatches {
	return pendingWatches{waits: make([]pendingWatch, slots), byClient: make(map[ClientID]*pendingWatchHeap, clients)}
}

func (w *pendingWatches) cancel(index int) {
	wait := &w.waits[index]
	if owner := wait.owner; owner != nil {
		heap.Remove(owner, wait.index)
		if owner.Len() == 0 {
			delete(w.byClient, owner.client)
		}
	}
}

// watch makes slot index wake node once client's clock reaches clock.
func (w *pendingWatches) watch(index int, client ClientID, clock uint64, node int) {
	h := w.byClient[client]
	if h == nil {
		h = &pendingWatchHeap{client: client}
		w.byClient[client] = h
	}
	wait := &w.waits[index]
	wait.clock, wait.node = clock, node
	heap.Push(h, wait)
}

// woken removes client's lowest watch if clock reaches it and returns the
// node that watch wakes.
func (w *pendingWatches) woken(client ClientID, clock uint64) (int, bool) {
	h := w.byClient[client]
	if h == nil || h.items[0].clock > clock {
		return 0, false
	}
	wait := heap.Pop(h).(*pendingWatch)
	if h.Len() == 0 {
		delete(w.byClient, client)
	}
	return wait.node, true
}

// Each blocked group watches one missing dependency and its own coverage.
// Replacing a head removes both old watches; stale waiters cannot accumulate
// while overlapping groups cover successive structs of a long blocked tail.
type pendingGroupWorklist struct {
	pendingWatches
	groups       []pendingGroup
	queue        []int
	front, count int
}

func (w *pendingGroupWorklist) enqueue(group int) {
	w.cancel(2 * group)
	w.cancel(2*group + 1)
	g := &w.groups[group]
	if g.done || g.queued {
		return
	}
	g.queued = true
	w.queue[(w.front+w.count)%len(w.queue)] = group
	w.count++
}

func (w *pendingGroupWorklist) advance(client ClientID, clock uint64) {
	for {
		group, ok := w.woken(client, clock)
		if !ok {
			return
		}
		w.enqueue(group)
	}
}

func resolvePendingGroups(groups []pendingGroup, known StateVector) error {
	w := pendingGroupWorklist{pendingWatches: newPendingWatches(2*len(groups), len(groups)), groups: groups, queue: make([]int, len(groups))}
	for i := range groups {
		w.enqueue(i)
	}
	for w.count > 0 {
		index := w.queue[w.front]
		w.front = (w.front + 1) % len(w.queue)
		w.count--
		g := &groups[index]
		g.queued = false
		for !g.done {
			if g.end > known.Clock(g.client) {
				if client, clock, missing := g.missing(known); missing {
					w.watch(2*index, client, clock, index)
					w.watch(2*index+1, g.client, g.end, index)
					break
				}
				known[g.client] = g.end
				w.advance(g.client, g.end)
			}
			if err := g.next(); err != nil {
				return err
			}
		}
	}
	return nil
}

// deferredWorklist schedules the retries of the items a remote update
// deferred during decoding (resolveWithinUpdatePending). The items form runs:
// consecutive deferred items of one client whose clocks follow on. A run is
// retried in clock order until an item does not integrate; the items after it
// wait behind it, and the run waits, like a blocked pendingGroup, on the one
// dependency that item stopped at and on its own client's clock covering it
// (two watch slots, 2r and 2r+1). It is queued again only when one is met.
//
// Retries are ordered as the fixed-point passes this replaced made them: a
// pass tries every unintegrated item in arrival order, and the next pass
// starts when one ends. A run woken by the integration of an item that
// arrived before its next item is retried later in the same pass; one woken
// by a later arrival waits for the next pass. Each item is retried at the
// first such slot after the dependencies it was seen blocked on are met, which
// is where the passes first found it integrable, so the items integrate in the
// same order.
type deferredWorklist struct {
	pendingWatches
	runs  []deferredRun
	done  []bool         // by arrival index: integrated (or dropped as covered)
	queue []deferredSlot // min-heap on (pass, index)
	at    deferredSlot   // the item being retried
}

// deferredRun holds the arrival indices [next, end) of a run not yet integrated.
type deferredRun struct{ next, end int }

// deferredSlot is a retry of run's item index in a pass.
type deferredSlot struct{ pass, index, run int }

func (a deferredSlot) before(b deferredSlot) bool {
	return a.pass < b.pass || a.pass == b.pass && a.index < b.index
}

func newDeferredWorklist(pending []*Item) *deferredWorklist {
	w := &deferredWorklist{done: make([]bool, len(pending))}
	for i := 0; i < len(pending); {
		client, end := pending[i].ID.Client, pending[i].ID.Clock+uint64(pending[i].Content.Len())
		j := i + 1
		for ; j < len(pending) && pending[j].ID.Client == client && pending[j].ID.Clock == end; j++ {
			end += uint64(pending[j].Content.Len())
		}
		// The first pass in arrival order is already a heap.
		w.queue = append(w.queue, deferredSlot{index: i, run: len(w.runs)})
		w.runs = append(w.runs, deferredRun{next: i, end: j})
		i = j
	}
	w.pendingWatches = newPendingWatches(2*len(w.runs), 0)
	return w
}

// next returns the next run to retry, or false once every deferred item is
// integrated or waits on a dependency nothing met.
func (w *deferredWorklist) next() (int, bool) {
	q := w.queue
	if len(q) == 0 {
		return 0, false
	}
	w.at = q[0]
	last := len(q) - 1
	q[0] = q[last]
	q = q[:last]
	for i := 0; ; {
		child := 2*i + 1
		if child >= len(q) {
			break
		}
		if child+1 < len(q) && q[child+1].before(q[child]) {
			child++
		}
		if !q[child].before(q[i]) {
			break
		}
		q[i], q[child] = q[child], q[i]
		i = child
	}
	w.queue = q
	return w.at.run, true
}

// integrate retries run r's items in clock order until one does not
// integrate, and blocks the run on that item.
func (w *deferredWorklist) integrate(txn *Transaction, pending []*Item, r int) {
	store := txn.doc.store
	for run := &w.runs[r]; run.next < run.end; run.next++ {
		w.at.index = run.next
		item := pending[run.next]
		if !tryIntegrate(txn, item) {
			w.block(r, item, store)
			return
		}
		w.done[run.next] = true
		w.advance(item.ID.Client, store.NextClock(item.ID.Client))
	}
}

// block makes run r, whose item could not integrate, wait for what
// tryIntegrate needs first: the same-client predecessor of a clock gap, or
// the origin, right origin or parent container itemMissingDep names. The run
// also waits for its client's clock to cover the item, as another struct of
// the update can carry its range and tryIntegrate drops a covered item.
func (w *deferredWorklist) block(r int, item *Item, store *StructStore) {
	if item.ID.Clock > store.NextClock(item.ID.Client) {
		w.watch(2*r, item.ID.Client, item.ID.Clock, r)
		return
	}
	// No store clock passes math.MaxUint64: such a dependency never resolves.
	if dep, missing := itemMissingDep(item, store); missing && dep.Clock < math.MaxUint64 {
		w.watch(2*r, dep.Client, dep.Clock+1, r)
	}
	if end := item.ID.Clock + uint64(item.Content.Len()); end > item.ID.Clock {
		w.watch(2*r+1, item.ID.Client, end, r)
	}
}

// advance queues every run waiting on client that clock (the client's store
// clock after the item being retried integrated) satisfies.
func (w *deferredWorklist) advance(client ClientID, clock uint64) {
	for {
		r, ok := w.woken(client, clock)
		if !ok {
			return
		}
		w.cancel(2 * r)
		w.cancel(2*r + 1)
		slot := deferredSlot{pass: w.at.pass, index: w.runs[r].next, run: r}
		if slot.index < w.at.index {
			slot.pass++
		}
		w.queue = append(w.queue, slot)
		q := w.queue
		for i := len(q) - 1; i > 0; {
			parent := (i - 1) / 2
			if !q[i].before(q[parent]) {
				break
			}
			q[i], q[parent] = q[parent], q[i]
			i = parent
		}
	}
}
