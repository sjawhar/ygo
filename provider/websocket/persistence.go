package websocket

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/reearth/ygo/crdt"
)

// persistStranded writes to the adapter the updates a room's persistence
// worker can no longer take (#229). It is called once the worker has begun
// retiring: from the doc.OnUpdate observer (persistUpdate), for an update it
// found the worker retired for and did not send (passed as extra), and, after
// a send that raced the retirement, for updates sent that may have missed the
// worker's final drain; and from the room's feeder (feedPersistence), for the
// queued updates it still holds (passed as extra) and for what its own sends
// may have left in persistCh after that drain.
//
// It first waits for the worker to be completely gone (persistDone). That wait
// is what lets it call the adapter without a lock the worker also holds: the
// adapter contract promises calls for one room are serialised, and after
// persistDone the worker is issuing none. The wait lasts as long as the
// worker's exit, which calls the adapter: its last stores, each of which can
// be followed by a SaveVersion when the version interval has passed, then a
// forced version check and Compact, when the adapter has those calls. Called
// from the observer, the wait holds the committing goroutine, so it can
// deadlock: an adapter call in that exit that waits on something the
// committer holds, such as a lock of the consumer's, never returns, and the
// worker and the committer wait on each other. persistUpdate keeps a commit
// from waiting on a running worker;
// this wait on a retiring one is the limit it keeps from #229: a commit made
// while the worker retires, with no feeder running, waits here. After that
// wait every caller takes persistFallbackMu and holds it across its adapter
// calls, so a committer here also waits for another caller's stores, the
// feeder's included, and an adapter call of that caller's that waits on what
// the committer holds deadlocks them the same way. The feeder holds no lock
// while it waits on persistDone and is not a committing goroutine, so the
// worker's exit needs nothing it holds and its own wait forms no cycle.
// Teardown waits on persistDone, not on this function, but it can still wait
// for a call of this function on another goroutine or its own: a forced
// CloseRoom waits for the read loops of the peers it closes, and one of those
// can be inside this function, committing as the worker retires; and the
// teardown that removed the room from s.rooms waits for persistDone and then
// calls OnUnloadDocument synchronously, as an evicting handleDisconnect does
// OnLastPeer (a CloseRoom that lost that race waits for persistDone and
// returns without the hook), so a hook that commits into a document it
// retained, with no feeder running, runs this function on teardown's own
// goroutine before teardown returns.
// Shutdown waits on persistDone and then on the goroutines in this function
// too (Server.stranded), each bounded by its ctx.
//
// Anything still stranded in persistCh is stored FIRST, then extra, in order,
// so a single stranded call writes in the order the updates were handed over.
// That is best-effort ordering WITHIN one call only: two committing goroutines
// can be in the observer at once (see room.persistFallbackMu), so across
// concurrent stranded calls the adapter may see the room's updates out of
// commit order. Harmless for a CRDT log — updates merge order-independently —
// but do not read it as a stronger promise than it is. Errors are logged, not
// returned: the observer and the feeder have no caller to report to.
// Auto-versioning and compaction are deliberately NOT re-run here — the worker
// already performed its final maybeVersion/maybeCompact, and everything stored
// here missed its final drain: late commits, and updates its feeder handed on
// too late for that drain.
//
// A commit whose observer calls persistStranded has been handed to the adapter
// when the commit returns - by the worker, in its loop before it retired or in
// its final drain, by that call, or by another persistStranded call that took
// it from persistCh under persistFallbackMu, which this call waits for: the
// observer pays for this synchronously, on the committing goroutine. That is
// the intended trade: it
// happens only during and after room retirement, and the alternative — the
// pre-#229 behaviour — was silent data loss. Two consequences worth stating
// rather than discovering:
//
//   - crdt.buildPhase2 fires OnUpdate observers in registration order and
//     loadRoom registers persistence BEFORE the relay observers, so a stranded
//     write the observer makes delays that update's cluster fan-out for as
//     long as the waits above and the adapter take. Bounded to room
//     retirement, and a publish onto an already-retired lane is counted in
//     RelayStats().Dropped rather than lost silently — but it does stretch
//     the "no blocking I/O on the commit path" convention further than the
//     rest of the package does.
//   - It writes for a room name the server may already consider gone, and can
//     do so indefinitely if a caller retains the *crdt.Doc. See
//     CompactableAdapter's godoc for what that means for adapters. Ordinary
//     commits reach this too, through the room's feeder, which teardown does
//     not join: handleDisconnect, evictIdleRoom and closeRoom wait for the
//     worker (persistDone), and their flush barrier (awaitOverflow) waits at
//     most for the updates queued before it began - it also returns once the
//     worker has exited, with some of those possibly still queued. A feeder
//     can then still hold updates - ones queued after the barrier began, or,
//     when the barrier returned on the worker's exit, ones queued before it -
//     whether it was running when the barrier returned or started after; it
//     writes what it holds when it finds the worker retired through here,
//     once the worker has gone, possibly after teardown returned and after a
//     reload of the same name bootstrapped from a store that did not hold
//     them yet. A store that fails here is logged and dropped, and a process
//     that exits first (Shutdown waits for the feeder only until its ctx
//     expires) loses what the feeder still holds; a CRDT log converges
//     whatever the order its updates land in. A room reloaded meanwhile does
//     not get them from the store until its name is loaded again; a client
//     that has them can send them to it sooner.
func (s *Server) persistStranded(r *room, name string, extra [][]byte) {
	<-r.persistDone

	r.persistFallbackMu.Lock()
	defer r.persistFallbackMu.Unlock()

	store := func(update []byte) {
		defer func() {
			if rv := recover(); rv != nil {
				log.Printf("ygo/websocket: StoreUpdate panic for retired room %q: %v", name, rv)
			}
		}()
		// context.Background, never the worker's ctx: that ctx is cancelled by
		// the very signal (shutdown / stop) that retired the worker, so using it
		// would make every ctx-aware adapter discard exactly the writes this
		// function exists to save.
		var err error
		if pac, ok := s.persistence.(PersistenceAdapterContext); ok {
			err = pac.StoreUpdateContext(context.Background(), name, update)
		} else {
			err = s.persistence.StoreUpdate(name, update)
		}
		if err != nil {
			log.Printf("ygo/websocket: StoreUpdate for retired room %q: %v", name, err)
		}
	}

	for {
		select {
		case update := <-r.persistCh:
			store(update)
		default:
			for i, update := range extra {
				store(update)
				extra[i] = nil
			}
			return
		}
	}
}

// persistUpdate hands update, which the room's document just committed, to the
// room's persistence worker without waiting for it while the worker runs. It
// runs in the room's doc.OnUpdate observer, on the committing goroutine, and
// that goroutine may hold what the adapter itself waits on - a lock of the
// consumer's, or a write the adapter must see finished before it stores. A
// send that waited for the 256-slot buffer to free would then wait on a worker
// that waits on the committer, and neither would move; the buffer exists so
// that a slow storage backend does not block the Transact caller (N-H7). So,
// while the worker runs, the update goes into persistCh only when no feeder
// runs and that is immediate; otherwise it queues on persistOverflow, behind
// every update queued before it, which a feeder goroutine hands on
// (feedPersistence). While a feeder runs, every update queues behind the
// updates it holds, whatever the worker's state, so none overtakes them; once
// the feeder finds the worker retired, it writes what it still holds itself,
// through persistStranded.
//
// With no feeder running, the observer handles the worker's retirement (#229)
// itself. A send that completed while persistRetire was still open is taken
// by the worker: by its loop before it retires, or by its final drain, which
// strictly follows close(persistRetire). A send that raced the latch may have
// missed that drain, and an update the observer finds the worker retired for
// is not sent at all, so in both cases the
// observer calls persistStranded: with the unsent update, which only that
// call writes, or with nothing, to write what is left in persistCh, which
// another persistStranded call may have taken first. That one wait on the
// worker is on a worker that has closed its latch and is finishing its exit
// - its final drain, which still takes what reaches persistCh until it finds
// the channel empty, and its last adapter calls - not on a running worker;
// that exit calls the adapter, so an adapter that waits on what the committer
// holds deadlocks it there (see persistStranded).
func (s *Server) persistUpdate(r *room, name string, update []byte) {
	r.persistMu.Lock()
	if r.persistFeeding {
		r.queuePersistLocked(update)
		r.persistMu.Unlock()
		return
	}
	select {
	case <-r.persistRetire:
		r.persistMu.Unlock()
		s.persistStranded(r, name, [][]byte{update})
		return
	default:
	}
	select {
	case r.persistCh <- update:
		r.persistMu.Unlock()
		select {
		case <-r.persistRetire:
			s.persistStranded(r, name, nil)
		default:
		}
		return
	default:
	}
	r.queuePersistLocked(update)
	s.startFeederLocked(r, name)
	r.persistMu.Unlock()
}

// queuePersistLocked appends update to the room's queue (persistOverflow) and
// counts it in persistQueued. The caller holds persistMu.
func (r *room) queuePersistLocked(update []byte) {
	r.persistOverflow = append(r.persistOverflow, update)
	r.persistQueued++
}

// startFeederLocked starts the room's feeder (feedPersistence). It is counted
// with the committing goroutines in the persistence observer
// (Server.stranded), so Shutdown waits, until its ctx expires, for the feeder
// to finish. The caller holds persistMu and has checked that no feeder runs.
func (s *Server) startFeederLocked(r *room, name string) {
	r.persistFeeding = true
	s.stranded.enter()
	go s.feedPersistence(r, name)
}

// feedPersistence hands the room's queued updates (persistOverflow) to its
// persistence worker, oldest first, waiting for the worker, which the observer
// never does while the worker runs: the feeder holds no lock while it waits
// for the worker. An update stays at the head of the queue until its send
// completes, so the observer keeps queueing behind it and the updates reach
// persistCh in the order the observer handed them over. Its select offers
// the send and persistRetire alike, and the worker's final drain frees slots
// after close(persistRetire), so a send can still complete once the worker
// has begun retiring; the drain stores it if the drain still sees it. Once the
// feeder finds persistRetire closed - in that select, or with its queue empty
// - it calls persistStranded, which writes what persistCh still holds (what
// its late sends left there, unless another persistStranded call took it
// first) and then what the feeder still holds. Each update it hands on, sent
// or passed to persistStranded, counts in persistHanded and releases the
// waits that count reaches (handedOnLocked). It ends when the queue is empty,
// by which time it has released every wait.
func (s *Server) feedPersistence(r *room, name string) {
	defer s.stranded.leave()
	r.persistMu.Lock()
	for {
		if len(r.persistOverflow) == 0 {
			select {
			case <-r.persistRetire:
				r.persistMu.Unlock()
				s.persistStranded(r, name, nil)
				r.persistMu.Lock()
				if len(r.persistOverflow) > 0 {
					continue
				}
			default:
			}
			r.persistFeeding = false
			r.persistOverflow = nil
			r.persistMu.Unlock()
			return
		}
		update := r.persistOverflow[0]
		r.persistMu.Unlock()
		select {
		case r.persistCh <- update:
			r.persistMu.Lock()
			r.persistOverflow[0] = nil
			r.persistOverflow = r.persistOverflow[1:]
			r.handedOnLocked(1)
		case <-r.persistRetire:
			r.persistMu.Lock()
			left := r.persistOverflow
			r.persistOverflow = nil
			r.persistMu.Unlock()
			s.persistStranded(r, name, left)
			r.persistMu.Lock()
			r.handedOnLocked(len(left))
		}
	}
}

// handedOnLocked counts n more queued updates as handed on (persistHanded) and
// releases, lowest mark first, every wait whose mark that count has reached.
// The caller holds persistMu.
func (r *room) handedOnLocked(n int) {
	r.persistHanded += uint64(n)
	for len(r.persistWaiters) > 0 && r.persistWaiters[0].mark <= r.persistHanded {
		close(r.persistWaiters[0].released)
		r.persistWaiters[0] = persistWaiter{}
		r.persistWaiters = r.persistWaiters[1:]
	}
}

// persistWaiter is one wait on a room's feeder (handedOnWait): the feeder
// closes released once persistHanded reaches mark.
type persistWaiter struct {
	mark     uint64
	released chan struct{}
}

// handedOnWait registers a wait for every update queued on persistOverflow so
// far to be handed on, and returns the channel the feeder closes once they
// are; it returns nil when there is nothing to wait for, because every queued
// update has been handed on or stop is closed. It reads persistQueued and
// appends the wait under one persistMu hold, and persistQueued only grows, so
// persistWaiters stays in mark order for handedOnLocked to release from the
// front. It allocates only when it registers a wait.
func (r *room) handedOnWait(stop <-chan struct{}) <-chan struct{} {
	r.persistMu.Lock()
	defer r.persistMu.Unlock()
	if r.persistHanded >= r.persistQueued {
		return nil
	}
	select {
	case <-stop:
		return nil
	default:
	}
	released := make(chan struct{})
	r.persistWaiters = append(r.persistWaiters, persistWaiter{mark: r.persistQueued, released: released})
	return released
}

// awaitHandedOn returns once the room's feeder has handed on every update
// queued on persistOverflow when it was called - sent it to persistCh or, once
// it found the worker retired, passed it to persistStranded and that call
// returned - or once stop is closed. An update queued after the call queues
// behind the wait rather than extending it.
func (r *room) awaitHandedOn(stop <-chan struct{}) {
	if c := r.handedOnWait(stop); c != nil {
		select {
		case <-c:
		case <-stop:
		}
	}
}

// awaitPersistence holds a peer's read loop, before it applies an update the
// peer sent, until the updates queued beyond the room's 256-slot persistence
// buffer when it began have been handed on, so storage lag slows a fast peer.
// The wait covers everything the read loop committed before it began, so
// until the worker retires, what one read loop has in that queue is at most
// what it committed since its last wait: the commits of the one update it
// applied after that wait, which can be more than one - a remote update that
// changes formatted text can be followed, in the same apply, by the local
// transaction of the document's format cleanup (crdt.WithFormatCleanup,
// Doc.SetFormatCleanup), and consumer code the apply runs
// (Server.OnPeerUpdate, a document observer) can commit too - and whatever
// consumer code it ran for the peer's other messages since then committed (an
// OnStateless hook, say). The read loop holds no lock here, which is what
// makes this the place to wait. It returns once those updates are handed on
// or the worker retires (persistRetire), and on nothing else: a connection
// closed while the read loop waits here, by a cancelled request context or
// otherwise, does not end the wait, and once it returns the read loop
// attempts the update it holds (applyPeerUpdate, which a hook or the decoder
// can refuse).
func (r *room) awaitPersistence() {
	if r.persistCh == nil {
		return
	}
	r.awaitHandedOn(r.persistRetire)
}

// awaitOverflow returns once the updates queued beyond the room's buffer when
// it was called have been handed to the worker's channel, or once the worker
// has exited (persistDone); some of them can then still be queued, for the
// room's feeder to write through persistStranded, and a sender finds
// persistDone closed and makes no flush. When an on-demand flush (flushReq)
// sent after it reaches the worker instead, every update whose persistUpdate
// ran before the call is in persistCh or already taken by the worker; the
// flush takes what persistCh holds then, which can include updates handed
// over after the call. A transaction committed before the call whose
// observer had not run yet (observers fire after the document's lock is
// released) is not among them: it is handled like an update handed over
// after the call. A commit made while it waits queues behind it rather than
// extending it.
func (r *room) awaitOverflow() {
	r.awaitHandedOn(r.persistDone)
}

// startPersistenceWorker spawns the goroutine that drains r.persistCh and
// forwards each update to the PersistenceAdapter. It must be called with
// r.persistCh, r.persistStop, r.persistRetire, and r.persistDone already
// initialised.
//
// The worker exits when either r.persistStop or s.shutdownCh is closed,
// draining persistCh until it is empty before returning, so an update
// buffered there when it retires is not left unread (a failed store is
// logged).
//
// That guarantee needs a handshake with the producer, not just a final drain
// (#229). A drain is one-shot: producers — peer read loops above all — keep
// committing throughout Shutdown, and Shutdown closes shutdownCh long before it
// closes the peer connections. So the FIRST act of every exit path is to close
// r.persistRetire, publishing "I am leaving" before the final drain rather than
// after it. loadRoom's doc.OnUpdate observer (persistUpdate) reads that latch
// when no feeder runs: when its send completed while the latch was open, it
// relies on the worker, whose loop takes the update before it retires or
// whose final drain, which necessarily follows that send, does; it calls
// persistStranded when it finds the latch closed, before it would send
// (to write the update itself) or after its send (to write what is left in the
// channel, unless another persistStranded call took it first); and when it
// finds the latch open and the buffer full it neither sends nor stores, but
// queues the update and starts the room's feeder. While a feeder runs, the
// observer queues the update without reading the latch, and the feeder hands
// the queue on under the same latch: a send of its that completed while the
// latch was open is taken by the worker, by its loop or its final drain, and a
// later one by that drain if the drain still sees it; once it finds the latch
// closed it calls persistStranded, which writes what is left in the channel
// and then what the feeder still holds. Before the latch, the observer chose
// between a send and persistStop, and a commit after the drain was lost: sent
// into the unread 256-slot buffer, dropped by that select once persistStop was
// closed, or, with persistStop open and the buffer full, held in its
// committer's send until persistStop closed and then dropped.
//
// When coalescing is enabled (see resolveCoalesceConfig / Server.PersistCoalesceWindow),
// bursts of updates are debounced into one merged write - or, when the merge
// fails, one write per update: each new update restarts the window timer, and
// a per-batch maxWait timer, started with the batch's first update, becomes
// ready maxWait later. A ready timer makes a flush one of the cases the loop's
// select can choose, and the select can still choose an update waiting in
// persistCh first, so a steady stream of updates can hold the flush past
// maxWait. A timed flush that fails keeps the batch but clears both timers, so
// the batch waits for the next update (which arms them again), an on-demand
// flush or the exit path. When disabled (window < 0), each update is stored
// individually.
//
// If s.persistence implements PersistenceAdapterContext, store calls are made
// via StoreUpdateContext with a ctx that is cancelled when shutdown or stop
// fires — that cancellation exists so a blocking adapter cannot wedge Shutdown
// indefinitely. Every FINAL flush, on both paths, and the coalescing path's
// on-demand flush are the exceptions: they use context.Background(), so the
// last write is not aborted by the very signal that triggers it (#229 — the
// strict path used to pass the cancellable ctx here, which made ctx-aware
// adapters discard the whole tail). Otherwise falls back to StoreUpdate
// (existing behaviour).
//
// When s.persistence also implements CompactableAdapter, Compact is invoked
// on room unload (including server shutdown) and, when s.CompactEvery > 0,
// after every N persistence flushes on the coalescing path.
//
// When s.persistence also implements VersionableAdapter and s.AutoVersionEvery
// > 0, SaveVersion is invoked at most once per AutoVersionEvery per room, and
// only when the room changed since its last version, plus once on room unload if
// it changed after that. The hook lives in store() rather than in either select
// loop, so both the coalescing and the strict per-update paths get it.
func (s *Server) startPersistenceWorker(r *room, name string) {
	clock := s.clock
	if clock == nil {
		clock = realClock{}
	}
	mergeFn := s.mergeFn
	if mergeFn == nil {
		mergeFn = crdt.MergeUpdatesV1
	}
	enabled, window, maxWait := resolveCoalesceConfig(s.PersistCoalesceWindow, s.PersistCoalesceMaxWait)

	// Auto-versioning setup, resolved BEFORE the goroutine starts. versioner is
	// left nil when the feature is off or unsupported, which makes maybeVersion a
	// cheap no-op on the hot path.
	versioner, _ := s.persistence.(VersionableAdapter)
	autoVersionEvery := s.AutoVersionEvery
	if autoVersionEvery <= 0 {
		versioner = nil
	}
	// lastVersion is stamped here, not inside the goroutine, so the interval is
	// measured from when the worker was CREATED rather than from whenever its
	// goroutine happened to be scheduled.
	lastVersion := clock.now()

	go func() {
		defer close(r.persistDone)

		// retire publishes "this worker is leaving" to the doc.OnUpdate producer
		// (#229). Ordering is load-bearing in two directions:
		//
		//   - it must run BEFORE the final drain, so a send that completed while
		//     the latch was open, and that the loop has not taken yet, is picked
		//     up by that drain;
		//   - it must run BEFORE close(r.persistDone), so a producer that sees
		//     the latch and blocks on persistDone is released only once the
		//     worker really is finished with the adapter.
		//
		// The defer is the backstop for an exit no explicit call covers (a panic
		// escaping the loop): deferred calls run LIFO, so this fires before the
		// close(r.persistDone) registered above it. sync.Once keeps the explicit
		// exit-path calls and this backstop from double-closing.
		var retireOnce sync.Once
		retire := func() { retireOnce.Do(func() { close(r.persistRetire) }) }
		defer retire()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Cancel the adapter ctx when shutdown or stop fires.
		go func() {
			select {
			case <-s.shutdownCh:
			case <-r.persistStop:
			}
			cancel()
		}()

		compactor, _ := s.persistence.(CompactableAdapter)
		compactEvery := s.CompactEvery
		flushCount := 0

		// dirtySinceVersion, like lastVersion above, is mutated only from
		// store()/maybeVersion on THIS goroutine, so it needs no synchronisation.
		dirtySinceVersion := false

		// maybeVersion asks the adapter for a labelled version, but only when the
		// room actually changed since the last one (dirtySinceVersion) and either
		// the interval has elapsed or force is set (room unload). That pairing is
		// the anti-churn guarantee: a quiet room is never versioned, and an active
		// room yields at most one version per AutoVersionEvery.
		//
		// Contained by recover; errors and panics are logged and never fatal.
		// On failure lastVersion is still advanced so a permanently failing adapter
		// is retried once per interval rather than on every flush, while
		// dirtySinceVersion stays set so the change is not forgotten.
		maybeVersion := func(force bool) {
			if versioner == nil || !dirtySinceVersion {
				return
			}
			if !force && clock.now().Sub(lastVersion) < autoVersionEvery {
				return
			}
			defer func() {
				if rv := recover(); rv != nil {
					lastVersion = clock.now()
					log.Printf("ygo/websocket: SaveVersion panic for room %q: %v", name, rv)
				}
			}()
			_, err := versioner.SaveVersion(context.Background(), name, AutoVersionLabel)
			lastVersion = clock.now()
			if err != nil {
				log.Printf("ygo/websocket: SaveVersion for room %q: %v", name, err)
				return
			}
			dirtySinceVersion = false
		}

		// maybeCompact calls the adapter's Compact (if any) with a background
		// ctx, contained by recover; errors/panics are logged, never fatal.
		maybeCompact := func() {
			if compactor == nil {
				return
			}
			defer func() {
				if rv := recover(); rv != nil {
					log.Printf("ygo/websocket: Compact panic for room %q: %v", name, rv)
				}
			}()
			if err := compactor.Compact(context.Background(), name); err != nil {
				log.Printf("ygo/websocket: Compact for room %q: %v", name, err)
			}
		}

		// onFlushed records a non-empty flush and triggers count-based compaction.
		onFlushed := func(wrote bool) {
			if !wrote || compactEvery <= 0 || compactor == nil {
				return
			}
			flushCount++
			if flushCount >= compactEvery {
				flushCount = 0
				maybeCompact()
			}
		}

		// store issues one backing-store write for a single update using the
		// given ctx and returns whether it failed. Panics are contained and
		// reported as a non-nil error; errors are logged (no retry).
		store := func(ctx context.Context, update []byte) (err error) {
			defer func() {
				if rv := recover(); rv != nil {
					log.Printf("ygo/websocket: StoreUpdate panic for room %q: %v", name, rv)
					err = fmt.Errorf("StoreUpdate panic for room %q: %v", name, rv)
				}
			}()
			if pac, ok := s.persistence.(PersistenceAdapterContext); ok {
				err = pac.StoreUpdateContext(ctx, name, update)
			} else {
				err = s.persistence.StoreUpdate(name, update)
			}
			if err != nil {
				log.Printf("ygo/websocket: StoreUpdate for room %q: %v", name, err)
				return err
			}
			// A durable write is the one signal that the room changed, and it is
			// common to BOTH the coalescing and the strict per-update paths, so
			// auto-versioning hooks in here rather than in either select loop.
			dirtySinceVersion = true
			maybeVersion(false)
			return nil
		}

		// flush merges batch into one update and stores it. On merge failure it
		// stores each update individually so a bad merge never drops data. It
		// returns true only if the batch is fully persisted and therefore safe to
		// discard; a false result (e.g. ctx cancelled at shutdown, or a transient
		// store error) means the caller must RETAIN the batch so it can be
		// re-flushed — otherwise a batch could be lost if a timer flush races with
		// shutdown cancelling the worker ctx.
		flush := func(ctx context.Context, batch [][]byte) bool {
			if len(batch) == 0 {
				return true
			}
			if len(batch) == 1 {
				return store(ctx, batch[0]) == nil
			}
			merged, err := mergeFn(batch...)
			if err != nil {
				log.Printf("ygo/websocket: merge of %d updates for room %q failed (%v); storing individually", len(batch), name, err)
				ok := true
				for _, u := range batch {
					if store(ctx, u) != nil {
						ok = false
					}
				}
				return ok
			}
			return store(ctx, merged) == nil
		}

		// Strict per-update path (coalescing disabled): one store per update.
		if !enabled {
			// pending holds updates whose store returned an error and whose
			// worker ctx was cancelled when the worker checked it after the store
			// returned, whatever the adapter's reason. Cancellation is a
			// shutdown/stop signal, not a verdict on the data, so such an update
			// is retained and its store attempted again on the exit path under a
			// background ctx — the same retain-and-re-flush discipline the
			// coalescing path already applies to an unflushed batch; that attempt
			// can fail too, and is then logged. A failed store whose ctx is not
			// cancelled at that check is logged and dropped. Without the
			// retention a ctx-aware adapter loses every update the worker happens
			// to pick up in the window between shutdown cancelling the ctx and
			// the worker reaching its exit case (#229). Mutated only from this
			// goroutine.
			var pending [][]byte

			// storeRetaining is store() plus that retention rule.
			storeRetaining := func(c context.Context, update []byte) bool {
				if err := store(c, update); err != nil {
					if c.Err() != nil {
						pending = append(pending, update)
					}
					return false
				}
				return true
			}

			// drain attempts a store of each update buffered, until the channel
			// is empty, and discards the results: the final drain of the exit
			// path, run on context.Background, so a store that fails there is
			// logged by store and not retried, and persistDone closes without it.
			drain := func(c context.Context) {
				for {
					select {
					case update := <-r.persistCh:
						_ = storeRetaining(c, update)
					default:
						return
					}
				}
			}
			// storeBuffered stores the updates buffered when it is called and
			// reports whether each of those stores succeeded, which is what an
			// on-demand flush acks; an update the worker took, and stored or
			// failed to store, before the flush is not part of that result.
			// While the worker runs nothing else receives from persistCh
			// (persistStranded waits for persistDone), so reading
			// len(r.persistCh) of them never blocks.
			storeBuffered := func(c context.Context) bool {
				ok := true
				for n := len(r.persistCh); n > 0; n-- {
					if !storeRetaining(c, <-r.persistCh) {
						ok = false
					}
				}
				return ok
			}
			// exit runs the shared retire-then-drain sequence for both exit
			// triggers. retire() FIRST (see the retire comment above), then the
			// retained updates and the final drain under a BACKGROUND ctx so a
			// ctx-aware adapter is not handed the context that shutdown itself
			// just cancelled (#229 — this path used to pass the cancellable one).
			exit := func() {
				retire()
				retained := pending
				pending = nil
				for _, u := range retained {
					_ = store(context.Background(), u)
				}
				drain(context.Background())
				maybeVersion(true)
				maybeCompact()
			}
			for {
				select {
				case update := <-r.persistCh:
					_ = storeRetaining(ctx, update)
				case ack := <-r.flushReq:
					// Store what is buffered now, which is all the flush must
					// cover: its sender waited (awaitOverflow) until every
					// update whose persistUpdate ran before its barrier was in
					// the channel or already taken by this worker. What is
					// buffered can also include updates handed over after the
					// barrier. Not until the channel is empty, since new
					// commits refill each slot the flush frees, through the
					// room's feeder or the observer's own send, and a committer
					// that outpaces the adapter would keep the flush going as
					// long as it commits.
					ack <- storeBuffered(ctx)
				case <-r.persistStop:
					exit()
					return
				case <-s.shutdownCh:
					exit()
					return
				}
			}
		}

		// Coalescing path.
		var batch [][]byte
		var windowT, maxT wsTimer

		clearTimers := func() {
			if windowT != nil {
				windowT.stop()
				windowT = nil
			}
			if maxT != nil {
				maxT.stop()
				maxT = nil
			}
		}
		// drainBuffered pulls updates from persistCh into batch without
		// blocking, until the channel is empty. Used on stop/shutdown before the
		// final flush.
		drainBuffered := func() {
			for {
				select {
				case update := <-r.persistCh:
					batch = append(batch, update)
				default:
					return
				}
			}
		}
		// exit runs the shared retire-then-drain-then-flush sequence for both
		// exit triggers. retire() comes FIRST because a producer can still hand
		// an update to the channel after this worker's last sweep - a committer
		// that read the latch open before it closed, a feeder whose select takes
		// a slot the drain freed - and the closed latch is what makes it write
		// what is left itself, through persistStranded (the observer's
		// post-send check, the feeder's retirement branch) (#229). The flush
		// uses a background ctx so the last batched write survives the very
		// signal that triggered the exit; a failed final flush is logged and
		// not retried.
		exit := func() {
			retire()
			drainBuffered()
			flush(context.Background(), batch)
			clearTimers() // release any pending timers before exit
			maybeVersion(true)
			maybeCompact()
		}

		for {
			var windowC, maxC <-chan time.Time
			if windowT != nil {
				windowC = windowT.ch()
			}
			if maxT != nil {
				maxC = maxT.ch()
			}
			select {
			case update := <-r.persistCh:
				batch = append(batch, update)
				if windowT == nil {
					// First update of a new batch: arm both timers.
					windowT = clock.newTimer(window)
					maxT = clock.newTimer(maxWait)
				} else {
					// Debounce: restart the window timer; leave maxWait (it is
					// measured from the batch's first update). Create-new avoids
					// any Reset/drain hazard.
					windowT.stop()
					windowT = clock.newTimer(window)
				}
			case <-windowC:
				// Retain the batch if the flush did not fully persist: a timer
				// flush can race with shutdown cancelling ctx, and niling an
				// unpersisted batch would lose it. Timers are cleared regardless;
				// a retained batch re-flushes on the next update or at exit.
				// A batch retained here has no self-driven retry timer in steady
				// state; it is re-flushed at the stop/shutdown exit case below
				// with a background context.
				wrote := len(batch) > 0
				ok := flush(ctx, batch)
				if ok {
					batch = nil
				}
				clearTimers()
				onFlushed(wrote && ok)
			case <-maxC:
				wrote := len(batch) > 0
				ok := flush(ctx, batch)
				if ok {
					batch = nil
				}
				clearTimers()
				onFlushed(wrote && ok)
			case ack := <-r.flushReq:
				// The just-arrived edit may still be in persistCh, not yet in
				// batch — take what is buffered first so an on-demand flush
				// never misses it. Only what is buffered now, as on the strict
				// path: new commits refill each slot this frees, and the flush
				// need not cover an update committed after its sender's barrier
				// (awaitOverflow). Nothing else receives from persistCh while
				// the worker runs, so this never blocks.
				for n := len(r.persistCh); n > 0; n-- {
					batch = append(batch, <-r.persistCh)
				}
				wrote := len(batch) > 0
				ok := flush(context.Background(), batch)
				if ok {
					batch = nil
				}
				clearTimers()
				onFlushed(wrote && ok)
				// Report the result so teardown can gate eviction on it (an empty batch reports true).
				// On failure the batch is retained (not niled) for a later retry.
				// ack is buffered (cap 1) by every caller, so this never blocks.
				ack <- ok
			case <-r.persistStop:
				exit()
				return
			case <-s.shutdownCh:
				exit()
				return
			}
		}
	}()
}
