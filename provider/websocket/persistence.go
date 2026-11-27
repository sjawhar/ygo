package websocket

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/reearth/ygo/crdt"
)

// persistStranded is the durable destination for updates a room's persistence
// worker can no longer take (#229). It is called once the worker has begun
// retiring: from the doc.OnUpdate observer (persistUpdate), for an update that
// could not be enqueued at all (passed as extra) and for updates that were
// enqueued but may have missed the worker's final drain; and from the room's
// feeder (feedPersistence), for the queued updates it still holds.
//
// It first waits for the worker to be completely gone (persistDone). That wait
// is what lets it call the adapter without a lock the worker also holds: the
// adapter contract promises calls for one room are serialised, and after
// persistDone the worker is issuing none. The wait cannot deadlock — the worker
// never waits on a producer, and Shutdown/eviction wait on persistDone too, not
// on this function.
//
// Anything still stranded in persistCh is stored FIRST, then extra, in order,
// so a single stranded call writes in commit order. That is best-effort ordering
// WITHIN one call only: two committing goroutines can be in the observer at once
// (see room.persistFallbackMu), so across concurrent stranded calls the adapter
// may see the room's updates out of commit order. Harmless for a CRDT log —
// updates merge order-independently — but do not read it as a stronger promise
// than it is. Errors are logged, not returned: the observer has no caller to
// report to. Auto-versioning and compaction are deliberately NOT re-run here —
// the worker already performed its final maybeVersion/maybeCompact, and a
// stranded update is by definition a late straggler, not a new steady state.
//
// The observer pays for this synchronously, on the committing goroutine, so a
// commit that returns after the worker retired has been stored. That is the
// intended trade: it happens only during and after room retirement, and the
// alternative — the pre-#229 behaviour — was silent data loss. Two consequences
// worth stating rather than discovering:
//
//   - crdt.buildPhase2 fires OnUpdate observers in registration order and
//     loadRoom registers persistence BEFORE the relay observers, so a stranded
//     write delays that update's cluster fan-out for as long as the adapter
//     takes. Bounded to room retirement, and a publish onto an already-retired
//     lane is counted in RelayStats().Dropped rather than lost silently — but
//     it does stretch the "no blocking I/O on the commit path" convention
//     further than the rest of the package does.
//   - It writes for a room name the server may already consider gone, and can
//     do so indefinitely if a caller retains the *crdt.Doc. See
//     CompactableAdapter's godoc for what that means for adapters.
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
			for _, update := range extra {
				store(update)
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
// that waits on the committer, and neither would move: the buffer exists so
// that a slow storage backend does not block the Transact caller (N-H7), and at
// the bound it did. So the update goes into persistCh only when that is
// immediate and nothing waits ahead of it; otherwise it queues, in commit
// order, on persistOverflow, which a feeder goroutine hands on
// (feedPersistence). While a feeder runs, every update queues behind the
// updates it holds, whatever the worker's state, so none overtakes them.
//
// The worker's retirement (#229) is read as before, and handled as before when
// no feeder runs. A send that completed while persistRetire was still open is
// swept by the worker's final drain, which strictly follows
// close(persistRetire); a send that raced it may have missed that drain, and an
// update the observer finds the worker retired for cannot be sent at all, so
// in both cases the observer writes itself (persistStranded). That one wait is
// on a worker that has stopped taking updates and is finishing its final
// drain, not on a running worker.
func (s *Server) persistUpdate(r *room, name string, update []byte) {
	r.persistMu.Lock()
	if r.persistDrained != nil {
		r.persistOverflow = append(r.persistOverflow, update)
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
	r.persistOverflow = append(r.persistOverflow, update)
	s.startFeederLocked(r, name)
	r.persistMu.Unlock()
}

// startFeederLocked starts the room's feeder (feedPersistence). It is counted
// with the observers that write for a retired worker (Server.stranded), so
// Shutdown waits for what the feeder still holds. The caller holds persistMu
// and has checked that no feeder runs.
func (s *Server) startFeederLocked(r *room, name string) {
	r.persistDrained = make(chan struct{})
	s.stranded.enter()
	go s.feedPersistence(r, name)
}

// feedPersistence hands the room's queued updates (persistOverflow) to its
// persistence worker, oldest first, waiting for the worker as the observer no
// longer does: the feeder holds nothing a committer or the adapter needs. An
// update stays at the head of the queue until its send completes, so the
// observer keeps queueing behind it and the worker receives the room's updates
// in commit order. Once the worker has retired, the feeder stores what it
// still holds, and whatever the worker's final drain left in the channel,
// through persistStranded, channel first. It ends when the queue is empty,
// closing persistDrained for the peers waiting on it (awaitPersistence).
func (s *Server) feedPersistence(r *room, name string) {
	defer s.stranded.leave()
	for {
		r.persistMu.Lock()
		if len(r.persistOverflow) == 0 {
			select {
			case <-r.persistRetire:
				r.persistMu.Unlock()
				s.persistStranded(r, name, nil)
				r.persistMu.Lock()
				if len(r.persistOverflow) > 0 {
					r.persistMu.Unlock()
					continue
				}
			default:
			}
			close(r.persistDrained)
			r.persistDrained = nil
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
			r.persistMu.Unlock()
		case <-r.persistRetire:
			r.persistMu.Lock()
			left := r.persistOverflow
			r.persistOverflow = nil
			r.persistMu.Unlock()
			s.persistStranded(r, name, left)
		}
	}
}

// awaitPersistence holds a peer's read loop, before it applies the peer's next
// update, while the room's persistence worker is behind by more than its
// 256-slot buffer (the feeder runs), so storage lag still slows a fast peer as
// the observer's send used to. The read loop holds nothing the worker or a
// committer needs, which is what makes this the place to wait. It returns once
// the queue drains, the worker retires, or done is closed.
func (r *room) awaitPersistence(done <-chan struct{}) {
	if r.persistCh == nil {
		return
	}
	r.persistMu.Lock()
	drained := r.persistDrained
	r.persistMu.Unlock()
	if drained == nil {
		return
	}
	select {
	case <-drained:
	case <-r.persistRetire:
	case <-done:
	}
}

// awaitOverflow returns once the room's queued updates have all been handed
// to the worker's channel, or stop is closed, so an on-demand flush (flushReq)
// covers every update committed before it, as it did when the observer's send
// waited.
func (r *room) awaitOverflow(stop <-chan struct{}) {
	r.persistMu.Lock()
	drained := r.persistDrained
	r.persistMu.Unlock()
	if drained == nil {
		return
	}
	select {
	case <-drained:
	case <-stop:
	}
}

// startPersistenceWorker spawns the goroutine that drains r.persistCh and
// forwards each update to the PersistenceAdapter. It must be called with
// r.persistCh, r.persistStop, r.persistRetire, and r.persistDone already
// initialised.
//
// The worker exits when either r.persistStop or s.shutdownCh is closed,
// draining any buffered updates before returning so that no committed
// transaction is silently lost.
//
// That guarantee needs a handshake with the producer, not just a final drain
// (#229). A drain is one-shot: producers — peer read loops above all — keep
// committing throughout Shutdown, and Shutdown closes shutdownCh long before it
// closes the peer connections. So the FIRST act of every exit path is to close
// r.persistRetire, publishing "I am leaving" before the final drain rather than
// after it. loadRoom's doc.OnUpdate observer reads that latch and either relies
// on the final drain (its send completed while the latch was open, so the drain
// necessarily follows it) or performs the write itself via persistStranded.
// Without the latch, everything committed after the drain landed in an unread
// 256-slot buffer and vanished.
//
// When coalescing is enabled (see resolveCoalesceConfig / Server.PersistCoalesceWindow),
// bursts of updates are debounced into a single merged write: each new update
// restarts the window timer, and a per-batch maxWait timer bounds staleness.
// When disabled (window < 0), each update is stored individually.
//
// If s.persistence implements PersistenceAdapterContext, store calls are made
// via StoreUpdateContext with a ctx that is cancelled when shutdown or stop
// fires — that cancellation exists so a blocking adapter cannot wedge Shutdown
// indefinitely. Every FINAL flush is the exception, on both paths: it uses
// context.Background() so the last write is not aborted by the very signal that
// triggers it (#229 — the strict path used to pass the cancellable ctx here,
// which made ctx-aware adapters discard the whole tail). Otherwise falls back
// to StoreUpdate (existing behaviour).
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
		//     the latch was open is guaranteed to be picked up by that drain;
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
			// pending holds updates whose store was ABORTED by ctx cancellation
			// rather than rejected by the adapter. Cancellation is a shutdown/stop
			// signal, not a verdict on the data, so the update is retained and
			// re-stored on the exit path under a background ctx — the same
			// retain-and-re-flush discipline the coalescing path already applies to
			// an unflushed batch. Without it a ctx-aware adapter loses every update
			// the worker happens to pick up in the window between shutdown
			// cancelling the ctx and the worker reaching its exit case (#229).
			// Mutated only from this goroutine.
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

			// drain stores everything currently buffered and reports whether every
			// store succeeded, so an on-demand flush can act as a durability barrier.
			drain := func(c context.Context) bool {
				ok := true
				for {
					select {
					case update := <-r.persistCh:
						if !storeRetaining(c, update) {
							ok = false
						}
					default:
						return ok
					}
				}
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
					ok := drain(ctx) // store everything currently buffered
					ack <- ok
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
		// drainBuffered pulls everything currently queued into batch without
		// blocking. Used on stop/shutdown before the final flush.
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
		// exit triggers. retire() must come FIRST so the doc.OnUpdate producer
		// can never hand an update to a channel this worker has already swept
		// for the last time (#229); the flush uses a background ctx so the last
		// batched write survives the very signal that triggered the exit.
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
				// batch — drain first so an on-demand flush never misses it.
				drainBuffered()
				wrote := len(batch) > 0
				ok := flush(context.Background(), batch)
				if ok {
					batch = nil
				}
				clearTimers()
				onFlushed(wrote && ok)
				// Report success so teardown can gate eviction on real durability.
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
