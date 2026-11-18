package websocket

import (
	"context"
	"sync"
	"unsafe"
)

// PersistenceAdapterStored is an optional extension to PersistenceAdapter
// for Server.AnswerOnceStored. StoreUpdateStored is StoreUpdateContext that
// also says whether the update is stored once the call returns: an adapter
// may return nil for an update it dropped (one of a room it has given up on,
// say), which AnswerOnceStored must not answer as stored. Under
// AnswerOnceStored the persistence worker calls it, when the adapter
// implements it, in place of StoreUpdateContext and StoreUpdate; otherwise an
// update is stored when the call that took it returned nil. Without the
// option it is never called.
type PersistenceAdapterStored interface {
	StoreUpdateStored(ctx context.Context, room string, update []byte) (stored bool, err error)
}

// answersOnceStored is whether a peer's answers wait for the store: under
// AnswerOnceStored, on a server with persistence. Without persistence nothing
// stores an update, and every answer goes out as without the option.
func (s *Server) answersOnceStored() bool {
	return s.AnswerOnceStored && s.persistence != nil
}

// storeOne is one adapter call for update, the call the persistence worker
// makes, and whether it stored update: under AnswerOnceStored,
// StoreUpdateStored's answer where the adapter implements it, and otherwise a
// nil error.
func (s *Server) storeOne(ctx context.Context, name string, update []byte) (bool, error) {
	if st, ok := s.persistence.(PersistenceAdapterStored); ok && s.answersOnceStored() {
		return st.StoreUpdateStored(ctx, name, update)
	}
	var err error
	if pac, ok := s.persistence.(PersistenceAdapterContext); ok {
		err = pac.StoreUpdateContext(ctx, name, update)
	} else {
		err = s.persistence.StoreUpdate(name, update)
	}
	return err == nil, err
}

// updateKey is what a waiting answer is found by: the first byte of the very
// update slice the room's OnUpdate observers were handed, which the
// persistence worker is handed too. While an answer waits, its update is
// held by the worker's queue, so no other update has that address.
func updateKey(update []byte) *byte { return unsafe.SliceData(update) }

// noteStored completes the wait of the answer waiting on update, if one is,
// with whether it was stored. A second call for the same update finds none.
// It runs on the persistence worker's goroutine, or on the committing one for
// a retired room's write (persistStranded).
func (s *Server) noteStored(r *room, update []byte, stored bool) {
	if !s.answersOnceStored() || len(update) == 0 {
		return
	}
	if w, ok := r.storeWaiters.LoadAndDelete(updateKey(update)); ok {
		w.(*storeWait).complete(stored)
	}
}

// armStoreWait is the first step of the room's persistence observer, which
// runs on the committing goroutine before the update reaches the worker: the
// commit of the peer apply in hand (origin, that peer) is the update that
// peer's answer waits on. A peer's apply runs on its read loop, which is also
// the goroutine that armed the wait and reads it back (applySyncUpdate), so
// taken needs no lock. A commit of anything else, and a second commit of the
// same apply (the format cleanup a remote apply may run), takes nothing.
func (s *Server) armStoreWait(r *room, update []byte, origin any) {
	if !s.answersOnceStored() || len(update) == 0 {
		return
	}
	p, ok := origin.(*peer)
	if !ok || p.server != s || p.storeWait == nil || p.storeWait.taken {
		return
	}
	p.storeWait.taken = true
	r.storeWaiters.Store(updateKey(update), p.storeWait)
}

// storeWait is one apply's wait for the store of its commit.
type storeWait struct {
	taken bool // the apply's commit is the update waited on; the read loop's own

	mu               sync.Mutex
	resolved, stored bool
	then             func(stored bool)
}

// complete records whether the update was stored and runs the wait's
// continuation if the read loop has handed it one.
func (w *storeWait) complete(stored bool) {
	w.mu.Lock()
	if then := w.then; then != nil {
		w.then, w.resolved = nil, true
		w.mu.Unlock()
		then(stored)
		return
	}
	w.resolved, w.stored = true, stored
	w.mu.Unlock()
}

// whenStored runs then once the store is known, at once when it already is.
func (w *storeWait) whenStored(then func(stored bool)) {
	w.mu.Lock()
	if w.resolved {
		stored := w.stored
		w.mu.Unlock()
		then(stored)
		return
	}
	w.then = then
	w.mu.Unlock()
}

// peerAnswers is what a peer is owed under AnswerOnceStored, in the order its
// frames arrived: each frame's SyncStatus, and a close the server decided on
// (a hook's refusal, Server.ClosePeer). Each goes out once it is ready and
// everything before it has gone out.
type peerAnswers struct {
	mu    sync.Mutex
	queue []*peerAnswer
}

// peerAnswer is one entry: a frame's SyncStatus (status, applied), a close,
// or both, ready once its store is known.
type peerAnswer struct {
	ready, status, applied bool
	close                  *PeerClose
}

// reserveAnswer takes the next place in p's answers for a frame's SyncStatus.
func (p *peer) reserveAnswer() *peerAnswer {
	a := &peerAnswer{status: true}
	p.answers.mu.Lock()
	p.answers.queue = append(p.answers.queue, a)
	p.answers.mu.Unlock()
	return a
}

// resolveAnswer makes a ready with its answer and close, and sends every
// ready entry at the head of p's answers, in order. It holds the answers'
// lock while it sends, so two goroutines resolving at once cannot send out of
// order.
func (p *peer) resolveAnswer(a *peerAnswer, applied bool, closeWith *PeerClose) {
	q := &p.answers
	q.mu.Lock()
	defer q.mu.Unlock()
	a.ready, a.applied, a.close = true, applied, closeWith
	for len(q.queue) > 0 && q.queue[0].ready {
		head := q.queue[0]
		q.queue[0] = nil
		q.queue = q.queue[1:]
		if head.status {
			p.sendSyncStatus(head.applied)
		}
		if head.close != nil {
			p.closeFromAnyGoroutine(head.close.Code, head.close.Reason)
		}
	}
}

// closeFromAnyGoroutine is enqueueClose for a caller on any goroutine. It
// holds wmu across the send of the close sentinel, as write holds it across
// its send, so the send cannot race handleDisconnect's close of writeCh, and
// sends the close frame itself, once wmu is released, when the queue is full.
// It reports whether it queued or sent the close: false for a peer that is
// closed or already closing.
func (p *peer) closeFromAnyGoroutine(code int, reason string) bool {
	p.inputClosed.Store(true)
	p.wmu.Lock()
	if p.closed || p.closeCode != 0 {
		p.wmu.Unlock()
		return false
	}
	p.closeCode, p.closeReason = code, reason
	queued := false
	select {
	case p.writeCh <- nil:
		queued = true
	default:
	}
	p.wmu.Unlock()
	if !queued {
		p.sendCloseFrame()
	}
	return true
}

// ClosePeer closes the peer whose updates carry origin as their transaction
// origin - PeerUpdateInfo.Origin, or a sender a crdt error names, such as
// crdt.DrainDiscardedError.Senders - with code and reason, and reports
// whether origin was a peer of s still open. It may be called from any
// goroutine, an OnPeerUpdate hook included. A code ServeHTTP would not send
// (see PeerClose) closes with 4205 "Reset Connection" instead, and a reason
// is cut as a PeerClose's is. Nothing the peer sends from then on lands.
//
// Under AnswerOnceStored the close goes out after every SyncStatus the peer
// is owed for frames it sent before the call, once those are answered, so a
// client reads the answers to its frames before the close; otherwise it goes
// out behind what the peer's write queue holds, as a hook's refusal does.
func (s *Server) ClosePeer(origin any, code int, reason string) bool {
	p, ok := origin.(*peer)
	if !ok || p.server != s {
		return false
	}
	if c, r, sendable := sendableClose(code, reason); sendable {
		code, reason = c, r
	} else {
		code, reason = wsCodeResetConnection, wsReasonResetConnection
	}
	p.wmu.Lock()
	open := !p.closed && p.closeCode == 0
	p.wmu.Unlock()
	if !open {
		return false
	}
	p.inputClosed.Store(true)
	if s.answersOnceStored() {
		a := &peerAnswer{}
		p.answers.mu.Lock()
		p.answers.queue = append(p.answers.queue, a)
		p.answers.mu.Unlock()
		p.resolveAnswer(a, false, &PeerClose{Code: code, Reason: reason})
		return true
	}
	return p.closeFromAnyGoroutine(code, reason)
}
