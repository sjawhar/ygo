package websocket

import (
	"errors"

	"github.com/reearth/ygo/crdt"
	ygsync "github.com/reearth/ygo/sync"
)

// StateAnswerInfo is passed to Server.OnStateAnswer for each SyncStep1 the
// server is about to answer with the room's state. Fields may be added in
// future versions; callers must not rely on the struct being fixed-size.
type StateAnswerInfo struct {
	// Room is the room name the SyncStep1 arrived on.
	Room string
	// Doc is the room's document, whose state the answer carries. The hook
	// must not apply to it or take a transaction on it: the answer reads it
	// as soon as the hook returns, on this goroutine.
	Doc *crdt.Doc
	// Structs is how many structs the answer may have to encode: every
	// struct Doc holds, the ones waiting in its pending queue included,
	// since the answer carries those too (crdt.SplitUpdateV1). It is read
	// the instant before the hook is called and is a snapshot: the room
	// goes on taking updates while a hook that waits holds the answer, so
	// a hook that waits must read StructsNow before it decides how much to
	// reserve. The figure is a count of structs, not of bytes; what one
	// struct costs to decode depends on the content it carries.
	Structs int
	// Step1 is the sync message the peer sent, whose state vector decides
	// how much of Doc the answer carries. The hook must not modify or
	// retain it.
	Step1 []byte
}

// StructsNow is Structs read again, for a hook that waited before it
// admitted the answer: the answer is built after the hook returns, so a
// reservation sized on a figure read before a wait is a reservation sized
// on a room that has since grown.
func (i StateAnswerInfo) StructsNow() int {
	return answerStructs(i.Doc)
}

// answerStructs is how many structs an answer carrying the whole of doc
// would encode: what the store holds and what waits in the pending queue,
// which the answer carries as well (crdt.SplitUpdateV1, withParked). A nil
// doc is no structs.
func answerStructs(doc *crdt.Doc) int {
	if doc == nil {
		return 0
	}
	return doc.StructCount() + doc.PendingStats().Items
}

// StateAnswerHook is the signature of Server.OnStateAnswer, which runs
// immediately before the server answers a peer's SyncStep1 with the room's
// state and whose release runs once that answer has been handed to the
// peer's writer.
//
// What it guards is the memory of one answer. Answering decodes the whole of
// what the peer is missing into a merge store, in order to cut it into frames
// each small enough for a browser to apply in one task
// (sync.EncodeSyncStep2Split, crdt.SplitUpdateV1), so the server holds the
// room's state decoded, once per answer, however small the SyncStep1 that
// asked for it was. A room with many peers joining at once answers each of
// them, and nothing else in the server bounds how many of those decodes run
// together: an owner that must not be driven out of memory by a join storm
// weighs each answer here, and lets it through, makes it wait, or refuses it.
//
// The hook runs on the peer's read loop, so the peer's next frame waits for
// it, and the answer is read and sent as soon as it returns. A non-nil
// release runs once the answer's frames are in the peer's write queue, which
// is where the merge store is released and Server.PeerWriteQueueBytes takes
// over the accounting; it runs exactly once, also when the answer could not
// be encoded and nothing was sent. A nil release is no release to run, and a
// release that panics is recovered and logged, as the hook itself is, so it
// does not take the peer's read loop down with it.
//
// A hook that returns a non-nil error refuses the answer: nothing is sent and
// the peer is closed, with the code and reason of a *PeerClose when the error
// is or wraps one and with 4205 "Reset Connection" otherwise, as a refused
// peer update is closed (PeerUpdateHook). A hook that panics is recovered,
// logged and treated as such a refusal. A refusal runs no release: a hook
// that refuses the answer reserved nothing for it, so a release returned
// beside an error is dropped. A refusal is a close rather than a silent drop
// because a peer that sent SyncStep1 and is answered nothing waits for a sync
// that never comes: @hocuspocus/provider 2.x reconnects at once after 4205
// and tries again.
type StateAnswerHook func(info StateAnswerInfo) (release func(), err error)

// errStateAnswerPanic refuses the answer of a hook that panicked, which
// safeHook recovered and logged (StateAnswerHook).
var errStateAnswerPanic = errors.New("ygo/websocket: OnStateAnswer panicked")

// answerSyncStep1 answers step1, a peer's SyncStep1 message, with the series
// of sync messages carrying what its sender is missing, through
// Server.OnStateAnswer when one is set. A message the server cannot encode is
// dropped and logged, as it is without a hook.
func (p *peer) answerSyncStep1(step1 []byte) {
	release, closeWith := p.beginStateAnswer(step1)
	if closeWith != nil {
		p.enqueueClose(closeWith.Code, closeWith.Reason)
		return
	}
	if release != nil {
		// Through safeHook, as every other callback of the server's is: a
		// release that panics is the embedder's fault, and letting it
		// escape here would take down the peer's read loop with it, on a
		// path the answer has already succeeded on.
		defer p.server.safeHook("OnStateAnswer release", release)
	}
	// Peer sent step-1 — send the room's state only to them, cut into the
	// frames stateFrameBytes allows so that applying one is one short task
	// on the receiver. Read-only peers may still request state (#59).
	reply, err := ygsync.EncodeSyncStep2Split(p.room.doc, step1, p.stateMessageBytes())
	if err != nil {
		p.server.log().Debug("discarded unappliable sync message",
			"room", p.roomName, "err", err)
		return
	}
	p.sendSyncSeries(reply)
}

// beginStateAnswer runs Server.OnStateAnswer for the answer about to be
// encoded, and reports the release to run once it has been sent and, when
// the hook refused it, the close's sendable code and reason (see
// StateAnswerHook). With no hook it reports no release and no close, which is
// the behaviour of a server that sets none.
func (p *peer) beginStateAnswer(step1 []byte) (release func(), closeWith *PeerClose) {
	hook := p.server.OnStateAnswer
	if hook == nil {
		return nil, nil
	}
	var (
		hookErr  error
		returned bool
	)
	p.server.safeHook("OnStateAnswer", func() {
		release, hookErr = hook(StateAnswerInfo{
			Room:    p.roomName,
			Doc:     p.room.doc,
			Structs: answerStructs(p.room.doc),
			Step1:   step1,
		})
		returned = true
	})
	if !returned {
		// safeHook recovered a panic: the hook admitted nothing, so the
		// answer is refused as it is for an error (StateAnswerHook).
		hookErr = errStateAnswerPanic
	}
	if hookErr == nil {
		return release, nil
	}
	// A hook that refuses reserved nothing, so a release returned beside
	// its error is not run.
	p.server.log().Debug("state answer refused", "room", p.roomName, "err", hookErr)
	var refusal *PeerClose
	if errors.As(hookErr, &refusal) && refusal != nil {
		if code, reason, ok := sendableClose(refusal.Code, refusal.Reason); ok {
			return nil, &PeerClose{Code: code, Reason: reason}
		}
	}
	return nil, &PeerClose{Code: wsCodeResetConnection, Reason: wsReasonResetConnection}
}
