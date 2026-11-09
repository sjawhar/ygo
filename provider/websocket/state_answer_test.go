package websocket_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
	"github.com/reearth/ygo/encoding"
	ygws "github.com/reearth/ygo/provider/websocket"
	ygsync "github.com/reearth/ygo/sync"
)

// Tests for Server.OnStateAnswer, which runs before the server answers a
// peer's SyncStep1 with the room's state and whose release runs once that
// answer is sent.

// stateAnswerRoom is a server whose room "room" holds a text, so an answer to
// a SyncStep1 carries something, with the test HTTP server serving it.
// configure, when non-nil, runs before the server starts.
func stateAnswerRoom(t *testing.T, text string, configure func(*ygws.Server)) *httptest.Server {
	t.Helper()
	srv, ts := hocuspocusServer(t, configure)
	require.NoError(t, srv.Apply(context.Background(), stateAnswerRoomName,
		func(doc *crdt.Doc, transact func(func(*crdt.Transaction))) {
			txt := doc.GetText("t") // MUST be outside transact
			transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, text, nil) })
		}))
	return ts
}

const stateAnswerRoomName = "room"

// joinAndReadStep1 connects to the room and reads the SyncStep1 the server
// sends every peer on connect, so what follows is the answer to the peer's
// own SyncStep1 and nothing else.
func joinAndReadStep1(t *testing.T, ts *httptest.Server) *gws.Conn {
	t.Helper()
	conn := dial(t, ts, stateAnswerRoomName)
	tag, body, err := hpRead(conn, stateAnswerRoomName, 2*time.Second)
	require.NoError(t, err)
	require.Equal(t, tagSync, tag)
	sub, _, err := ygsync.ReadSyncMessage(body)
	require.NoError(t, err)
	require.Equal(t, ygsync.MsgSyncStep1, sub, "the server greets a peer with its own step-1")
	return conn
}

// readStateAnswer reads the frames of one answer, up to and including the
// SyncStep2 that ends it, and returns the sync messages they carried.
func readStateAnswer(t *testing.T, conn *gws.Conn) [][]byte {
	t.Helper()
	var msgs [][]byte
	for {
		tag, body, err := hpRead(conn, stateAnswerRoomName, 2*time.Second)
		require.NoError(t, err)
		if tag != tagSync {
			continue
		}
		msgs = append(msgs, body)
		sub, _, err := ygsync.ReadSyncMessage(body)
		require.NoError(t, err)
		if sub == ygsync.MsgSyncStep2 {
			return msgs
		}
	}
}

// The hook runs before the answer and its release after it: while the hook
// has not returned the peer is sent nothing and the release has not run, and
// the release runs once the answer is on its way.
func TestInteg_OnStateAnswer_RunsBeforeTheAnswerAndReleasesAfterIt(t *testing.T) {
	admit := make(chan struct{})
	called := make(chan ygws.StateAnswerInfo, 4)
	released := make(chan struct{}, 4)

	ts := stateAnswerRoom(t, "the room's state", func(srv *ygws.Server) {
		srv.OnStateAnswer = func(info ygws.StateAnswerInfo) (func(), error) {
			called <- info
			<-admit
			return func() { released <- struct{}{} }, nil
		}
	})

	conn := joinAndReadStep1(t, ts)
	// The frames the server sends from here on, read off the socket as they
	// arrive: a read with a deadline would leave the connection unusable
	// once it timed out, so the test watches this channel instead.
	sent := make(chan []byte, 32)
	go func() {
		for {
			tag, body, err := hpRead(conn, stateAnswerRoomName, 30*time.Second)
			if err != nil {
				close(sent)
				return
			}
			if tag == tagSync {
				sent <- body
			}
		}
	}()
	hpSend(t, conn, stateAnswerRoomName, tagSync, ygsync.EncodeSyncStep1(crdt.New()))

	var info ygws.StateAnswerInfo
	select {
	case info = <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("the hook never ran")
	}
	assert.Equal(t, stateAnswerRoomName, info.Room)
	assert.NotNil(t, info.Doc, "the hook is given the document the answer reads")
	assert.Positive(t, info.Structs, "the hook is told how many structs the answer decodes")
	assert.NotEmpty(t, info.Step1, "the hook is given the message it answers")

	// Nothing reaches the peer while the hook holds the answer, and the
	// release has not run: the hook runs before the answer, not beside it.
	time.Sleep(250 * time.Millisecond)
	require.Empty(t, sent, "the peer was answered before the hook returned")
	require.Empty(t, released, "the release ran before the hook returned")

	close(admit)
	for {
		var body []byte
		select {
		case body = <-sent:
		case <-time.After(2 * time.Second):
			t.Fatal("the admitted answer never reached the peer")
		}
		sub, _, err := ygsync.ReadSyncMessage(body)
		require.NoError(t, err)
		if sub == ygsync.MsgSyncStep2 {
			break
		}
	}
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("the release never ran")
	}
}

// The release runs on every way out of the answer, including the one that
// sends nothing: a SyncStep1 whose state vector does not decode is dropped,
// as it is without a hook, and what the hook weighed is given back.
func TestInteg_OnStateAnswer_ReleaseRunsWhenTheAnswerCannotBeEncoded(t *testing.T) {
	released := make(chan struct{}, 4)
	calls := make(chan struct{}, 4)
	ts := stateAnswerRoom(t, "the room's state", func(srv *ygws.Server) {
		srv.OnStateAnswer = func(ygws.StateAnswerInfo) (func(), error) {
			calls <- struct{}{}
			return func() { released <- struct{}{} }, nil
		}
	})

	conn := joinAndReadStep1(t, ts)
	// A step-1 message whose state vector is a truncated varuint: the peer
	// loop reads the message and EncodeSyncStep2Split cannot read it.
	hpSend(t, conn, stateAnswerRoomName, tagSync, encoding.EncodeBytes(func(enc *encoding.Encoder) {
		enc.WriteVarUint(uint64(ygsync.MsgSyncStep1))
		enc.WriteVarBytes([]byte{0xFF})
	}))

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("the release never ran for an answer that could not be encoded")
	}
	require.Len(t, calls, 1, "the hook runs once for the one SyncStep1")

	// The peer stays connected and is answered its next SyncStep1.
	hpSend(t, conn, stateAnswerRoomName, tagSync, ygsync.EncodeSyncStep1(crdt.New()))
	require.NotEmpty(t, readStateAnswer(t, conn))
}

// A hook that refuses sends no answer and closes the peer: with the code and
// reason of a *PeerClose it names, and with 4205 "Reset Connection" for a
// plain error or a panic.
func TestInteg_OnStateAnswer_ARefusalSendsNoAnswerAndClosesThePeer(t *testing.T) {
	for name, c := range map[string]struct {
		err    error
		code   int
		reason string
	}{
		"a named close": {
			err:    &ygws.PeerClose{Code: 4403, Reason: "the room is too large to answer now"},
			code:   4403,
			reason: "the room is too large to answer now",
		},
		"a plain error": {err: errors.New("no room for this answer"), code: 4205, reason: "Reset Connection"},
		"a panic":       {code: 4205, reason: "Reset Connection"},
	} {
		t.Run(name, func(t *testing.T) {
			released := make(chan struct{}, 4)
			ts := stateAnswerRoom(t, "the room's state", func(srv *ygws.Server) {
				srv.OnStateAnswer = func(ygws.StateAnswerInfo) (func(), error) {
					if c.err == nil {
						panic("the hook panicked")
					}
					return func() { released <- struct{}{} }, c.err
				}
			})

			conn := joinAndReadStep1(t, ts)
			hpSend(t, conn, stateAnswerRoomName, tagSync, ygsync.EncodeSyncStep1(crdt.New()))

			// Every frame up to the close, none of which may answer.
			for {
				tag, body, err := hpRead(conn, stateAnswerRoomName, 2*time.Second)
				if err != nil {
					var closed *gws.CloseError
					require.ErrorAs(t, err, &closed, "the refusal must close the connection")
					assert.Equal(t, c.code, closed.Code)
					assert.Equal(t, c.reason, closed.Text)
					break
				}
				if tag != tagSync {
					continue
				}
				sub, _, err := ygsync.ReadSyncMessage(body)
				require.NoError(t, err)
				require.Equal(t, ygsync.MsgSyncStep1, sub, "a refused answer must send no state")
			}
			require.Empty(t, released, "a refused answer runs no release")
		})
	}
}

// A server with no hook answers exactly as it does today, and a hook that
// admits the answer changes nothing about it: the same series of messages,
// of the same types, leaving the receiver holding the same document. The
// bytes themselves differ only in the client the room wrote its own structs
// under, which is a new random identity per room.
func TestInteg_OnStateAnswer_AServerWithNoHookAnswersAsBefore(t *testing.T) {
	text := strings.Repeat("the room's state, long enough to be cut into frames. ", 200)
	answer := func(configure func(*ygws.Server)) (subs []int, held string) {
		conn := joinAndReadStep1(t, stateAnswerRoom(t, text, configure))
		hpSend(t, conn, stateAnswerRoomName, tagSync, ygsync.EncodeSyncStep1(crdt.New()))
		receiver := crdt.New(crdt.WithClientID(99))
		for _, msg := range readStateAnswer(t, conn) {
			sub, _, err := ygsync.ReadSyncMessage(msg)
			require.NoError(t, err)
			subs = append(subs, sub)
			_, err = ygsync.ApplySyncMessage(receiver, msg, nil)
			require.NoError(t, err)
		}
		return subs, receiver.GetText("t").ToString()
	}

	noneSubs, noneHeld := answer(nil)
	require.NotEmpty(t, noneSubs)
	require.Equal(t, text, noneHeld)
	require.Greater(t, len(noneSubs), 1, "the answer must be cut into frames, so the comparison is worth making")

	calls := make(chan struct{}, 4)
	hookSubs, hookHeld := answer(func(srv *ygws.Server) {
		srv.OnStateAnswer = func(ygws.StateAnswerInfo) (func(), error) {
			calls <- struct{}{}
			return nil, nil
		}
	})
	assert.Equal(t, noneSubs, hookSubs, "a hook that admits the answer must not change its frames")
	assert.Equal(t, noneHeld, hookHeld, "a hook that admits the answer must not change what it carries")
	assert.Len(t, calls, 1, "the hook runs once for the one SyncStep1")
}

// parkedStructsUpdate is a V1 update of n one-character structs for client 2
// at clocks 1 to n, each written beside the one before it. Clock 0 is never
// sent, so a room that has not seen it parks every one of them - and carries
// every one of them in the answer to a SyncStep1 (crdt.SplitUpdateV1).
func parkedStructsUpdate(n int) []byte {
	return encoding.EncodeBytes(func(enc *encoding.Encoder) {
		enc.WriteVarUint(1) // clients
		enc.WriteVarUint(uint64(n))
		enc.WriteVarUint(2) // client
		enc.WriteVarUint(1) // first clock: clock 0 is the gap
		for i := range n {
			enc.WriteUint8(4 | 0x80)    // a string with a left origin
			enc.WriteVarUint(2)         // the origin's client
			enc.WriteVarUint(uint64(i)) // its clock, the struct before this one
			enc.WriteVarString("x")
		}
		enc.WriteVarUint(0) // delete set: no clients
	})
}

// The count the hook is given is the count of what the answer encodes, and
// a hook that waits can read it again.
//
// It understated the answer twice. It left out the structs waiting in the
// room's pending queue, which the answer carries like any other, so a room
// answering with a hundred parked structs told the hook it held one; and it
// is read before the hook is called while the answer is built after the
// hook returns, so a hook that waits for room sized its reservation on the
// room as it was when it began to wait. An owner granting memory on this
// figure under-reserved on both counts.
func TestInteg_OnStateAnswer_TheCountCoversWhatTheAnswerEncodes(t *testing.T) {
	const parked, arrivedWhileWaiting = 100, 40
	admit := make(chan struct{})
	called := make(chan ygws.StateAnswerInfo, 4)
	now := make(chan int, 4)

	srv, ts := hocuspocusServer(t, func(s *ygws.Server) {
		s.OnStateAnswer = func(info ygws.StateAnswerInfo) (func(), error) {
			called <- info
			<-admit
			now <- info.StructsNow()
			return nil, nil
		}
	})
	require.NoError(t, srv.Apply(context.Background(), stateAnswerRoomName,
		func(doc *crdt.Doc, transact func(func(*crdt.Transaction))) {
			txt := doc.GetText("t")
			transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, "seed", nil) })
		}))
	room := srv.GetDoc(stateAnswerRoomName)
	require.NotNil(t, room)
	require.NoError(t, crdt.ApplyUpdateV1(room, parkedStructsUpdate(parked), nil))
	require.Equal(t, parked, room.PendingStats().Items, "the room must hold the parked structs")
	stored := room.StructCount()

	conn := joinAndReadStep1(t, ts)
	hpSend(t, conn, stateAnswerRoomName, tagSync, ygsync.EncodeSyncStep1(crdt.New()))

	var info ygws.StateAnswerInfo
	select {
	case info = <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("the hook never ran")
	}
	assert.Equal(t, stored+parked, info.Structs,
		"the figure must count the parked structs the answer carries, not only the stored ones")

	// The room goes on taking updates while the hook holds the answer, and
	// the answer is built from the room as it is then.
	require.NoError(t, crdt.ApplyUpdateV1(room, parkedStructsUpdate(parked+arrivedWhileWaiting), nil))
	close(admit)
	select {
	case got := <-now:
		assert.Equal(t, stored+parked+arrivedWhileWaiting, got,
			"StructsNow must report the room as it is when the hook admits the answer")
		assert.Greater(t, got, info.Structs, "the figure read before the wait is the stale one")
	case <-time.After(2 * time.Second):
		t.Fatal("the hook never read the count again")
	}
	require.NotEmpty(t, readStateAnswer(t, conn))
}

// A release that panics does not take the peer down with it: it goes
// through the recovery every other callback of the server's does, the
// answer it released has already been sent, and the peer answers the next
// SyncStep1.
func TestInteg_OnStateAnswer_APanickingReleaseIsRecovered(t *testing.T) {
	ts := stateAnswerRoom(t, "the room's state", func(srv *ygws.Server) {
		srv.OnStateAnswer = func(ygws.StateAnswerInfo) (func(), error) {
			return func() { panic("the release panicked") }, nil
		}
	})

	conn := joinAndReadStep1(t, ts)
	hpSend(t, conn, stateAnswerRoomName, tagSync, ygsync.EncodeSyncStep1(crdt.New()))
	require.NotEmpty(t, readStateAnswer(t, conn), "the answer the release released must reach the peer")

	hpSend(t, conn, stateAnswerRoomName, tagSync, ygsync.EncodeSyncStep1(crdt.New()))
	require.NotEmpty(t, readStateAnswer(t, conn), "the peer must still be answered after the panic")
}
