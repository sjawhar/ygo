package websocket_test

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
	ygws "github.com/reearth/ygo/provider/websocket"
	ygsync "github.com/reearth/ygo/sync"
)

// applyDocumentUpdates sends a Ping on conn and reads frames until its Pong,
// applying each document update (Sync frame) to doc, and returns how many it
// applied.
func applyDocumentUpdates(t *testing.T, conn *gws.Conn, room string, doc *crdt.Doc) int {
	t.Helper()
	hpSend(t, conn, room, tagPing, nil)
	n := 0
	for {
		tag, body, err := hpRead(conn, room, 2*time.Second)
		require.NoError(t, err)
		switch tag {
		case tagPong:
			return n
		case tagSync:
			_, err := ygsync.ApplySyncMessage(doc, body, nil)
			require.NoError(t, err)
			n++
		}
	}
}

// truncatedEdit returns "hello" written by client 1 and that client's next
// edit, which appends "!" and deletes two characters, with its last byte cut
// off: its struct decodes and integrates, then its delete set does not decode.
func truncatedEdit() (seed, edit []byte) {
	doc := crdt.New(crdt.WithClientID(1))
	text := doc.GetText("t")
	doc.Transact(func(txn *crdt.Transaction) { text.Insert(txn, 0, "hello", nil) })
	seed = crdt.EncodeStateAsUpdateV1(doc, nil)
	sv := doc.StateVector()
	doc.Transact(func(txn *crdt.Transaction) {
		text.Insert(txn, 5, "!", nil)
		text.Delete(txn, 1, 2)
	})
	whole := crdt.EncodeStateAsUpdateV1(doc, sv)
	return seed, whole[:len(whole)-1]
}

// dispatchServer is a server configured as Dispatch configures it: Hocuspocus
// framing, one StoreUpdate per update, a pending cap of 1<<20 and no scan
// bound.
func dispatchServer(t *testing.T, adapter *recordingAdapter) (*ygws.Server, *httptest.Server) {
	t.Helper()
	srv := ygws.NewServerWithPersistence(adapter)
	srv.HocuspocusFraming = true
	srv.PersistCoalesceWindow = -1
	srv.MaxPendingItems = 1 << 20
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts
}

// A room configured as Dispatch configures it takes an edit whose delete set
// was cut short: its struct ("!") integrates, then the delete set fails to
// decode. The room keeps the "!", its OnUpdate observers are told and it
// stores it, so the room's other peers must get it too. In v1.51.3-sami.10's
// candidate the other peer got no document update while the room's text
// became "hello!". Now the other peer gets exactly what the room committed and
// holds what the room and its store hold; the sender is answered 0, since the
// room did not take its whole edit.
func TestInteg_PartlyAppliedUpdateReachesTheOtherPeers(t *testing.T) {
	for _, viaHook := range []bool{false, true} {
		name := "without a hook"
		if viaHook {
			name = "through OnPeerUpdate"
		}
		t.Run(name, func(t *testing.T) {
			adapter := &recordingAdapter{}
			srv, ts := dispatchServer(t, adapter)
			if viaHook {
				srv.OnPeerUpdate = func(_ ygws.PeerUpdateInfo, apply func() error) error { return apply() }
			}
			seed, edit := truncatedEdit()

			sender := hpDial(t, ts, "room", false, crdt.New(crdt.WithClientID(7)))
			otherDoc := crdt.New(crdt.WithClientID(6))
			other := hpDial(t, ts, "room", false, otherDoc)
			room := srv.GetDoc("room")
			require.NotNil(t, room)

			hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(seed))
			require.Equal(t, []uint64{1}, syncStatuses(t, sender, "room"))
			require.Equal(t, 1, applyDocumentUpdates(t, other, "room", otherDoc))
			require.Equal(t, "hello", otherDoc.GetText("t").ToString())
			beforeEdit := crdt.EncodeStateAsUpdateV1(room, nil)

			var (
				mu       sync.Mutex
				observed [][]byte
			)
			room.OnUpdate(func(u []byte, _ any) {
				mu.Lock()
				defer mu.Unlock()
				observed = append(observed, append([]byte(nil), u...))
			})

			hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(edit))
			assert.Equal(t, []uint64{0}, syncStatuses(t, sender, "room"), "the room did not take the whole edit")
			assert.Equal(t, "hello!", room.GetText("t").ToString(), "the room keeps the struct that integrated")
			assert.Equal(t, 1, applyDocumentUpdates(t, other, "room", otherDoc), "the other peer gets what the room committed")
			assert.Equal(t, room.GetText("t").ToString(), otherDoc.GetText("t").ToString())
			assert.Equal(t, crdt.EncodeStateAsUpdateV1(room, nil), crdt.EncodeStateAsUpdateV1(otherDoc, nil),
				"the other peer holds what the room holds")

			// What the room's update observer received for the edit brings a
			// replica of the room before it to the same state the relay did.
			mu.Lock()
			require.Len(t, observed, 1, "the room's OnUpdate observers are told once")
			committed := observed[0]
			mu.Unlock()
			replica := crdt.New()
			require.NoError(t, crdt.ApplyUpdateV1(replica, beforeEdit, nil))
			require.NoError(t, crdt.ApplyUpdateV1(replica, committed, nil))
			assert.Equal(t, crdt.EncodeStateAsUpdateV1(otherDoc, nil), crdt.EncodeStateAsUpdateV1(replica, nil))

			require.Eventually(t, func() bool { return len(adapter.stored()) >= 2 }, 5*time.Second, 5*time.Millisecond)
			stored := crdt.New()
			for _, u := range adapter.stored() {
				require.NoError(t, crdt.ApplyUpdateV1(stored, u, nil))
			}
			assert.Equal(t, crdt.EncodeStateAsUpdateV1(otherDoc, nil), crdt.EncodeStateAsUpdateV1(stored, nil),
				"the other peer holds what the room stored")
		})
	}
}

// A bounded room takes the same edit back whole, so it relays nothing of it:
// the room, its store and the other peer stay at "hello". A later edit from
// another writer is stored after it, so the store's order shows that the
// edit taken back was never stored.
func TestInteg_PartlyAppliedUpdateOfABoundedRoomRelaysNothing(t *testing.T) {
	adapter := &recordingAdapter{}
	srv, ts := dispatchServer(t, adapter)
	srv.OnLoadDocument = func(_ context.Context, _ string, doc *crdt.Doc) error {
		doc.SetMaxIntegrateScan(1 << 40)
		return nil
	}
	seed, edit := truncatedEdit()
	later := textUpdate(crdt.New(crdt.WithClientID(2)), "X")

	sender := hpDial(t, ts, "room", false, crdt.New(crdt.WithClientID(7)))
	otherDoc := crdt.New(crdt.WithClientID(6))
	other := hpDial(t, ts, "room", false, otherDoc)
	room := srv.GetDoc("room")
	require.NotNil(t, room)

	hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(seed))
	hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(edit))
	assert.Equal(t, []uint64{1, 0}, syncStatuses(t, sender, "room"))
	assert.Equal(t, 1, applyDocumentUpdates(t, other, "room", otherDoc), "the other peer gets the seed only")
	assert.Equal(t, "hello", room.GetText("t").ToString())
	assert.Equal(t, crdt.EncodeStateAsUpdateV1(room, nil), crdt.EncodeStateAsUpdateV1(otherDoc, nil))

	hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(later))
	assert.Equal(t, []uint64{1}, syncStatuses(t, sender, "room"))
	require.Eventually(t, func() bool { return len(adapter.stored()) >= 2 }, 5*time.Second, 5*time.Millisecond)
	stored := adapter.stored()
	assert.Len(t, stored, 2, "the room stored the seed and the later edit, and nothing of the edit it took back")
	replay := crdt.New()
	for _, u := range stored {
		require.NoError(t, crdt.ApplyUpdateV1(replay, u, nil))
	}
	assert.Equal(t, crdt.EncodeStateAsUpdateV1(room, nil), crdt.EncodeStateAsUpdateV1(replay, nil))
}
