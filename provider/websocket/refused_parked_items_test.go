package websocket_test

import (
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
	ygws "github.com/reearth/ygo/provider/websocket"
	ygsync "github.com/reearth/ygo/sync"
)

// A room configured as Dispatch configures it (Hocuspocus framing, one
// StoreUpdate per update, a pending cap and no scan bound) refuses an update
// that would park more items than the cap without changing anything: the
// update inserts "hello" and sets five keys on a map whose creator the room
// never heard from, with a cap of two. The room's state and text are as they
// were, its OnUpdate observers are handed nothing, nothing is stored, the
// other peer gets no document update and the sender gets SyncStatus(0). In
// v1.51.3-sami.9 the room kept and stored "hello" and fired OnUpdate, while
// the sender was told 0 and the other peer was sent nothing. Once the map's
// creator is in, the same update parks nothing and applies, and reaches both.
func TestInteg_RefusedParkedItemUpdateLeavesTheRoomUnchanged(t *testing.T) {
	const limit, parked = 2, 5
	adapter := &recordingAdapter{}
	srv := ygws.NewServerWithPersistence(adapter)
	srv.HocuspocusFraming = true
	srv.PersistCoalesceWindow = -1
	srv.MaxPendingItems = limit
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	creator := crdt.New(crdt.WithClientID(1))
	root := creator.GetMap("m")
	creator.Transact(func(txn *crdt.Transaction) { root.Set(txn, "nested", crdt.NewMapPrelim()) })
	created := crdt.EncodeStateAsUpdateV1(creator, nil)
	editor := crdt.New(crdt.WithClientID(2))
	require.NoError(t, crdt.ApplyUpdateV1(editor, created, nil))
	v, _ := editor.GetMap("m").Get("nested")
	nested, ok := v.(*crdt.YMap)
	require.True(t, ok, "nested is %T", v)
	text := editor.GetText("t")
	editor.Transact(func(txn *crdt.Transaction) {
		text.Insert(txn, 0, "hello", nil)
		for i := range parked {
			nested.Set(txn, "k"+strconv.Itoa(i), i)
		}
	})
	refused := crdt.EncodeStateAsUpdateV1(editor, creator.StateVector())

	sender := hpDial(t, ts, "room", false, crdt.New(crdt.WithClientID(7)))
	other := hpDial(t, ts, "room", false, crdt.New(crdt.WithClientID(6)))
	room := srv.GetDoc("room")
	require.NotNil(t, room)
	before := crdt.EncodeStateAsUpdateV1(room, nil)
	var observed atomic.Int32
	room.OnUpdate(func([]byte, any) { observed.Add(1) })

	hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(refused))
	assert.Equal(t, []uint64{0}, syncStatuses(t, sender, "room"), "the sender is told the update was not applied")
	assert.Equal(t, 0, documentUpdates(t, other, "room"), "the other peer gets no document update")
	assert.Empty(t, room.GetText("t").ToString(), "the room's text")
	assert.Equal(t, before, crdt.EncodeStateAsUpdateV1(room, nil), "the room's state")
	assert.Zero(t, room.PendingStats().Items, "the room parks nothing of the refused update")
	assert.Equal(t, int32(0), observed.Load(), "the room's OnUpdate observers were handed the refused update")

	hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(created))
	hpSend(t, sender, "room", tagSync, ygsync.EncodeUpdate(refused))
	assert.Equal(t, []uint64{1, 1}, syncStatuses(t, sender, "room"))
	assert.Equal(t, 2, documentUpdates(t, other, "room"), "the other peer gets the map's creation and the edit")
	assert.Equal(t, "hello", room.GetText("t").ToString())
	assert.Equal(t, int32(2), observed.Load())

	require.Eventually(t, func() bool { return len(adapter.stored()) >= 2 }, 5*time.Second, 5*time.Millisecond)
	stored := adapter.stored()
	assert.Len(t, stored, 2, "the room stored the map's creation and the edit, and nothing of the refused update")
	replay := crdt.New()
	for _, u := range stored {
		require.NoError(t, crdt.ApplyUpdateV1(replay, u, nil))
	}
	assert.Equal(t, "hello", replay.GetText("t").ToString())
	assert.Zero(t, replay.PendingStats().Items, "the stored updates park nothing")
}
