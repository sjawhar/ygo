package websocket_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
	ygsync "github.com/reearth/ygo/sync"
)

func TestInteg_PersistedRoom_ParkedDeletesSurviveRestart(t *testing.T) {
	srv, ts, adapter := persistingServer(t, 200)
	peer := hpDial(t, ts, "room", false, crdt.New(crdt.WithClientID(9)))
	room := srv.GetDoc("room")
	require.NotNil(t, room)
	var events atomic.Int32
	room.OnUpdate(func([]byte, any) { events.Add(1) })

	author := crdt.New(crdt.WithClientID(1))
	text := author.GetText("t")
	author.Transact(func(txn *crdt.Transaction) { text.Insert(txn, 0, "abc", nil) })
	missing := crdt.EncodeStateAsUpdateV1(author, nil)
	before := author.StateVector()
	author.Transact(func(txn *crdt.Transaction) { text.Delete(txn, 0, 1) })
	deletion := crdt.EncodeStateAsUpdateV1(author, before)

	hpSend(t, peer, "room", tagSync, ygsync.EncodeUpdate(deletion))
	require.Equal(t, []uint64{1}, syncStatuses(t, peer, "room"))
	require.Equal(t, 1, room.PendingStats().DeleteRanges)
	require.Eventually(t, func() bool { return len(adapter.stored()) == 1 }, 5*time.Second, 5*time.Millisecond)
	// Replaying repeated parked deletes must remain idempotent.
	hpSend(t, peer, "room", tagSync, ygsync.EncodeUpdate(deletion))
	require.Equal(t, []uint64{1}, syncStatuses(t, peer, "room"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
	require.Zero(t, events.Load(), "parking a deletion does not emit an OnUpdate event")
	stored := adapter.stored()
	require.NotEmpty(t, stored)

	replay := crdt.New(crdt.WithClientID(10))
	for _, update := range stored {
		require.NoError(t, crdt.ApplyUpdateV1(replay, update, nil))
	}
	require.Equal(t, 1, replay.PendingStats().DeleteRanges)
	require.NoError(t, crdt.ApplyUpdateV1(replay, missing, nil))
	require.Equal(t, "bc", replay.GetText("t").ToString())
	require.Zero(t, replay.PendingStats().DeleteRanges)
}
