package crdt_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/crdt"
)

func TestUnit_Doc_UpdateLog_ParkedReplayAndClose(t *testing.T) {
	parent := crdt.New(crdt.WithClientID(1))
	root := parent.GetMap("m")
	parent.Transact(func(txn *crdt.Transaction) { root.Set(txn, "nested", crdt.NewMapPrelim()) })
	parentUpdate := crdt.EncodeStateAsUpdateV1(parent, nil)
	author := crdt.New(crdt.WithClientID(2))
	require.NoError(t, crdt.ApplyUpdateV1(author, parentUpdate, nil))
	nested, ok := author.GetMap("m").Get("nested")
	require.True(t, ok)
	before := author.StateVector()
	author.Transact(func(txn *crdt.Transaction) { nested.(*crdt.YMap).Set(txn, "key", "value") })
	parked := crdt.EncodeStateAsUpdateV1(author, before)

	room := crdt.New(crdt.WithClientID(3))
	var updates int
	origin := &struct{ name string }{"author"}
	log := room.OpenUpdateLog()
	defer log.Close()
	room.OnUpdate(func([]byte, any) { updates++ })
	require.NoError(t, crdt.ApplyUpdateV1(room, parked, origin))
	require.Equal(t, 0, updates, "parking alone is not a Yjs update event")
	base, records := log.Drain()
	require.NotNil(t, base)
	require.Len(t, records, 1)
	require.Same(t, origin, records[0].Origin)
	require.NotNil(t, records[0].Parked)
	require.Equal(t, 1, room.PendingStats().Items)

	require.NoError(t, crdt.ApplyUpdateV1(room, parked, origin))
	_, duplicate := log.Drain()
	require.Empty(t, duplicate, "re-sending parked state must not log it again")

	replay := crdt.New(crdt.WithClientID(4))
	require.NoError(t, crdt.ApplyUpdateV1(replay, base, nil))
	for _, record := range records {
		require.NoError(t, crdt.ApplyUpdateV1(replay, record.Update, nil))
		require.NoError(t, crdt.ApplyUpdateV1(replay, record.Parked, nil))
	}
	require.Equal(t, 1, replay.PendingStats().Items)
	require.NoError(t, crdt.ApplyUpdateV1(replay, parentUpdate, nil))
	v, ok := replay.GetMap("m").Get("nested")
	require.True(t, ok)
	value, ok := v.(*crdt.YMap).Get("key")
	require.True(t, ok)
	require.Equal(t, "value", value)
	require.Zero(t, replay.PendingStats().Items)

	log.Close()
	require.NoError(t, crdt.ApplyUpdateV1(room, parentUpdate, nil))
	_, closed := log.Drain()
	require.Empty(t, closed, "closing detaches the update log")
	require.Equal(t, 1, updates, "integrating the dependency still emits OnUpdate")
	require.Zero(t, room.PendingStats().Items)
}
