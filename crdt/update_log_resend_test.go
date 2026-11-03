package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A resend of a struct the pending queue holds parks nothing and changes
// nothing, so the commit is not logged, as OnUpdate is not called for it: a
// log reader holds the parked tail once, as the room does.
func TestUnit_UpdateLog_AResentParkedStructLogsNoParked(t *testing.T) {
	const n = 5
	keystrokes, tail, ab, _ := parkedTail(t, n)
	room := New(WithClientID(1), WithMaxPendingItems(n))
	log := room.OpenUpdateLog()
	for _, k := range keystrokes {
		require.NoError(t, ApplyUpdateV1(room, k, nil))
	}
	base, updates := log.Drain()
	for _, u := range updates {
		require.NotNil(t, u.Parked, "each keystroke parks")
	}
	for range 3 {
		require.NoError(t, ApplyUpdateV1(room, tail, nil))
	}
	_, resent := log.Drain()
	require.Empty(t, resent, "a resend that changes and parks nothing is not logged")
	replica := replay(t, base, updates)
	require.Equal(t, room.PendingStats(), replica.PendingStats())
	require.NoError(t, ApplyUpdateV1(room, ab, nil))
	require.NoError(t, ApplyUpdateV1(replica, ab, nil))
	require.Equal(t, room.GetText("t").ToString(), replica.GetText("t").ToString())
	require.Zero(t, replica.PendingStats().Items)
}
