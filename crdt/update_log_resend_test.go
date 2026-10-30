package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A resend of a struct the pending queue holds parks nothing, so the commit
// logs no Parked: a log reader holds the parked tail once, as the room does.
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
	require.Len(t, resent, 3)
	for i, u := range resent {
		require.Nil(t, u.Parked, "resend %d logged parked structs", i+1)
	}
	replica := replay(t, base, append(updates, resent...))
	require.Equal(t, room.PendingStats(), replica.PendingStats())
	require.NoError(t, ApplyUpdateV1(room, ab, nil))
	require.NoError(t, ApplyUpdateV1(replica, ab, nil))
	require.Equal(t, room.GetText("t").ToString(), replica.GetText("t").ToString())
	require.Zero(t, replica.PendingStats().Items)
}
