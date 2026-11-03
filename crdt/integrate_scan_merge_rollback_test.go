package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A bounded document refuses an update that fails partway, and leaves the
// pending queue as it found it, also when the update's park merged into the
// queue (#340), which empties, compacts, cuts and grows the queue's slots and
// structs in place.
func TestUnit_IntegrateScan_ARefusedUpdateLeavesTheQueueItsMergeChanged(t *testing.T) {
	type sent struct {
		clock uint64
		s     string
	}
	changed := 0
	for _, first := range []sent{{1, "abc"}, {2, "abc"}, {1, "abcdef"}} {
		for _, again := range []sent{{1, "xy"}, {1, "xyz"}, {1, "xyzw"}, {2, "xy"}, {0, "xyzw"}, {1, "abcdef"}, {3, "x"}} {
			a := itemAfter(99, first.clock, ID{Client: 5, Clock: 0}, first.s)
			b := itemAfter(99, again.clock, ID{Client: 5, Clock: 0}, again.s)
			cut := b[:len(b)-1] // the delete set is cut off, after the struct parks

			// The merge must change a queued struct for the case to test it.
			free := New(WithClientID(1))
			free.GetText("t")
			require.NoError(t, ApplyUpdateV1(free, a, nil))
			q := free.store.pending.items[0]
			id, n := q.ID, q.Content.Len()
			_ = ApplyUpdateV1(free, cut, nil)
			if q.ID != id || q.Content.Len() != n || free.store.pending.items[0] != q {
				changed++
			}

			room := New(WithClientID(1), WithMaxIntegrateScan(1<<20))
			room.GetText("t")
			require.NoError(t, ApplyUpdateV1(room, a, nil))
			before, stats := EncodeStateAsUpdateV1(room, nil), room.PendingStats()
			require.Error(t, ApplyUpdateV1(room, cut, nil), "first %v, again %v", first, again)
			require.Equal(t, before, EncodeStateAsUpdateV1(room, nil), "first %v, again %v: the refused update changed the queue", first, again)
			require.Equal(t, stats, room.PendingStats(), "first %v, again %v", first, again)

			// The queue still merges the same struct sent again whole.
			require.NoError(t, ApplyUpdateV1(room, b, nil))
			require.NoError(t, ApplyUpdateV1(free, b, nil))
		}
	}
	require.NotZero(t, changed, "no case's merge changed a queued struct, so nothing here is tested")

	// Three queued structs, of which the update sent again empties one slot,
	// too few for the queue to compact: the refused update leaves the queue
	// with no emptied slot, as it found it.
	room := New(WithClientID(1), WithMaxIntegrateScan(1<<20))
	room.GetText("t")
	for _, clock := range []uint64{1, 10, 20} {
		require.NoError(t, ApplyUpdateV1(room, itemAfter(99, clock, ID{Client: 5, Clock: 0}, "abc"), nil))
	}
	before, stats := EncodeStateAsUpdateV1(room, nil), room.PendingStats()
	require.Equal(t, 3, stats.Items)
	b := itemAfter(99, 0, ID{Client: 5, Clock: 0}, "xyzw")
	free := New(WithClientID(1))
	free.GetText("t")
	for _, clock := range []uint64{1, 10, 20} {
		require.NoError(t, ApplyUpdateV1(free, itemAfter(99, clock, ID{Client: 5, Clock: 0}, "abc"), nil))
	}
	_ = ApplyUpdateV1(free, b[:len(b)-1], nil)
	require.Equal(t, 1, free.store.pending.dead, "the update sent again emptied no slot, so nothing here is tested")
	require.Error(t, ApplyUpdateV1(room, b[:len(b)-1], nil))
	require.Equal(t, before, EncodeStateAsUpdateV1(room, nil))
	require.Equal(t, stats, room.PendingStats())
	require.Zero(t, room.store.pending.dead)
}

// A drain taken back parks the queue it found again (parkAgain), whose slots
// a merge emptied (#340) are left out, not touched.
func TestUnit_IntegrateScan_ParkAgainSkipsAnEmptiedSlot(t *testing.T) {
	doc := New(WithClientID(1))
	item := &Item{ID: ID{Client: 99, Clock: 4}, Content: NewContentDeleted(1)}
	found := &pendingUpdate{items: []*Item{nil, item}, dead: 1, missing: StateVector{99: 0}}
	again := parkAgain(doc.store, found)
	require.Equal(t, []*Item{item}, again.items)
	require.Equal(t, 1, again.heldStructs())
}
