package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/encoding"
)

// deletesOnly is a V1 update with no structs and the deletions ranges names.
func deletesOnly(ranges map[ClientID][]DeleteRange) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(0) // no structs
	enc.WriteVarUint(uint64(len(ranges)))
	for client, rs := range ranges {
		enc.WriteVarUint(uint64(client))
		enc.WriteVarUint(uint64(len(rs)))
		for _, r := range rs {
			enc.WriteVarUint(r.Clock)
			enc.WriteVarUint(r.Len)
		}
	}
	return append([]byte(nil), enc.Bytes()...)
}

// requireCovers requires the structs and deletions of update to be in
// covering, clock for clock: no byte of update is in neither.
func requireCovers(t *testing.T, update, covering []byte) {
	t.Helper()
	want, wantDs, err := buildMergeStore([][]byte{update}, decodeStructsV1)
	require.NoError(t, err)
	got, gotDs, err := buildMergeStore([][]byte{covering}, decodeStructsV1)
	require.NoError(t, err)
	holds := func(client ClientID, clock uint64) bool {
		for _, it := range got[client] {
			if clock >= it.ID.Clock && clock < it.ID.Clock+it.Content.Len() {
				return true
			}
		}
		return false
	}
	for client, items := range want {
		for _, it := range items {
			for c := it.ID.Clock; c < it.ID.Clock+it.Content.Len(); c++ {
				require.True(t, holds(client, c), "client %d's clock %d is in neither the logged update nor its parked one", client, c)
			}
		}
	}
	for client, ranges := range wantDs.clients {
		for _, r := range ranges {
			for c := r.Clock; c < r.Clock+r.Len; c++ {
				require.True(t, gotDs.IsDeleted(ID{Client: client, Clock: c}), "the deletion of client %d's clock %d is in neither the logged update nor its parked one", client, c)
			}
		}
	}
}

// A bounded room's apply that integrates part of its update, parks the rest,
// and then has its drain taken back (drainParked) logs what the room
// integrated as the entry's Update and what it parked as its Parked, so the
// two together hold every byte the client sent, and a reload from the log
// alone holds everything the live room holds. The reload may be ahead of the
// room: it has no bound, so its drain integrates the queue the room's drain
// gave back, which the room integrates at its next apply that brings a client
// the queue waits on.
func TestUnit_UpdateLog_ATakenBackDrainLogsEveryByteTheApplyKept(t *testing.T) {
	const l = 2_000
	parked := [][]byte{
		itemAfter(99, 1, ID{Client: 5, Clock: 0}, "abc"), // behind client 99's clock 0
		itemAfter(98, 0, ID{Client: 99, Clock: 2}, "r"),  // the middle of client 99's item
	}
	lastOf := func(doc *Doc) *Item {
		last := doc.GetText("t").start
		for last.Right != nil {
			last = last.Right
		}
		return last
	}
	// The drain's allowance holds client 99's item and runs out in the scan
	// of client 98's, so the drain is taken back.
	probe := chainedList(t, l)
	require.NoError(t, ApplyUpdateV1(probe, parked[0], nil))
	limit := scannedBy(t, probe, fillTheGap(lastOf(probe)))

	room := chainedList(t, l, WithMaxIntegrateScan(limit))
	log := room.OpenUpdateLog()
	for _, p := range parked {
		require.NoError(t, ApplyUpdateV1(room, p, nil))
	}
	// The client's update: client 99's first character, which integrates; a
	// struct waiting on client 77, which parks; a deletion of the list's first
	// character, which applies; and one of client 77's, which parks.
	input, err := MergeUpdatesV1(
		fillTheGap(lastOf(room)),
		itemAfter(76, 0, ID{Client: 77, Clock: 0}, "z"),
		deletesOnly(map[ClientID][]DeleteRange{5: {{Clock: 0, Len: 1}}, 77: {{Clock: 0, Len: 1}}}),
	)
	require.NoError(t, err)
	before := room.PendingStats()
	require.Equal(t, limit, scannedBy(t, room, input), "the drain was not refused at the bound")
	require.Equal(t, uint64(1), room.StateVector().Clock(99), "client 99's clock: the apply kept its own struct and the drain was taken back")
	require.Equal(t, before.Items+1, room.PendingStats().Items, "the queue: as the drain found it, and the struct the apply parked")
	require.Equal(t, 1, room.PendingStats().DeleteRanges, "the deletion the apply parked")

	base, updates := log.Drain()
	require.Len(t, updates, len(parked)+1)
	kept := updates[len(updates)-1]
	require.NotNil(t, kept.Update)
	require.NotNil(t, kept.Parked, "the apply parked a struct and a deletion")

	// No byte of the client's update is in neither Update nor Parked.
	both, err := MergeUpdatesV1(kept.Update, kept.Parked)
	require.NoError(t, err)
	requireCovers(t, input, both)

	// Replayed on the log's state before the apply, the entry does what the
	// client's update does.
	sent, logged := replay(t, base, updates[:len(updates)-1]), replay(t, base, updates[:len(updates)-1])
	require.NoError(t, ApplyUpdateV1(sent, input, nil))
	require.NoError(t, ApplyUpdateV1(logged, kept.Update, nil))
	require.NoError(t, ApplyUpdateV1(logged, kept.Parked, nil))
	requireSameRoom(t, sent, logged, "the logged entry against the client's update")

	// A reload from the log alone holds everything the live room holds: the
	// room's whole state, parked structs and deletions included, adds nothing
	// to it.
	reload := replay(t, base, updates)
	held := EncodeStateAsUpdateV1(reload, nil)
	sv, stats := reload.StateVector(), reload.PendingStats()
	require.NoError(t, ApplyUpdateV1(reload, EncodeStateAsUpdateV1(room, nil), nil))
	require.Equal(t, sv, reload.StateVector(), "the room holds structs the reload lacks")
	require.Equal(t, stats, reload.PendingStats(), "the room holds parked state the reload lacks")
	require.Equal(t, held, EncodeStateAsUpdateV1(reload, nil), "the room holds what the reload lacks")
	for client, clock := range room.StateVector() {
		require.GreaterOrEqual(t, sv.Clock(client), clock, "client %d", client)
	}
	// The reload is ahead: its drain integrated the queue the room gave back.
	require.Equal(t, uint64(4), sv.Clock(99))
	require.Equal(t, uint64(1), sv.Clock(98))

	// The room catches up at its next apply that brings a client the queue
	// waits on, with a bound that holds the drain, and then reads as the reload.
	room.SetMaxIntegrateScan(testScanBound)
	more := itemAfter(77, 0, ID{Client: 5, Clock: 1}, "w")
	require.NoError(t, ApplyUpdateV1(room, more, nil))
	require.NoError(t, ApplyUpdateV1(reload, more, nil))
	requireSameRoom(t, room, reload, "the room after its next apply")
}
