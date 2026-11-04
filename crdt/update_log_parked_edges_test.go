package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// craftedUpdate encodes items, which no document holds, as one V1 update.
func craftedUpdate(items ...*Item) []byte {
	perClient := map[ClientID][]*Item{}
	for _, it := range items {
		perClient[it.ID.Client] = append(perClient[it.ID.Client], it)
	}
	return encodeStructStoreV1(perClient, newDeleteSet(), nil)
}

// requireSameRoom requires replica to hold what room holds: state, text,
// parked structs and parked deletions.
func requireSameRoom(t *testing.T, room, replica *Doc, msg ...any) {
	t.Helper()
	require.Equal(t, EncodeStateAsUpdateV1(room, nil), EncodeStateAsUpdateV1(replica, nil), msg...)
	require.Equal(t, room.GetText("t").ToString(), replica.GetText("t").ToString(), msg...)
	require.Equal(t, room.StateVector(), replica.StateVector(), msg...)
	require.Equal(t, room.PendingStats(), replica.PendingStats(), msg...)
}

// A parked struct that starts below clocks the room holds (client 2's 3..8
// when the room holds 0..5, with an origin the room lacks) is logged whole,
// so the replica parks it as the room does, rather than its suffix, which the
// replica would integrate after the room's own clock 4.
func TestUnit_UpdateLog_AParkedStructOverlappingHeldClocksIsLoggedWhole(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		src := New(WithClientID(2))
		src.Transact(func(txn *Transaction) { txn.GetText("t").Insert(txn, 0, "abcde", nil) })
		room := New(WithClientID(1))
		log := room.OpenUpdateLog()
		applyAs(t, room, v2, EncodeStateAsUpdateV1(src, nil))
		over := &Item{ID: ID{Client: 2, Clock: 3}, Origin: &ID{Client: 3, Clock: 9}, Content: NewContentString("XXXXX")}
		applyAs(t, room, v2, craftedUpdate(over))
		require.Equal(t, 1, room.PendingStats().Items, "v2=%v", v2)

		base, updates := log.Drain()
		requireSameRoom(t, room, replay(t, base, updates), "v2=%v", v2)
	}
}

// A parked struct whose origin is a GC struct, parked in the same update
// (client 2's 5..7) or held by the room, keeps its origins on the wire: the
// log and the base both encode it so a reader parks it and later turns it into
// a GC struct, as the room does, instead of integrating its content into a
// root type named "" or at another position.
func TestUnit_UpdateLog_AParkedStructWithAGCOriginKeepsItsOrigins(t *testing.T) {
	t.Run("origin inside a GC struct parked with it", func(t *testing.T) {
		room := New(WithClientID(1))
		room.GetText("t")
		log := room.OpenUpdateLog()
		gc := &Item{ID: ID{Client: 2, Clock: 5}, Content: NewContentDeleted(2), Deleted: true}
		hi := &Item{ID: ID{Client: 2, Clock: 7}, Origin: &ID{Client: 2, Clock: 6}, Content: NewContentString("hi")}
		require.NoError(t, ApplyUpdateV1(room, craftedUpdate(gc, hi), nil))
		require.Equal(t, 2, room.PendingStats().Items)
		base, updates := log.Drain()
		replica := replay(t, base, updates)
		copied := New(WithClientID(9))
		require.NoError(t, ApplyUpdateV1(copied, EncodeStateAsUpdateV1(room, nil), nil))

		filler := craftedUpdate(&Item{ID: ID{Client: 2, Clock: 0}, Content: NewContentDeleted(5), Deleted: true})
		for _, d := range []*Doc{room, replica, copied} {
			require.NoError(t, ApplyUpdateV1(d, filler, nil))
		}
		requireSameRoom(t, room, replica, "replayed from the log")
		requireSameRoom(t, room, copied, "loaded from the room's state")
	})
	t.Run("origin on a GC struct the room holds", func(t *testing.T) {
		room := New(WithClientID(1))
		room.GetText("t")
		require.NoError(t, ApplyUpdateV1(room, craftedUpdate(&Item{ID: ID{Client: 2, Clock: 0}, Content: NewContentDeleted(2), Deleted: true}), nil))
		log := room.OpenUpdateLog()
		z := &Item{ID: ID{Client: 4, Clock: 0}, Origin: &ID{Client: 2, Clock: 1}, OriginRight: &ID{Client: 3, Clock: 0}, Content: NewContentString("Z")}
		require.NoError(t, ApplyUpdateV1(room, craftedUpdate(z), nil))
		base, updates := log.Drain()
		replica := replay(t, base, updates)

		third := New(WithClientID(3))
		third.Transact(func(txn *Transaction) { txn.GetText("t").Insert(txn, 0, "Y", nil) })
		y := EncodeStateAsUpdateV1(third, nil)
		require.NoError(t, ApplyUpdateV1(room, y, nil))
		require.NoError(t, ApplyUpdateV1(replica, y, nil))
		requireSameRoom(t, room, replica, "after 3:0 arrives")
	})
}

func TestUnit_UpdateLog_TruncatedDeleteSetKeepsAnUnboundedPartialApply(t *testing.T) {
	src := New(WithClientID(2))
	text := src.GetText("t")
	src.Transact(func(txn *Transaction) { text.Insert(txn, 0, "hello", nil) })
	whole := EncodeStateAsUpdateV1(src, nil)
	cut := append(append([]byte(nil), whole[:len(whole)-1]...), 1, 42)
	for _, bounded := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbounded", true: "bounded"}[bounded], func(t *testing.T) {
			opts := []DocOption{WithClientID(1)}
			if bounded {
				opts = append(opts, WithMaxIntegrateScan(100))
			}
			room := New(opts...)
			log := room.OpenUpdateLog()
			landed, err := ApplyUpdateV1Landed(room, cut, nil)
			require.Error(t, err)
			_, entries := log.Drain()
			if bounded {
				require.Empty(t, room.GetText("t").ToString())
				require.Empty(t, landed)
				require.Empty(t, entries)
			} else {
				require.Equal(t, "hello", room.GetText("t").ToString())
				require.NotEmpty(t, landed)
				require.Len(t, entries, 1)
			}
			t.Logf("bounded=%v text=%q landed=%d log_entries=%d error=%v", bounded, room.GetText("t").ToString(), len(landed), len(entries), err)
		})
	}
}

func TestUnit_UpdateLog_AParkedThenDrainedDeletionIsInBothPayloads(t *testing.T) {
	room := New(WithClientID(1))
	parked := &Item{ID: ID{Client: 2}, Origin: &ID{Client: 3}, Content: NewContentString("b")}
	require.NoError(t, ApplyUpdateV1(room, craftedUpdate(parked), nil))
	log := room.OpenUpdateLog()
	src := New(WithClientID(3))
	src.Transact(func(txn *Transaction) { txn.GetText("t").Insert(txn, 0, "a", nil) })
	input, err := MergeUpdatesV1(EncodeStateAsUpdateV1(src, nil), deleteSetUpdate(map[ClientID][]DeleteRange{2: {{Clock: 0, Len: 1}}}))
	require.NoError(t, err)
	require.NoError(t, ApplyUpdateV1(room, input, nil))
	base, entries := log.Drain()
	require.Len(t, entries, 1)
	for _, payload := range [][]byte{entries[0].Update, entries[0].Parked} {
		ids, err := ContentIDsFromUpdateV1(payload)
		require.NoError(t, err)
		require.Equal(t, []IDRange{{Clock: 0, Len: 1}}, ids.Deletes.Ranges(2))
	}
	rebuilt := replay(t, base, entries)
	requireSameRoom(t, room, rebuilt)
	require.Equal(t, "a", room.GetText("t").ToString())
	t.Logf("deletion 2@0+1 appears in Update and Parked; replay text=%q", rebuilt.GetText("t").ToString())
}
