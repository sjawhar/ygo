package crdt

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/encoding"
)

// A document with a scan bound takes back an apply that fails
// (apply_rollback.go), which cuts the pending queue's items back in place.
// The room already parks client 2's first keystroke, and a resend of it has
// built the queue's cover. An update then parks client 2's next two
// keystrokes and fails on its delete set. The same keystrokes, sent again
// whole, must park: the queue does not hold them.
func TestUnit_PendingQueue_ARolledBackParkIsNotHeld(t *testing.T) {
	keystrokes, _, ab, writer := parkedTail(t, 3)
	rest, err := MergeUpdatesV1(keystrokes[1], keystrokes[2])
	require.NoError(t, err)
	broken := append([]byte{}, rest[:len(rest)-1]...) // delete set cut off
	room := New(WithClientID(1), WithMaxIntegrateScan(1000))
	require.NoError(t, ApplyUpdateV1(room, keystrokes[0], nil))
	require.NoError(t, ApplyUpdateV1(room, keystrokes[0], nil)) // the resend builds the cover
	require.Equal(t, 1, room.PendingStats().Items)

	require.Error(t, ApplyUpdateV1(room, broken, nil), "the update fails after it parks")
	require.Equal(t, 1, room.PendingStats().Items, "the take-back drops what the update parked")

	require.NoError(t, ApplyUpdateV1(room, rest, nil))
	require.Equal(t, 3, room.PendingStats().Items, "the resent keystrokes park")
	require.NoError(t, ApplyUpdateV1(room, ab, nil))
	require.Equal(t, writer.GetText("t").ToString(), room.GetText("t").ToString())
	require.Zero(t, room.PendingStats().Items)
}

// structsRun is a V1 update holding one group of client from start: for each
// length in lens, a struct of that length (a GC struct, or a deleted item of
// root "t" when items), each after a skip of gap clocks but the first.
func structsRun(client ClientID, start uint64, lens []int, gap int, items bool) []byte {
	e := encoding.NewEncoder()
	e.WriteVarUint(1)
	n := len(lens)
	if gap > 0 {
		n = 2*len(lens) - 1
	}
	e.WriteVarUint(uint64(n))
	e.WriteVarUint(uint64(client))
	e.WriteVarUint(start)
	for i, l := range lens {
		if i > 0 && gap > 0 {
			e.WriteUint8(10)
			e.WriteVarUint(uint64(gap))
		}
		if items {
			e.WriteUint8(1) // ContentDeleted item, explicit parent
			e.WriteUint8(1) // named root
			e.WriteVarString("t")
		} else {
			e.WriteUint8(0)
		}
		e.WriteVarUint(uint64(l))
	}
	e.WriteVarUint(0)
	return e.Bytes()
}

// A bounded document parks random runs of structs of three clients, in and
// out of clock order, overlapping what it parks, as GC structs and as items,
// a few per update and many; a third of the updates fail on their delete set
// and are taken back. After every apply the queue's cover is what its items
// span.
func TestUnit_PendingQueue_ARolledBackApplyLeavesTheCoverItsItems(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	checks, rollbacks := 0, 0
	for round := range 300 {
		doc := New(WithClientID(1), WithMaxIntegrateScan(1000), WithMaxPendingItems(1<<20))
		for range 40 {
			client := ClientID(5 + rng.IntN(3))
			k := 1 + rng.IntN(4)
			if rng.IntN(4) == 0 {
				k = 9 + rng.IntN(30)
			}
			lens := make([]int, k)
			for i := range lens {
				lens[i] = 1 + rng.IntN(3)
			}
			u := structsRun(client, 100+uint64(rng.IntN(400)), lens, rng.IntN(3), rng.IntN(2) == 0)
			broken := rng.IntN(3) == 0
			if broken {
				u = u[:len(u)-1]
			}
			err := ApplyUpdateV1(doc, u, nil)
			if broken && err != nil {
				rollbacks++
			}
			p := doc.store.pending
			if p == nil || p.cover == nil {
				// A park asks for the cover; make one so it exists and is checked.
				_ = ApplyUpdateV1(doc, structsRun(client, 100000+uint64(round), []int{1}, 0, false), nil)
				continue
			}
			checks++
			require.True(t, spansEqual(p.cover.spans, sortedSpans(p.items, false)), "round %d: cover %v, items span %v", round, p.cover.spans, sortedSpans(p.items, false))
			require.True(t, spansEqual(p.cover.gc, sortedSpans(p.items, true)), "round %d: GC cover %v, GC items span %v", round, p.cover.gc, sortedSpans(p.items, true))
		}
	}
	require.NotZero(t, rollbacks)
	require.NotZero(t, checks)
}

// spansEqual reports whether a and b hold the same spans for every client,
// a client with no spans equal to one absent.
func spansEqual(a, b map[ClientID][]clockSpan) bool {
	for c, l := range a {
		if len(l) > 0 && !slices.Equal(l, b[c]) {
			return false
		}
	}
	for c, l := range b {
		if len(l) > 0 && !slices.Equal(l, a[c]) {
			return false
		}
	}
	return true
}
