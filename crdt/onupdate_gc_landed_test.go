package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A failed apply's landed bytes are encoded at commit, after the GC and the
// end-of-transaction merges, as OnUpdate's are, so the two are the same bytes:
// with GC on, the map entry the apply both integrated and overwrote goes out
// as a tombstone in both.
func TestUnit_ApplyUpdateV1Landed_EncodedAfterGC(t *testing.T) {
	for _, gc := range []bool{true, false} {
		t.Run(map[bool]string{true: "GC on", false: "GC off"}[gc], func(t *testing.T) { landedEqualsOnUpdate(t, gc) })
	}
}

func landedEqualsOnUpdate(t *testing.T, gc bool) {
	editor := New(WithClientID(7), WithGC(false))
	ek := editor.GetMap("k")
	editor.Transact(func(txn *Transaction) { ek.Set(txn, "a", "first") })
	editor.Transact(func(txn *Transaction) { ek.Set(txn, "a", "second") })
	// The pending cap refuses before any change, so the apply fails partway on
	// a delete set cut short, after both entries integrate.
	update := EncodeStateAsUpdateV1(editor, nil)
	cut := update[:len(update)-1]

	room := New(WithClientID(100), WithGC(gc))
	room.GetMap("k")
	var published [][]byte
	room.OnUpdate(func(u []byte, _ any) { published = append(published, u) })
	landed, err := ApplyUpdateV1Landed(room, cut, nil)
	require.ErrorIs(t, err, ErrInvalidUpdate)
	v, _ := room.GetMap("k").Get("a")
	require.Equal(t, "second", v, "the overwrite integrated")
	require.Len(t, published, 1)
	require.Equal(t, published[0], landed)
}

// TestUnit_ApplyUpdateV1Landed_SuccessAllocatesNoMoreThanApply checks that a
// successful ApplyUpdateV1Landed allocates no more than ApplyUpdateV1: only a
// failed apply needs a place for its landed bytes.
func TestUnit_ApplyUpdateV1Landed_SuccessAllocatesNoMoreThanApply(t *testing.T) {
	author := newTestDoc(1)
	txt := author.GetText("t")
	author.Transact(func(txn *Transaction) { txt.Insert(txn, 0, "abc", nil) })
	update := EncodeStateAsUpdateV1(author, nil)
	// The fewest over a few measurements: a GC cycle or another goroutine
	// during one measurement only adds allocations to it.
	fewest := func(f func()) float64 {
		n := testing.AllocsPerRun(50, f)
		for range 4 {
			n = min(n, testing.AllocsPerRun(50, f))
		}
		return n
	}
	apply := fewest(func() {
		require.NoError(t, ApplyUpdateV1(newTestDoc(2), update, nil))
	})
	landed := fewest(func() {
		_, err := ApplyUpdateV1Landed(newTestDoc(2), update, nil)
		require.NoError(t, err)
	})
	require.LessOrEqual(t, landed, apply)
}
