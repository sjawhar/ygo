package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A bounded document takes back an apply that fails after it brought the
// target of a move that waits for it (resolvePendingMoves pops the move from
// its heap), so the move waits again and moves the target once the target
// arrives whole.
func TestUnit_IntegrateScan_RolledBackApplyKeepsTheWaitingMove(t *testing.T) {
	doc := New(WithClientID(1))
	arr := doc.GetArray("a")
	target := ID{Client: 8, Clock: 0}
	mv := &Item{ID: ID{Client: 1}, Parent: &arr.abstractType, Content: NewContentMove(&target, 1)}
	doc.Transact(func(txn *Transaction) {
		ok, _, err := tryIntegrate(txn, mv)
		require.True(t, ok)
		require.NoError(t, err)
	})
	require.Len(t, doc.store.pendingMoves[8], 1)
	doc.SetMaxIntegrateScan(1000)

	w := New(WithClientID(8))
	wa := w.GetArray("a")
	w.Transact(func(txn *Transaction) { wa.Insert(txn, 0, []any{"x"}) })
	update := EncodeStateAsUpdateV1(w, nil)
	cut := update[:len(update)-1] // the delete set is cut off, after the target integrates

	require.Error(t, ApplyUpdateV1(doc, cut, nil))
	require.Len(t, doc.store.pendingMoves[8], 1, "the move waits again")
	require.Equal(t, 1, doc.store.pendingMoveCount)

	require.NoError(t, ApplyUpdateV1(doc, update, nil))
	got := doc.store.Find(target)
	require.NotNil(t, got)
	require.Same(t, mv, got.MovedBy, "the move moves its target once the target arrives")
	require.Empty(t, doc.store.pendingMoves[8])
}
