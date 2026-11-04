package crdt

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnit_Retryable_CloneKeepsAnEarlierWake(t *testing.T) {
	for _, c := range partlyAppliedCases(t) {
		t.Run(c.name, func(t *testing.T) {
			doc := New(WithClientID(13))
			defer doc.Destroy()
			text := doc.GetText("t")
			require.NoError(t, c.apply(doc, c.gap))
			doc.Transact(func(txn *Transaction) { text.Insert(txn, 0, "a", nil) })
			require.Equal(t, 1, doc.PendingStats().Items)
			require.True(t, doc.store.retryable())
			copy := doc.Clone(WithClientID(99))
			defer copy.Destroy()
			empty := []byte{0, 0}
			if c.v2 {
				var err error
				empty, err = UpdateV1ToV2(empty)
				require.NoError(t, err)
			}
			for _, d := range []*Doc{doc, copy} {
				require.NoError(t, c.apply(d, empty))
				require.Equal(t, "ab", d.GetText("t").ToString())
				require.Zero(t, d.PendingStats().Items)
			}
		})
	}
}

func TestUnit_Retryable_RollbackRestoresTheEarlierWake(t *testing.T) {
	for _, c := range partlyAppliedCases(t) {
		t.Run(c.name, func(t *testing.T) {
			doc := New(WithClientID(1), WithMaxIntegrateScan(1<<20))
			defer doc.Destroy()
			require.NoError(t, c.apply(doc, c.gap))
			require.False(t, doc.store.retryable())
			require.ErrorIs(t, c.apply(doc, c.bad), ErrInvalidUpdate)
			require.Zero(t, doc.StateVector().Clock(13), "the bounded refusal must roll back the advanced dependency")
			require.Equal(t, 1, doc.PendingStats().Items)
			require.False(t, doc.store.retryable(), "the rolled-back clock must not leave a wake")
		})
	}
}
