package crdt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// B holds a marker that A's deletion leaves redundant, as in
// TestUnit_YText_FormatCleanup_IsLocalFollowUpTxn, where the default cleans it
// up. With the cleanup off, the remote update is B's only transaction and
// update, and the marker stays.
func TestUnit_YText_WithoutFormatCleanup_RemoteApplyMakesNoCommit(t *testing.T) {
	a := newTestDoc(1)
	b := New(WithClientID(2), WithFormatCleanup(false))
	at, bt := a.GetText("t"), b.GetText("t")
	a.Transact(func(txn *Transaction) { at.Insert(txn, 0, "abc", nil) })
	syncText(t, a, b, nil)
	a.Transact(func(txn *Transaction) { at.Delete(txn, 0, 3) })
	b.Transact(func(txn *Transaction) { bt.Format(txn, 2, 1, Attributes{"bold": true}) })

	var origins []any
	var updates int
	b.OnAfterTransaction(func(txn *Transaction) { origins = append(origins, txn.Origin) })
	b.OnUpdate(func([]byte, any) { updates++ })
	syncText(t, a, b, "remote")
	assert.Equal(t, []any{"remote"}, origins, "no cleanup transaction")
	assert.Equal(t, 1, updates, "no cleanup update")
	assert.Equal(t, 2, countLiveContentFormat(b), "the redundant marker stays")
}

// A replica that applies each update of a document's own local transactions
// commits only those transactions with the cleanup off: it makes no commit
// the document did not make, and holds the document's state. With the cleanup
// on it also commits cleanup transactions of its own after the remote
// updates that change formatted text. Its state matches the document's either
// way, since the document, as Yjs does, cleans up the gap its own delete
// leaves (#350's group); before that, the replica with the cleanup on deleted
// a marker the document kept. A type observer on the replica makes the
// cleanup decision wait for it, which the option skips as well.
func TestUnit_YText_WithoutFormatCleanup_ReplicaReplaysLocalCommits(t *testing.T) {
	for _, tc := range []struct{ cleanup, observed bool }{
		{cleanup: true},
		{cleanup: true, observed: true},
		{cleanup: false},
		{cleanup: false, observed: true},
	} {
		t.Run(fmt.Sprintf("cleanup=%t/observed=%t", tc.cleanup, tc.observed), func(t *testing.T) {
			source := newTestDoc(1)
			replica := New(WithClientID(2), WithFormatCleanup(tc.cleanup))
			// A text nobody accessed is a raw root, which Yjs never cleans up.
			replicaText := replica.GetText("t")
			if tc.observed {
				replicaText.Observe(func(YTextEvent) {})
			}
			var origins []any
			replica.OnAfterTransaction(func(txn *Transaction) { origins = append(origins, txn.Origin) })
			source.OnUpdate(func(update []byte, _ any) {
				require.NoError(t, ApplyUpdateV1(replica, update, "replay"))
			})
			text := source.GetText("t")
			italicLeavingMarkers(source, text)
			source.Transact(func(txn *Transaction) { text.Delete(txn, 1, 1) })

			require.Equal(t, 2, countLiveContentFormat(source), "the source's delete cleans up its gap")
			assert.Equal(t, countLiveContentFormat(source), countLiveContentFormat(replica))
			assert.Equal(t, text.ToDelta(), replicaText.ToDelta())
			if tc.cleanup {
				assert.Contains(t, origins, nil, "the replica's own cleanup commits")
				return
			}
			assert.Equal(t, []any{"replay", "replay", "replay", "replay"}, origins, "no cleanup transaction")
			assert.Equal(t, EncodeStateAsUpdateV1(source, nil), EncodeStateAsUpdateV1(replica, nil))
		})
	}
}

// italicLeavingMarkers writes, in local transactions, text whose deletion of
// its second character leaves a format marker a remote cleanup removes.
func italicLeavingMarkers(doc *Doc, text *YText) {
	italic := Attributes{"italic": true}
	doc.Transact(func(txn *Transaction) { text.Insert(txn, 0, "x", nil) })
	doc.Transact(func(txn *Transaction) { text.Insert(txn, 0, "x", italic) })
	doc.Transact(func(txn *Transaction) { text.Insert(txn, 2, "x", italic) })
}

// SetFormatCleanup switches the cleanup after construction: a document loads
// stored state without it, then cleans up after the next remote change.
func TestUnit_YText_SetFormatCleanup(t *testing.T) {
	a := newTestDoc(1)
	at := a.GetText("t")
	italicLeavingMarkers(a, at)

	b := New(WithClientID(2), WithFormatCleanup(false))
	b.GetText("t")
	var origins []any
	b.OnAfterTransaction(func(txn *Transaction) { origins = append(origins, txn.Origin) })
	syncText(t, a, b, "load")
	require.Equal(t, []any{"load"}, origins)

	b.SetFormatCleanup(true)
	a.Transact(func(txn *Transaction) { at.Delete(txn, 1, 1) })
	syncText(t, a, b, "remote")
	assert.Equal(t, []any{"load", "remote", nil}, origins, "the next remote change cleans up")
	assert.Equal(t, countLiveContentFormat(a), countLiveContentFormat(b))
	assert.Equal(t, at.ToDelta(), b.GetText("t").ToDelta())
}
