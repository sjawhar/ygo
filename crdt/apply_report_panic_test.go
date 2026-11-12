package crdt

import (
	"reflect"
	"testing"
)

// A report callback that panics must not skip the commit's formatting cleanup.
//
// The report fired at the end of the apply's own body in the first draft of
// this change, so a panicking callback reached commitLocked as a panicking
// transaction and the remote formatting cleanup was skipped: a later plain
// insert came out bold, which nothing surfaced. It fires inside the deferred
// commit now, after that cleanup, with the panic kept and re-raised after the
// unlock (reportApplyRecovered). The shape is the release owner's, who found
// the defect: the apply under the panicking callback must be one the cleanup
// would otherwise reconcile - B holds a live format and applies A's delete of
// the run it formats.
func TestUnit_ApplyReport_APanickingCallbackDoesNotSkipCommitCleanup(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "callback returns", true: "callback panics"}[panics], func(t *testing.T) {
			a, b := newTestDoc(1), newTestDoc(2)
			at, bt := a.GetText("t"), b.GetText("t")
			a.Transact(func(txn *Transaction) { at.Insert(txn, 0, "abc", nil) })
			syncText(t, a, b, nil)
			a.Transact(func(txn *Transaction) { at.Delete(txn, 0, 3) })
			b.Transact(func(txn *Transaction) { bt.Format(txn, 2, 1, Attributes{"bold": true}) })

			b.SetApplyReport(func(any, ApplyReport) {
				if panics {
					panic("report")
				}
			})
			got := func() (p any) {
				defer func() { p = recover() }()
				syncText(t, a, b, "remote")
				return nil
			}()
			if panics && got != "report" {
				t.Fatalf("a panicking callback did not propagate: got %v", got)
			}
			if !panics && got != nil {
				t.Fatalf("a returning callback panicked: %v", got)
			}
			if !b.mu.TryLock() {
				t.Fatal("the document lock was held after the report")
			}
			b.mu.Unlock()

			b.SetApplyReport(nil)
			a.Transact(func(txn *Transaction) { at.Insert(txn, 0, "x", nil) })
			syncText(t, a, b, nil)
			want := []Delta{{Op: DeltaOpInsert, Insert: "x"}}
			if !reflect.DeepEqual(bt.ToDelta(), want) {
				t.Fatalf("a callback panic corrupted later formatting: delta=%+v want=%+v", bt.ToDelta(), want)
			}
		})
	}
}
