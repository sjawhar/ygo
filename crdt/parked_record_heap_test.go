//go:build go1.24

package crdt

import (
	"runtime"
	"testing"
	"unsafe"
	"weak"
)

// The test in this file needs the weak package, which Go 1.24 added; the
// module still builds with Go 1.23, which leaves it out.

// A string a merge drops from the pending queue is freed once the frame that
// dropped it is applied: the record of who parked each struct lets it go as it
// leaves the queue. It used to keep every one, up to 4,096 strings of about
// 900 KiB in a room, while the queue's figures counted none of them.
func TestUnit_DiscardRefusedDrain_AStringAMergeDroppedIsFreed(t *testing.T) {
	doc := New(WithClientID(1))
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: -1}
	})
	doc.SetDiscardRefusedDrain(true)
	var dropped []weak.Pointer[byte]
	coverParkedStrings(t, doc, 16, 256<<10, func(it *Item) {
		dropped = append(dropped, weak.Make(unsafe.StringData(it.Content.(*ContentString).Str)))
	}, func(string) {})
	runtime.GC()
	for i, w := range dropped {
		if w.Value() != nil {
			t.Fatalf("pass %d's string, which the merge dropped, is still reachable after a collection", i)
		}
	}
	runtime.KeepAlive(doc)
}
