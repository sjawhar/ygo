package crdt

import (
	"fmt"
	"testing"
)

// A bounded document's drain that applies a parked delete range and then
// passes its allowance is taken back, and the delete range is parked again as
// the drain found it: the pending deletes, the count of their ranges and the
// clients whose ranges the next retry tries. The retry takes a client's ranges
// out of the pending deletes, and the journal keeps those by reference, so the
// take goes to a copy of them. The encoded state, parked deletes included,
// then reads as an unbounded twin's, and once an apply drains the queue the
// deletion applies and the document equals the twin.
func TestUnit_IntegrateScan_TakenBackDrainKeepsTheParkedDeletes(t *testing.T) {
	const l = 2_000
	parked := [][]byte{
		itemAfter(99, 1, ID{Client: 5, Clock: 0}, "abc"), // behind client 99's clock 0
		itemAfter(98, 0, ID{Client: 99, Clock: 2}, "r"),  // the middle of client 99's item
	}
	// The deletion of client 99's clock 0, parked until the struct arrives.
	deletes := deleteRangesUpdate(99, DeleteRange{Clock: 0, Len: 1})
	lastOf := func(doc *Doc) *Item {
		last := doc.GetText("t").start
		for last.Right != nil {
			last = last.Right
		}
		return last
	}
	park := func(doc *Doc, updates ...[]byte) {
		t.Helper()
		for _, update := range updates {
			if err := ApplyUpdateV1(doc, update, "park"); err != nil {
				t.Fatal(err)
			}
		}
		if got := doc.PendingStats().DeleteRanges; got != 1 {
			t.Fatalf("%d delete ranges are parked, want 1", got)
		}
	}
	// What the update filling client 99's gap counts, on an unbounded
	// document, with the deletion and updates parked before it.
	filling := func(updates ...[]byte) int {
		doc := chainedList(t, l)
		park(doc, append([][]byte{deletes}, updates...)...)
		return scannedBy(t, doc, fillTheGap(lastOf(doc)))
	}
	// The drain's allowance holds client 99's item and runs out in the scan
	// of client 98's, after the drain's retry applied the parked deletion.
	limit := filling(parked[0])
	if both := filling(parked...); both <= limit {
		t.Fatalf("the drain of both items passed %d items, want more than the %d of client 99's alone", both, limit)
	}

	doc, twin := chainedList(t, l, WithMaxIntegrateScan(limit)), chainedList(t, l)
	park(doc, append([][]byte{deletes}, parked...)...)
	park(twin, append([][]byte{deletes}, parked...)...)
	stats := fmt.Sprintf("%+v", doc.PendingStats())
	fill := fillTheGap(lastOf(doc))
	if got := scannedBy(t, doc, fill); got != limit {
		t.Fatalf("the filling apply counted %d items, want its drain refused at %d", got, limit)
	}
	if err := ApplyUpdateV1(twin, fill, nil); err != nil {
		t.Fatal(err)
	}
	if got := doc.StateVector().Clock(99); got != 1 {
		t.Fatalf("client 99's clock is %d, want 1: the drain taken back", got)
	}
	if got := fmt.Sprintf("%+v", doc.PendingStats()); got != stats {
		t.Fatalf("after the drain was taken back the pending queue and deletes are %s, want %s as they were", got, stats)
	}
	copied := New()
	if err := ApplyUpdateV1(copied, EncodeStateAsUpdateV1(doc, nil), nil); err != nil {
		t.Fatal(err)
	}
	if a, b := copied.GetText("t").ToString(), twin.GetText("t").ToString(); a != b {
		t.Fatalf("the document's state, parked deletes included, reads differently from the twin's: ends %q, the twin's %q", a[max(0, len(a)-12):], b[max(0, len(b)-12):])
	}

	// An apply that brings no client the queue waits on retries the parked
	// deletion, which applies now that client 99's clock 0 is in: the retry
	// tries the clients the drain found moved.
	unrelated := itemAfter(50, 0, ID{Client: 99, Clock: 0}, "u")
	for _, d := range []*Doc{doc, twin} {
		if err := ApplyUpdateV1(d, unrelated, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := doc.PendingStats().DeleteRanges; got != 0 {
		t.Fatalf("after an apply, %d delete ranges are parked, want the deletion of client 99's clock 0 applied", got)
	}
	if item := doc.store.Find(ID{Client: 99, Clock: 0}); item == nil || !item.Deleted {
		t.Fatal("client 99's clock 0 is not deleted")
	}

	// The sender sends its parked item again, under an allowance that holds
	// the drain: the queue drains, the parked deletion applies, and the
	// document equals the twin.
	doc.SetMaxIntegrateScan(testScanBound)
	for _, d := range []*Doc{doc, twin} {
		if err := ApplyUpdateV1(d, parked[0], "more"); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := fmt.Sprintf("%+v", doc.PendingStats()), fmt.Sprintf("%+v", twin.PendingStats()); got != want {
		t.Fatalf("the pending queue and deletes are %s, the twin's %s", got, want)
	}
	if a, b := doc.GetText("t").ToString(), twin.GetText("t").ToString(); a != b {
		t.Fatalf("the text diverged from the twin's: ends %q, the twin's %q", a[max(0, len(a)-12):], b[max(0, len(b)-12):])
	}
	if a, b := EncodeStateAsUpdateV1(doc, nil), EncodeStateAsUpdateV1(twin, nil); string(a) != string(b) {
		t.Fatal("the document's state, parked items and deletes included, differs from the twin's")
	}
}
