package crdt

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// originAtTheStartFrom is originAtTheStart from client, starting at clock: n
// one-character items each naming the first item of chainedList's text as its
// left origin and no right origin, so each scans the whole list.
func originAtTheStartFrom(client ClientID, clock uint64, n int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(uint64(n))
	enc.WriteVarUint(uint64(client))
	enc.WriteVarUint(clock)
	for range n {
		enc.WriteUint8(wireString | flagHasOrigin)
		enc.WriteVarUint(5)
		enc.WriteVarUint(0)
		enc.WriteVarString("x")
	}
	enc.WriteVarUint(0) // delete set: no clients
	return append([]byte(nil), enc.Bytes()...)
}

// scannedBy applies update to doc as a remote update and returns what its
// committed transaction reports its scans passed.
func scannedBy(t *testing.T, doc *Doc, update []byte) int {
	t.Helper()
	scanned := -1
	stop := doc.OnAfterTransaction(func(txn *Transaction) { scanned = txn.IntegrateScanned() })
	defer stop()
	if err := ApplyUpdateV1(doc, update, nil); err != nil {
		t.Fatal(err)
	}
	if scanned < 0 {
		t.Fatal("the apply committed no transaction")
	}
	return scanned
}

// A document with a budget asks it, once as each remote apply begins and with
// that apply's origin, how many items the apply's scans may pass, so updates
// applied with different origins are bounded differently, in whatever order
// they arrive: an update is refused at one item under what its scans pass and
// taken at exactly that, by the same document; an answer of zero leaves that
// apply unbounded; and the V2 form is bounded the same way.
func TestUnit_IntegrateScanBudget_EachRemoteApplyIsBoundedByTheBudgetItsOriginIsGiven(t *testing.T) {
	const l, n = 2_000, 20
	first, second := originAtTheStartFrom(9, 0, n), originAtTheStartFrom(10, 0, n)
	secondV2, err := UpdateV1ToV2(second)
	if err != nil {
		t.Fatal(err)
	}
	// What the first update's scans pass, measured on a twin with no budget.
	scans := scannedBy(t, chainedList(t, l), first)
	if scans < l {
		t.Fatalf("the update's scans passed %d items, want at least the list's %d", scans, l)
	}

	doc := chainedList(t, l)
	var asked []any
	doc.SetIntegrateScanBudget(func(origin any) int {
		asked = append(asked, origin)
		switch origin {
		case "tight":
			return scans - 1
		case "enough":
			return scans
		}
		return 0
	})
	before, text := EncodeStateAsUpdateV1(doc, nil), doc.GetText("t").ToString()
	updates := observed(doc)
	if err := ApplyUpdateV1(doc, first, "tight"); !errors.Is(err, ErrIntegrateScanBudget) {
		t.Fatalf("an update scanning %d items was applied under a budget of %d with err %v", scans, scans-1, err)
	}
	requireUnchanged(t, doc, before, text, updates)
	if err := ApplyUpdateV1(doc, first, "enough"); err != nil {
		t.Fatalf("an update scanning %d items was refused under a budget of %d: %v", scans, scans, err)
	}
	afterFirst, textAfterFirst := EncodeStateAsUpdateV1(doc, nil), doc.GetText("t").ToString()
	*updates = 0
	if err := ApplyUpdateV2(doc, secondV2, "tight"); !errors.Is(err, ErrIntegrateScanBudget) {
		t.Fatalf("the second update was applied under a budget of %d with err %v", scans-1, err)
	}
	requireUnchanged(t, doc, afterFirst, textAfterFirst, updates)
	if err := ApplyUpdateV1(doc, second, "unbounded"); err != nil {
		t.Fatalf("an apply its budget answered zero for was refused: %v", err)
	}
	if want := []any{"tight", "enough", "tight", "unbounded"}; !slices.Equal(asked, want) {
		t.Fatalf("the budget was asked for %v, want %v", asked, want)
	}

	twin := chainedList(t, l)
	for _, update := range [][]byte{first, second} {
		if err := ApplyUpdateV1(twin, update, nil); err != nil {
			t.Fatal(err)
		}
	}
	if a, b := EncodeStateAsUpdateV1(doc, nil), EncodeStateAsUpdateV1(twin, nil); string(a) != string(b) {
		t.Fatal("the budgeted document differs from a twin that took both updates unbounded")
	}
}

// A document with a budget is bounded whatever the budget answers: one that
// answers zero for every apply still refuses a remote update carrying a
// subdocument. While a budget is set the document's bound is not used, and
// removing the budget leaves the bound as it was.
func TestUnit_IntegrateScanBudget_ABudgetBoundsADocumentInPlaceOfItsBound(t *testing.T) {
	writer := New(WithClientID(3))
	m := writer.GetMap("m")
	writer.Transact(func(txn *Transaction) { m.Set(txn, "sub", New()) })
	subdocument := EncodeStateAsUpdateV1(writer, nil)
	unbounded := func(any) int { return 0 }
	budgeted := New(WithIntegrateScanBudget(unbounded))
	if err := ApplyUpdateV1(budgeted, subdocument, nil); !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("a document with a budget applied a subdocument with err %v", err)
	}
	if len(budgeted.StateVector()) != 0 {
		t.Fatalf("the refused subdocument left state %v", budgeted.StateVector())
	}

	const l, n = 2_000, 20
	doc := chainedList(t, l, WithMaxIntegrateScan(1), WithIntegrateScanBudget(unbounded))
	if err := ApplyUpdateV1(doc, originAtTheStartFrom(9, 0, n), nil); err != nil {
		t.Fatalf("the budget's zero did not take the bound's place: %v", err)
	}
	doc.SetIntegrateScanBudget(nil)
	if err := ApplyUpdateV1(doc, originAtTheStartFrom(9, n, n), nil); !errors.Is(err, ErrIntegrateScanBudget) {
		t.Fatalf("with the budget removed the bound of one item applied an update with err %v", err)
	}
}

// A committed transaction reports the items its conflict scans passed, on a
// document with no bound as on a bounded one, and the count is exactly what the
// bound measures: a document bounded at the count takes the update and one
// bounded one item under it refuses it. A local insert between two neighbours
// scans nothing.
func TestUnit_IntegrateScanBudget_ATransactionReportsTheItemsItsScansPassed(t *testing.T) {
	const l, n = 2_000, 20
	update := originAtTheStartFrom(9, 0, n)
	scans := scannedBy(t, chainedList(t, l), update)
	// The first item passes the l-1 items after the list's first, each naming
	// the one before it, and lands at the list's end. Every later item passes
	// the same l-1 and then stops at the item of its own update placed just
	// before it - the same origin, the same client - which it counts and lands
	// in front of, so it passes l (Item.integrate).
	if want := n*(l-1) + (n - 1); scans != want {
		t.Fatalf("the update's scans passed %d items, want %d", scans, want)
	}
	if got := scannedBy(t, chainedList(t, l, WithMaxIntegrateScan(scans)), update); got != scans {
		t.Fatalf("a bounded document counted %d items, the unbounded one %d", got, scans)
	}
	if err := ApplyUpdateV1(chainedList(t, l, WithMaxIntegrateScan(scans-1)), update, nil); !errors.Is(err, ErrIntegrateScanBudget) {
		t.Fatalf("a document bounded one item under the count applied the update with err %v", err)
	}

	local := chainedList(t, l)
	scanned := -1
	local.OnAfterTransaction(func(txn *Transaction) { scanned = txn.IntegrateScanned() })
	text := local.GetText("t")
	local.Transact(func(txn *Transaction) { text.Insert(txn, l/2, "y", nil) })
	if scanned != 0 {
		t.Fatalf("a local insert between two neighbours reported %d items scanned", scanned)
	}
}

// The scans of the drain of the pending queue are counted with the update that
// filled its gap, and so are those of a drain a bounded document takes back for
// passing its allowance: what it integrated is gone, but the work it did is
// not, and the count stops at the allowance the drain was refused at.
func TestUnit_IntegrateScanBudget_TheScanCountHoldsADrainThatStaysAndOneTakenBack(t *testing.T) {
	const l, n = 2_000, 20
	lastItem := func(doc *Doc) *Item {
		last := doc.GetText("t").start
		for last.Right != nil {
			last = last.Right
		}
		return last
	}
	unbounded := chainedList(t, l)
	if err := ApplyUpdateV1(unbounded, parkedBehindAGap(n), nil); err != nil {
		t.Fatal(err)
	}
	drained := scannedBy(t, unbounded, fillTheGap(lastItem(unbounded)))
	if drained < n*(l-1) {
		t.Fatalf("the drain counted %d items, want at least the %d its items' scans of the list pass", drained, n*(l-1))
	}
	for _, c := range []struct {
		bound, scanned int
		clock          uint64
	}{
		{bound: drained, scanned: drained, clock: n + 1},
		{bound: drained - 1, scanned: drained - 1, clock: 1},
	} {
		doc := chainedList(t, l, WithMaxIntegrateScan(c.bound))
		if err := ApplyUpdateV1(doc, parkedBehindAGap(n), nil); err != nil {
			t.Fatal(err)
		}
		if got := scannedBy(t, doc, fillTheGap(lastItem(doc))); got != c.scanned {
			t.Fatalf("bounded at %d, the filling update counted %d items, want %d", c.bound, got, c.scanned)
		}
		if got := doc.StateVector().Clock(99); got != c.clock {
			t.Fatalf("bounded at %d, client 99's clock is %d, want %d", c.bound, got, c.clock)
		}
	}
}

// On a document with a budget, the drain of the pending queue draws on the
// allowance its apply was given, so one apply's scans, drain included, never
// pass that allowance: the drain is refused at what the update that filled the
// gap left of it, and taken back with the queue parked again while that update
// stands. A document with a bound keeps giving the drain an allowance of its
// own, so its apply may scan up to the bound twice over.
func TestUnit_IntegrateScanBudget_ABudgetedApplysDrainDrawsOnTheApplysOwnAllowance(t *testing.T) {
	const l, n = 2_000, 20
	// The filling update is client 99's clock 0 naming the list's first item
	// as its left origin, so it passes the l-1 items after it and lands at the
	// end; each parked item then passes those l-1 and the item of client 99
	// just in front of it, where it stops (Item.integrate): l.
	fill := originAtTheStartFrom(99, 0, 1)
	own := scannedBy(t, chainedList(t, l), fill)
	if own != l-1 {
		t.Fatalf("the filling update alone passed %d items, want %d", own, l-1)
	}
	parkedDoc := func(opts ...DocOption) *Doc {
		doc := chainedList(t, l, opts...)
		if err := ApplyUpdateV1(doc, parkedBehindAGap(n), "park"); err != nil {
			t.Fatal(err)
		}
		if parked := doc.PendingStats().Items; parked != n {
			t.Fatalf("%d items are parked, want %d", parked, n)
		}
		return doc
	}
	total := scannedBy(t, parkedDoc(), fill)
	drain := total - own
	if drain != n*l {
		t.Fatalf("the drain passed %d items, want %d", drain, n*l)
	}

	for _, c := range []struct {
		name                   string
		opt                    func(fill int) DocOption
		limit, scanned, parked int
		clock                  uint64
	}{
		{"a budget the apply and its drain fit", budgetFor, total, total, 0, n + 1},
		{"a budget one item short of the drain", budgetFor, total - 1, total - 1, n, 1},
		{"a budget the filling update alone fits", budgetFor, own, own, n, 1},
		{"a bound the drain alone fits", WithMaxIntegrateScan, drain, total, 0, n + 1},
		{"a bound one item short of the drain", WithMaxIntegrateScan, drain - 1, own + drain - 1, n, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			doc := parkedDoc(c.opt(c.limit))
			if got := scannedBy(t, doc, fill); got != c.scanned {
				t.Fatalf("the filling apply counted %d items, want %d", got, c.scanned)
			}
			if got := doc.StateVector().Clock(99); got != c.clock {
				t.Fatalf("client 99's clock is %d, want %d", got, c.clock)
			}
			if parked := doc.PendingStats().Items; parked != c.parked {
				t.Fatalf("%d items are parked, want %d", parked, c.parked)
			}
		})
	}
}

// budgetFor is a budget of limit for an apply with origin nil, as the filling
// updates of these tests are applied, and none for any other origin.
func budgetFor(limit int) DocOption {
	return WithIntegrateScanBudget(func(origin any) int {
		if origin == nil {
			return limit
		}
		return 0
	})
}

// A remote update a bounded document refuses fires no observer, so ApplyUpdate's
// error is what reports the scans it cost: a RefusedUpdateError whose Scanned
// is what the refused apply's scans passed - its whole allowance when the
// budget refused it, everything it scanned when it failed for another reason
// after scanning - and which errors.Is still matches against why it was
// refused, in the V1 and the V2 form.
func TestUnit_IntegrateScanBudget_ARefusedApplyReportsTheScansItCost(t *testing.T) {
	const l, n = 2_000, 20
	update := originAtTheStartFrom(9, 0, n)
	updateV2, err := UpdateV1ToV2(update)
	if err != nil {
		t.Fatal(err)
	}
	scans := scannedBy(t, chainedList(t, l), update)
	// The same update cut short of its delete set: every item integrates, and
	// then the apply fails.
	truncated := update[:len(update)-1]

	for _, c := range []struct {
		name        string
		limit       int
		apply       func(*Doc) error
		scanned     int
		scanRefusal bool
	}{
		{"V1 past its budget", scans - 1, func(d *Doc) error { return ApplyUpdateV1(d, update, nil) }, scans - 1, true},
		{"V2 past its budget", scans - 1, func(d *Doc) error { return ApplyUpdateV2(d, updateV2, nil) }, scans - 1, true},
		{"V1 failing after its scans", scans, func(d *Doc) error { return ApplyUpdateV1(d, truncated, nil) }, scans, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			doc := chainedList(t, l, budgetFor(c.limit))
			before, text := EncodeStateAsUpdateV1(doc, nil), doc.GetText("t").ToString()
			updates := observed(doc)
			committed := 0
			doc.OnAfterTransaction(func(*Transaction) { committed++ })
			err := c.apply(doc)
			var refused *RefusedUpdateError
			if !errors.As(err, &refused) {
				t.Fatalf("the refused apply returned %v, want a RefusedUpdateError", err)
			}
			if refused.Scanned != c.scanned {
				t.Fatalf("the refusal reports %d items scanned, want %d", refused.Scanned, c.scanned)
			}
			if !errors.Is(err, ErrInvalidUpdate) || errors.Is(err, ErrIntegrateScanBudget) != c.scanRefusal {
				t.Fatalf("the refusal %v does not match why the apply was refused", err)
			}
			requireUnchanged(t, doc, before, text, updates)
			if committed != 0 {
				t.Fatalf("the refused apply fired %d OnAfterTransaction observers, want none", committed)
			}
		})
	}
}

// A document with neither a budget nor a bound is not bounded: an update that
// fails after its items integrated keeps them, as it always did, its error is
// not a refusal, and its committed transaction reports the scans it cost.
func TestUnit_IntegrateScanBudget_ADocumentWithoutABudgetOrBoundKeepsWhatAFailedApplyIntegrated(t *testing.T) {
	const l, n = 2_000, 20
	update := originAtTheStartFrom(9, 0, n)
	scans := scannedBy(t, chainedList(t, l), update)
	doc := chainedList(t, l)
	scanned := -1
	doc.OnAfterTransaction(func(txn *Transaction) { scanned = txn.IntegrateScanned() })
	err := ApplyUpdateV1(doc, update[:len(update)-1], nil)
	var refused *RefusedUpdateError
	if !errors.Is(err, ErrInvalidUpdate) || errors.As(err, &refused) {
		t.Fatalf("the failed apply on an unbounded document returned %v, want an invalid update that is no refusal", err)
	}
	if scanned != scans {
		t.Fatalf("the failed apply's transaction reports %d items scanned, want %d", scanned, scans)
	}
	if got := doc.StateVector().Clock(9); got != n {
		t.Fatalf("client 9's clock is %d, want the %d items the failed apply integrated", got, n)
	}
}

// A panic from the WithIntegrateScanBudget callback is recovered as a refused
// update whose message names the callback, not item integration, and the
// document is left as it was.
func TestUnit_IntegrateScanBudget_APanicFromTheBudgetCallbackIsReportedAsTheCallbacks(t *testing.T) {
	doc := New(WithClientID(1))
	doc.SetIntegrateScanBudget(func(any) int { panic("boom in the budget") })
	before := EncodeStateAsUpdateV1(doc, nil)

	writer := New(WithClientID(2))
	written := writer.GetText("t")
	writer.Transact(func(txn *Transaction) { written.Insert(txn, 0, "hi", nil) })
	err := ApplyUpdateV1(doc, EncodeStateAsUpdateV1(writer, nil), "peer")
	if !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("apply returned %v, want it to wrap ErrInvalidUpdate", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "WithIntegrateScanBudget callback") || !strings.Contains(msg, "boom in the budget") {
		t.Fatalf("error is %q, want it to name the budget callback and the panic", msg)
	}
	if strings.Contains(err.Error(), "item integration") {
		t.Fatalf("error is %q, want it not to blame item integration", err.Error())
	}
	if after := EncodeStateAsUpdateV1(doc, nil); string(after) != string(before) {
		t.Fatal("the refused update changed the document's state")
	}
}

// A panic from the budget callback is a RefusedUpdateError that hands no
// observer an update, in V1 and V2.
func TestUnit_IntegrateScanBudget_APanicFromTheBudgetCallbackTellsNoObserver(t *testing.T) {
	writer := New(WithClientID(2))
	written := writer.GetText("t")
	writer.Transact(func(txn *Transaction) { written.Insert(txn, 0, "hi", nil) })
	update := EncodeStateAsUpdateV1(writer, nil)
	updateV2, err := UpdateV1ToV2(update)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name  string
		apply func(*Doc) error
	}{
		{"V1", func(d *Doc) error { return ApplyUpdateV1(d, update, "peer") }},
		{"V2", func(d *Doc) error { return ApplyUpdateV2(d, updateV2, "peer") }},
	} {
		t.Run(c.name, func(t *testing.T) {
			doc := New(WithClientID(1))
			doc.SetIntegrateScanBudget(func(any) int { panic("boom in the budget") })
			var handed [][]byte
			doc.OnUpdate(func(u []byte, _ any) { handed = append(handed, u) })
			err := c.apply(doc)
			if len(handed) != 0 {
				t.Fatalf("the observers were handed %x, want nothing (err %v)", handed, err)
			}
			var refused *RefusedUpdateError
			if !errors.As(err, &refused) {
				t.Fatalf("apply returned %v, want a RefusedUpdateError", err)
			}
		})
	}
}

// The zero RefusedUpdateError does not panic in Error, and errors.As onto it
// reads its fields.
func TestUnit_IntegrateScanBudget_TheZeroRefusedUpdateErrorIsSafe(t *testing.T) {
	var e RefusedUpdateError
	if got := e.Error(); got == "" {
		t.Fatal("the zero RefusedUpdateError's Error is empty, want a message")
	}
	wrapped := fmt.Errorf("apply: %w", &RefusedUpdateError{Err: ErrIntegrateScanBudget, Scanned: 7})
	var got *RefusedUpdateError
	if !errors.As(wrapped, &got) {
		t.Fatal("errors.As did not find the RefusedUpdateError")
	}
	if got.Scanned != 7 || !errors.Is(got, ErrIntegrateScanBudget) {
		t.Fatalf("recovered %+v, want Scanned 7 wrapping ErrIntegrateScanBudget", got)
	}
}
