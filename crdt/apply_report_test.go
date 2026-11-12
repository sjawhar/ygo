package crdt

import (
	"testing"
)

// Tests for the report a committed apply makes (WithApplyReport): the totals
// the document is left holding, and the deltas the apply added.

// reports is what a document's report callback was called with, in order, so
// a test says "one report, and this is it" rather than reading a count the
// library keeps.
type reports struct {
	origins []any
	made    []ApplyReport
}

func (r *reports) take(origin any, report ApplyReport) {
	r.origins = append(r.origins, origin)
	r.made = append(r.made, report)
}

// only is the one report the applies made, and fails for any other number.
func (r *reports) only(t *testing.T) ApplyReport {
	t.Helper()
	if len(r.made) != 1 {
		t.Fatalf("%d reports were made, want one: %+v", len(r.made), r.made)
	}
	return r.made[0]
}

// structsHeld is what the document holds by the two counters the report's
// total is made of, read from outside the apply as an owner reads them.
func structsHeld(doc *Doc) int {
	return doc.StructCount() + doc.PendingStats().Items
}

// An apply that commits reports the structs the document holds, the ones
// parked in the pending queue included, and the deltas the budget counted
// for that apply.
//
// The parked structs are the point of the total. An owner sizing the next
// update's allowance from Doc.StructCount alone cannot see them, and the
// document holds every one of them in memory, returns them in its encoded
// state and integrates them as soon as their gap is filled: here five
// structs the apply parked leave Doc.StructCount at the one struct of the
// base text, and the report at six.
func TestUnit_ApplyReport_ReportsTheStructsHeldParkedIncludedAndTheApplysDeltas(t *testing.T) {
	const base, n = "hello world", 5
	update := parkedStructs(n)

	var made reports
	doc := budgetDoc(t, base)
	doc.SetApplyReport(made.take)
	if err := ApplyUpdateV1(doc, update, "peer"); err != nil {
		t.Fatalf("the update was refused: %v", err)
	}
	report := made.only(t)
	if made.origins[0] != "peer" {
		t.Fatalf("the report names origin %v, want the apply's \"peer\"", made.origins[0])
	}
	if got := doc.StructCount(); got != 1 {
		t.Fatalf("the store holds %d structs, want the one run of the base text", got)
	}
	if got := doc.PendingStats().Items; got != n {
		t.Fatalf("%d structs are parked, want the %d the update carried", got, n)
	}
	if report.Structs != 1+n {
		t.Fatalf("the report counts %d structs, want the one in the store and the %d parked", report.Structs, n)
	}
	if report.Structs != structsHeld(doc) {
		t.Fatalf("the report counts %d structs, want the %d an owner reads after the apply", report.Structs, structsHeld(doc))
	}
	if report.Integrated != n || report.Bytes != n {
		t.Fatalf("the report adds %d structs of %d bytes, want the %d one-byte structs the update parked",
			report.Integrated, report.Bytes, n)
	}

	// The deltas are the tally the budget keeps: the same update under a
	// budget one struct short is refused with those same four counts, which
	// is the only other route they reach a caller by.
	twin := budgetDoc(t, base)
	twin.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: n - 1, Bytes: -1, Split: -1}
	})
	before := stateOf(twin)
	refused := requireRefused(t, twin, before, ApplyUpdateV1(twin, update, "peer"), ErrApplyBudget)
	if report.Integrated != refused.Structs || report.Bytes != refused.Bytes ||
		report.Split != refused.Split || report.Decoded != refused.Decoded {
		t.Fatalf("the report adds %+v, want the tally the refusal of the same update carries: "+
			"%d structs, %d bytes, %d splits, %d decoded",
			report, refused.Structs, refused.Bytes, refused.Split, refused.Decoded)
	}
}

// The struct total is what the document holds once the apply has committed,
// not the structs the apply linked: the commit squashes a run into one
// struct (squashRuns) and merges back the split halves it no longer needs
// (tryMergeWithLefts), and a total read before those ran would be a number
// the document never holds.
//
// Here the apply carries one character, its drain lands the five its sender
// parked earlier, and the commit squashes all six into one run beside the
// base text: seven structs were linked, two are held, and only the second
// number is one an owner can compare with the count of a later load.
func TestUnit_ApplyReport_TheTotalIsWhatTheCommitLeavesTheDocumentHolding(t *testing.T) {
	const base, n = "hello world", 5
	doc := budgetDoc(t, base)
	if err := ApplyUpdateV1(doc, parkedStructs(n), "parker"); err != nil {
		t.Fatalf("the parking update was refused: %v", err)
	}

	var made reports
	doc.SetApplyReport(made.take)
	if err := ApplyUpdateV1(doc, gapStringUpdate(0, "A", nil), "filler"); err != nil {
		t.Fatalf("the update that fills the gap was refused: %v", err)
	}
	report := made.only(t)
	if got := doc.PendingStats().Items; got != 0 {
		t.Fatalf("%d structs are still parked, want the drain to have taken them all", got)
	}
	if got := doc.StructCount(); got != 2 {
		t.Fatalf("the store holds %d structs, want the base run and the one run the commit squashed "+
			"the fill and the %d structs its drain landed into", got, n)
	}
	if report.Structs != structsHeld(doc) {
		t.Fatalf("the report counts %d structs, want the %d the document is left holding", report.Structs, structsHeld(doc))
	}
	if report.Integrated != 1 || report.Bytes != 1 {
		t.Fatalf("the report adds %d structs of %d bytes, want the one character the update carried: "+
			"what its drain landed was charged to the apply that parked it",
			report.Integrated, report.Bytes)
	}
}

// An apply the document refuses reports nothing: it leaves the document as
// it was, and its tally is in the RefusedUpdateError. An owner that took a
// report for a refused apply would raise its account of the room on an
// update the room never took.
func TestUnit_ApplyReport_ARefusedApplyReportsNothing(t *testing.T) {
	const base = "hello world"
	update := threeInserts(t, base)

	var made reports
	doc := budgetDoc(t, base)
	doc.SetApplyReport(made.take)
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: 2, Bytes: -1, Split: -1}
	})
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, "peer"), ErrApplyBudget)
	if len(made.made) != 0 {
		t.Fatalf("the refused apply reported %+v, want nothing", made.made)
	}

	// Nor does an update refused before the apply began to read it.
	if err := ApplyUpdateV1(doc, []byte{0xff, 0xff, 0xff}, "peer"); err == nil {
		t.Fatal("unreadable bytes were applied")
	}
	if len(made.made) != 0 {
		t.Fatalf("an unreadable update reported %+v, want nothing", made.made)
	}

	// The same update under a budget that holds it commits, and reports the
	// tally the refusal carried: one apply, two outcomes, one set of counts.
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: 3, Bytes: -1, Split: -1}
	})
	requireApplied(t, doc, before, ApplyUpdateV1(doc, update, "peer"))
	report := made.only(t)
	if report.Integrated != refused.Structs || report.Bytes != refused.Bytes ||
		report.Decoded != refused.Decoded {
		t.Fatalf("the report adds %+v, want the %d structs, %d bytes and %d values the refusal counted",
			report, refused.Structs, refused.Bytes, refused.Decoded)
	}
	if report.Structs != structsHeld(doc) {
		t.Fatalf("the report counts %d structs, want the %d the document holds", report.Structs, structsHeld(doc))
	}
}

// The splits a drain of the pending queue makes are the landing apply's, so
// they are in its report: ten parked characters, each pinned between two
// clocks of the run the base text is held in, cut that run into eleven
// pieces when the gap is filled. Without them the report would say an apply
// that added one struct left the document twenty-two structs large with no
// account of the twenty.
func TestUnit_ApplyReport_ADrainsSplitsAreInTheReport(t *testing.T) {
	const base, n = "hello world", 10 // ten interior clocks of the run
	fill := gapStringUpdate(0, "A", nil)

	for name, v2 := range map[string]bool{"V1": false, "V2": true} {
		t.Run(name, func(t *testing.T) {
			doc := budgetDoc(t, base)
			if err := ApplyUpdateV1(doc, parkedSplitters(n), nil); err != nil {
				t.Fatalf("the parking update was refused: %v", err)
			}
			var made reports
			doc.SetApplyReport(made.take)
			if err := applyEither(t, doc, fill, v2); err != nil {
				t.Fatalf("the update that fills the gap was refused: %v", err)
			}
			report := made.only(t)
			if report.Split != n {
				t.Fatalf("the report counts %d splits, want the %d the drain cut the run into pieces by", report.Split, n)
			}
			if report.Integrated != 1 || report.Bytes != 1 {
				t.Fatalf("the report adds %d structs of %d bytes, want the one character the fill carried",
					report.Integrated, report.Bytes)
			}
			// The run in n+1 pieces, the fill, and the n structs the drain
			// landed: none of them merges back, each having one of the
			// landed characters beside it.
			if want := (n + 1) + 1 + n; report.Structs != want || structsHeld(doc) != want {
				t.Fatalf("the report counts %d structs and the document holds %d, want %d",
					report.Structs, structsHeld(doc), want)
			}
		})
	}

	// A drain the budget takes back is reported as the budget counted it:
	// the splits that passed ApplyBudget.Split stay in Split though the
	// document kept none of them, since the take-back gives back the cuts
	// and not the count (drainParked). The struct total is of the document
	// either way, which is what an owner reads the report for.
	back := budgetDoc(t, base)
	if err := ApplyUpdateV1(back, parkedSplitters(n), nil); err != nil {
		t.Fatalf("the parking update was refused: %v", err)
	}
	var backMade reports
	back.SetApplyReport(backMade.take)
	back.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: 1, Bytes: 1, Split: n - 1}
	})
	if err := ApplyUpdateV1(back, fill, nil); err != nil {
		t.Fatalf("the fill was refused for a drain that is not its own: %v", err)
	}
	report := backMade.only(t)
	if got := back.PendingStats().Items; got != n {
		t.Fatalf("%d structs are parked, want the %d the taken-back drain left", got, n)
	}
	if report.Split != n {
		t.Fatalf("the report counts %d splits, want the %d the taken-back drain counted", report.Split, n)
	}
	if want := 2 + n; report.Structs != want || structsHeld(back) != want {
		t.Fatalf("the report counts %d structs and the document holds %d, want the base run, the fill and the %d still parked",
			report.Structs, structsHeld(back), n)
	}
}
