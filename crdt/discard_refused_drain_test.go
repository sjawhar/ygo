package crdt

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests for Doc.SetDiscardRefusedDrain: a drain of the pending queue that a
// document takes back discards what the update that filled the gap released,
// instead of parking it again, and the apply says what it discarded and who
// parked it (DrainDiscardedError).

// discardScenario is newDrainScenario's document with the option on, the
// attacker's parked update applied with origin "attacker" once the option is
// set: client 4's "EVIL" after client 5's "y", which the document lacks,
// and, with deletes, client 4's deletion of that "y" too.
func discardScenario(t *testing.T, deletes bool) (*drainScenario, *Doc) {
	t.Helper()
	return discardScenarioBy(t, deletes, "attacker")
}

// discardScenarioBy is discardScenario with the attacker's update applied
// with origin.
func discardScenarioBy(t *testing.T, deletes bool, origin any) (*drainScenario, *Doc) {
	t.Helper()
	s := newDrainScenario(t)
	attacker := New(WithClientID(4))
	for _, u := range [][]byte{s.base, s.y} {
		require.NoError(t, ApplyUpdateV1(attacker, u, nil))
	}
	sv := attacker.StateVector()
	text := attacker.GetText("t")
	attacker.Transact(func(txn *Transaction) {
		text.Insert(txn, text.Len(), "EVIL", nil)
		if deletes {
			text.Delete(txn, text.Len()-5, 1) // the "y"
		}
	})
	doc := checkedDoc(t, s.base)
	doc.SetDiscardRefusedDrain(true)
	require.NoError(t, ApplyUpdateV1(doc, EncodeStateAsUpdateV1(attacker, sv), origin))
	require.Equal(t, 1, doc.PendingStats().Items, "EVIL parks behind client 5's gap")
	doc.SetApplyCheck(refuseEvil(doc.GetText("t")))
	return s, doc
}

// requireDiscarded returns err as a DrainDiscardedError, which reads as a
// drain refused while the update stood (ErrApplyCheckDrain) and wraps cause.
func requireDiscarded(t *testing.T, err, cause error) *DrainDiscardedError {
	t.Helper()
	var discarded *DrainDiscardedError
	require.ErrorAs(t, err, &discarded, "the apply returns a DrainDiscardedError")
	require.ErrorIs(t, err, ErrApplyCheckDrain)
	require.ErrorIs(t, err, cause)
	var refused *RefusedUpdateError
	assert.NotErrorAs(t, err, &refused, "the discard does not read as a refusal of the update")
	return discarded
}

// A drain the apply check refuses is taken back and what the fill released
// discarded: the parked struct and the parked deletion of the update's own
// clock. The update that filled the gap stands, the apply names what it
// discarded and who parked it, and the next update of the same client finds
// nothing left to drain, where off the refusal repeats with each of them.
func TestUnit_DiscardRefusedDrain_ARefusedDrainDiscardsWhatTheFillReleased(t *testing.T) {
	s, doc := discardScenario(t, true)
	require.Equal(t, 1, doc.PendingStats().DeleteRanges, "the deletion of the y parks too")

	discarded := requireDiscarded(t, ApplyUpdateV1(doc, s.y, "honest"), errCheckRefused)
	assert.Equal(t, 1, discarded.Structs)
	assert.Equal(t, 1, discarded.Ranges)
	assert.Equal(t, []any{"attacker"}, discarded.Senders)
	assert.Equal(t, "hello worldy", doc.GetText("t").ToString(), "the y stands, not deleted")
	assert.Equal(t, PendingStats{}, doc.PendingStats(), "nothing parked is left")

	require.NoError(t, ApplyUpdateV1(doc, s.z, "honest"), "nothing is left to drain, or to refuse")
	require.NoError(t, ApplyUpdateV1(s.control, s.y, nil))
	require.NoError(t, ApplyUpdateV1(s.control, s.z, nil))
	for _, pair := range [][2]*Doc{{doc, s.control}, {s.control, doc}} {
		held, err := DocContainsUpdateV1(pair[0], EncodeStateAsUpdateV1(pair[1], nil))
		require.NoError(t, err)
		assert.True(t, held, "the document is the control that never heard from the attacker")
	}
}

// Off, the same refusal parks the backlog again whole and returns the check's
// refusal alone, as before the option.
func TestUnit_DiscardRefusedDrain_OffParksTheBacklogAgain(t *testing.T) {
	s, doc := discardScenario(t, true)
	doc.SetDiscardRefusedDrain(false)

	err := ApplyUpdateV1(doc, s.y, "honest")
	requireDrainRefused(t, err)
	var discarded *DrainDiscardedError
	assert.NotErrorAs(t, err, &discarded)
	assert.Equal(t, 1, doc.PendingStats().Items, "EVIL is parked again")
	assert.Equal(t, 1, doc.PendingStats().DeleteRanges, "and so is the deletion")
	requireDrainRefused(t, ApplyUpdateV1(doc, s.z, "honest"))
}

// A parked deletion is discarded only below the clocks the fill released:
// its part past them stays parked, and lands with the update that brings
// those clocks. Each apply that parked what was discarded is named once.
func TestUnit_DiscardRefusedDrain_ADeleteRangeIsDiscardedBelowTheReleasedClocksOnly(t *testing.T) {
	s, doc := discardScenario(t, false)
	// Client 6 holds the y and the z, and deletes both.
	six := New(WithClientID(6))
	for _, u := range [][]byte{s.base, s.y, s.z} {
		require.NoError(t, ApplyUpdateV1(six, u, nil))
	}
	sv := six.StateVector()
	text := six.GetText("t")
	six.Transact(func(txn *Transaction) { text.Delete(txn, text.Len()-2, 2) })
	require.NoError(t, ApplyUpdateV1(doc, EncodeStateAsUpdateV1(six, sv), "six"))
	require.Equal(t, 1, doc.PendingStats().DeleteRanges)

	discarded := requireDiscarded(t, ApplyUpdateV1(doc, s.y, "honest"), errCheckRefused)
	assert.Equal(t, 1, discarded.Structs)
	assert.Equal(t, 1, discarded.Ranges, "the deletion of the y")
	assert.ElementsMatch(t, []any{"attacker", "six"}, discarded.Senders)
	assert.Equal(t, "hello worldy", doc.GetText("t").ToString())
	assert.Equal(t, PendingStats{DeleteRanges: 1}, doc.PendingStats(), "the deletion of the z stays parked")

	require.NoError(t, ApplyUpdateV1(doc, s.z, "honest"))
	assert.Equal(t, "hello worldy", doc.GetText("t").ToString(), "the z lands and its parked deletion with it")
	assert.Equal(t, PendingStats{}, doc.PendingStats())
}

// A drain past its scan allowance is discarded as one the check refused is,
// with the allowance's error as its cause. Under the race detector, which
// makes each scan several times slower, the bound is an eighth of
// testScanBound, against 500 items and 300 parked, whose drain still passes
// it.
func TestUnit_DiscardRefusedDrain_ADrainPastItsScanAllowanceIsDiscarded(t *testing.T) {
	l, n, bound := 5_000, 500, testScanBound
	if raceEnabled {
		l, n, bound = 500, 300, testScanBound/8
	}
	doc := chainedList(t, l, WithMaxIntegrateScan(bound))
	doc.SetDiscardRefusedDrain(true)
	text := doc.GetText("t")
	require.NoError(t, ApplyUpdateV1(doc, parkedBehindAGap(n), "parker"))
	require.Equal(t, n, doc.PendingStats().Items)
	last := text.start
	for last.Right != nil {
		last = last.Right
	}

	discarded := requireDiscarded(t, ApplyUpdateV1(doc, fillTheGap(last), "filler"), ErrIntegrateScanBudget)
	assert.Equal(t, n, discarded.Structs)
	assert.Zero(t, discarded.Ranges)
	assert.Equal(t, []any{"parker"}, discarded.Senders)
	assert.GreaterOrEqual(t, discarded.Scanned, bound, "the scans passed the allowance")
	assert.Equal(t, PendingStats{}, doc.PendingStats())
	assert.Equal(t, uint64(1), doc.StateVector().Clock(99), "the fill stands")
}

// A drain whose split passes the split budget is discarded too, with the
// budget's error as its cause, and the fill stands with its own content.
func TestUnit_DiscardRefusedDrain_ADrainPastTheSplitBudgetIsDiscarded(t *testing.T) {
	const base = "hello world"
	doc := budgetDoc(t, base)
	doc.SetDiscardRefusedDrain(true)
	require.NoError(t, ApplyUpdateV1(doc, parkedRunAndSplitter(), "parker"))
	require.Equal(t, 2, doc.PendingStats().Items)
	doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: 0} })

	discarded := requireDiscarded(t, ApplyUpdateV1(doc, gapStringUpdate(0, "A", nil), "filler"), ErrApplyBudget)
	assert.Equal(t, 2, discarded.Structs)
	assert.Equal(t, []any{"parker"}, discarded.Senders)
	assert.Equal(t, PendingStats{}, doc.PendingStats())
	assert.Equal(t, base+"A", doc.GetText("t").ToString(), "the fill's own character stands")
}

// Only what the fill released is discarded: a struct parked behind a gap the
// fill leaves open stays parked, and a park made before the option was set
// names nobody.
func TestUnit_DiscardRefusedDrain_WhatTheFillDoesNotReleaseStaysParked(t *testing.T) {
	s := newDrainScenario(t) // EVIL parked with no origin recorded
	doc := s.doc
	doc.SetDiscardRefusedDrain(true)
	other := New(WithClientID(8))
	for _, u := range [][]byte{s.base, s.y, s.z} {
		require.NoError(t, ApplyUpdateV1(other, u, nil))
	}
	sv := other.StateVector()
	text := other.GetText("t")
	other.Transact(func(txn *Transaction) { text.Insert(txn, text.Len(), "after z", nil) })
	require.NoError(t, ApplyUpdateV1(doc, EncodeStateAsUpdateV1(other, sv), "other"))
	require.Equal(t, 2, doc.PendingStats().Items)
	doc.SetApplyCheck(refuseEvil(doc.GetText("t")))

	discarded := requireDiscarded(t, ApplyUpdateV1(doc, s.y, "honest"), errCheckRefused)
	assert.Equal(t, 1, discarded.Structs, "EVIL, which the y released")
	assert.Empty(t, discarded.Senders, "EVIL was parked before the option was set")
	assert.Equal(t, 1, doc.PendingStats().Items, "the struct behind the z stays parked")

	require.NoError(t, ApplyUpdateV1(doc, s.z, "honest"), "the z releases it, and the check takes it")
	assert.Equal(t, "hello worldyzafter z", doc.GetText("t").ToString())
	assert.Zero(t, doc.PendingStats().Items)
}

// An origin that cannot be a map key is named as it is, without a panic.
func TestUnit_DiscardRefusedDrain_AnOriginThatIsNoMapKeyIsNamed(t *testing.T) {
	unhashable := []string{"a peer"}
	s, doc := discardScenarioBy(t, true, unhashable)
	discarded := requireDiscarded(t, ApplyUpdateV1(doc, s.y, "honest"), errCheckRefused)
	assert.Equal(t, []any{unhashable, unhashable}, discarded.Senders,
		"one origin no map can hold is named for the struct and for the deletion")
}

// A parked deletion still names an origin when the record of who parked it
// keeps its pending range under another sender's: here a's frame parks every
// other clock past the y's and b's parks the range over them, the y's clock
// with it, so the record, past twice the one pending range, keeps that range
// under a, the first to touch it.
func TestUnit_DiscardRefusedDrain_ARangeKeptUnderAnotherSendersOriginIsNamed(t *testing.T) {
	s, doc := discardScenario(t, false)
	y := doc.StateVector().Clock(5)
	const m = 20
	singles := make([]DeleteRange, m)
	for j := range singles {
		singles[j] = DeleteRange{Clock: y + 1 + 2*uint64(j), Len: 1}
	}
	require.NoError(t, ApplyUpdateV1(doc, deleteSetUpdate(map[ClientID][]DeleteRange{5: singles}), "a"))
	require.NoError(t, ApplyUpdateV1(doc, deleteSetUpdate(map[ClientID][]DeleteRange{5: {{Clock: y, Len: 2*m + 1}}}), "b"))
	require.Equal(t, 1, doc.PendingStats().DeleteRanges)
	require.Equal(t, 1, recordedRanges(doc), "the record keeps the one pending range under one origin")

	discarded := requireDiscarded(t, ApplyUpdateV1(doc, s.y, "honest"), errCheckRefused)
	assert.Equal(t, 1, discarded.Ranges, "the deletion of the y")
	assert.ElementsMatch(t, []any{"attacker", "a"}, discarded.Senders)
	assert.Equal(t, PendingStats{DeleteRanges: 1}, doc.PendingStats(), "the deletions past the y stay parked")
}

// A forgotten origin is dropped from the record of who parked each struct
// and delete range, which then holds nothing of it, and a discard of what it
// parked names it no more, while what it parked stays parked until the
// discard. Forgetting another origin, or one no map can hold, changes
// nothing.
func TestUnit_DiscardRefusedDrain_AForgottenOriginIsNoLongerNamed(t *testing.T) {
	type sender struct{ name string }
	for _, c := range []struct {
		name    string
		forget  func(attacker *sender) any
		senders func(attacker *sender) []any
	}{
		{"the parking sender", func(attacker *sender) any { return attacker }, func(*sender) []any { return nil }},
		{"another sender", func(*sender) any { return &sender{"another"} }, func(attacker *sender) []any { return []any{attacker} }},
		{"an origin no map can hold", func(*sender) any { return []string{"a peer"} }, func(attacker *sender) []any { return []any{attacker} }},
	} {
		t.Run(c.name, func(t *testing.T) {
			attacker := &sender{"attacker"}
			s, doc := discardScenarioBy(t, true, attacker)
			require.Equal(t, 1, doc.PendingStats().DeleteRanges, "the deletion of the y parks with EVIL")

			forgotten := c.forget(attacker)
			doc.ForgetParkedOrigin(forgotten)
			if forgotten == any(attacker) {
				for item, origin := range doc.parkedBy {
					assert.NotEqual(t, any(attacker), origin, "the record of parked structs keeps the forgotten origin for %v", item.ID)
				}
				assert.Empty(t, doc.parkedDsBy, "the record of parked deletions keeps the forgotten origin")
			} else {
				assert.Len(t, doc.parkedDsBy, 1, "forgetting another origin drops nothing")
			}
			assert.Equal(t, recordedRanges(doc), doc.parkedDsByRanges)
			assert.Equal(t, 1, doc.PendingStats().Items, "what the forgotten origin parked stays parked")

			discarded := requireDiscarded(t, ApplyUpdateV1(doc, s.y, "honest"), errCheckRefused)
			assert.Equal(t, 1, discarded.Structs)
			assert.Equal(t, 1, discarded.Ranges)
			assert.ElementsMatch(t, c.senders(attacker), discarded.Senders)
		})
	}
}

// requireRecordWithinQueue requires the record of who parked each struct to
// hold no struct the pending queue does not hold, and so no more structs than
// the queue holds.
func requireRecordWithinQueue(t *testing.T, doc *Doc, step string) {
	t.Helper()
	held := make(map[*Item]struct{})
	if p := doc.store.pending; p != nil {
		for _, it := range p.heldItems() {
			held[it] = struct{}{}
		}
	}
	require.LessOrEqual(t, len(doc.parkedBy), doc.PendingStats().Items,
		"after %s the record of parked structs holds more structs than the queue", step)
	for it := range doc.parkedBy {
		_, ok := held[it]
		require.True(t, ok, "after %s the record holds client %d's struct at clock %d, which the queue does not hold",
			step, it.ID.Client, it.ID.Clock)
	}
}

// coverParkedStrings runs, n times, the two frames that grew the record of who
// parked each struct past the queue: the first parks a string of size bytes
// behind a gap of client 9, and the second parks a deleted struct, one clock
// earlier, that covers it, so the merge drops the string. Each pass leaves the
// queue one struct longer and tells the record of two. parked, when not nil,
// gets each string's struct once it parks, and check runs after each frame.
func coverParkedStrings(t *testing.T, doc *Doc, n, size int, parked func(*Item), check func(step string)) {
	t.Helper()
	clock := uint64(2)
	for i := range n {
		str := &Item{ID: ID{Client: 9, Clock: clock}, Origin: &ID{Client: 9, Clock: clock - 1},
			Content: NewContentString(strings.Repeat(string(rune('a'+i%26)), size))}
		require.NoError(t, ApplyUpdateV1(doc, craftedUpdate(str), "a"), "pass %d's string", i)
		if parked != nil {
			held := doc.store.pending.heldItems()
			parked(held[len(held)-1])
		}
		check(fmt.Sprintf("pass %d's string", i))
		cover := &Item{ID: ID{Client: 9, Clock: clock - 1}, Origin: &ID{Client: 9, Clock: clock - 2},
			Content: NewContentDeleted(uint64(size) + 1)}
		require.NoError(t, ApplyUpdateV1(doc, craftedUpdate(cover), "a"), "pass %d's cover", i)
		check(fmt.Sprintf("pass %d's cover", i))
		require.Equal(t, i+1, doc.PendingStats().Items, "after pass %d the covers alone are parked", i)
		clock += uint64(size) + 3
	}
}

// typedTwice is client's "a" and then its "b" after it, each as the V1
// update a document holding what came before takes: the second waits on the
// first.
func typedTwice(client ClientID) (first, second []byte) {
	w := New(WithClientID(client))
	text := w.GetText("t")
	w.Transact(func(txn *Transaction) { text.Insert(txn, 0, "a", nil) })
	first = EncodeStateAsUpdateV1(w, nil)
	sv := w.StateVector()
	w.Transact(func(txn *Transaction) { text.Insert(txn, 1, "b", nil) })
	return first, EncodeStateAsUpdateV1(w, sv)
}

// A struct that leaves the pending queue, for any reason, leaves the record of
// who parked each struct, so the record never holds a struct the queue does
// not. A merge that drops a parked string while the struct that covers it
// parks used to leave the record two structs for the queue's one, below the
// 2*held+16 that pruned it, and each dropped string reachable from it.
func TestUnit_DiscardRefusedDrain_TheRecordOfParkedStructsHoldsOnlyWhatTheQueueHolds(t *testing.T) {
	t.Run("dropped by a merge", func(t *testing.T) {
		doc := New(WithClientID(1))
		doc.SetApplyBudget(func(any) ApplyBudget {
			return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: -1}
		})
		doc.SetDiscardRefusedDrain(true)
		coverParkedStrings(t, doc, 40, 64, nil, func(step string) { requireRecordWithinQueue(t, doc, step) })
	})

	t.Run("landed by a drain", func(t *testing.T) {
		doc := New(WithClientID(1))
		doc.SetDiscardRefusedDrain(true)
		fill, lands := typedTwice(9)
		_, stays := typedTwice(8)
		require.NoError(t, ApplyUpdateV1(doc, lands, "a"))
		require.NoError(t, ApplyUpdateV1(doc, stays, "b"))
		require.Len(t, doc.parkedBy, 2)
		require.NoError(t, ApplyUpdateV1(doc, fill, "c"))
		require.Equal(t, 1, doc.PendingStats().Items, "the fill lands client 9's struct, and client 8's stays parked")
		requireRecordWithinQueue(t, doc, "the fill")
	})

	t.Run("discarded", func(t *testing.T) {
		s, doc := discardScenario(t, false)
		_, stays := typedTwice(8)
		require.NoError(t, ApplyUpdateV1(doc, stays, "b"))
		require.Len(t, doc.parkedBy, 2)
		discarded := requireDiscarded(t, ApplyUpdateV1(doc, s.y, "honest"), errCheckRefused)
		require.Equal(t, 1, discarded.Structs, "EVIL is discarded")
		require.Equal(t, 1, doc.PendingStats().Items, "client 8's struct stays parked")
		requireRecordWithinQueue(t, doc, "the discard")
	})
}
