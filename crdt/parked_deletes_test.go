package crdt

import (
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/reearth/ygo/encoding"
)

// deleteSetUpdate is an update of no structs whose delete set names, for each
// client, its ranges.
func deleteSetUpdate(sets map[ClientID][]DeleteRange) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(0) // clients
	enc.WriteVarUint(uint64(len(sets)))
	for _, client := range slices.Sorted(maps.Keys(sets)) {
		enc.WriteVarUint(uint64(client))
		enc.WriteVarUint(uint64(len(sets[client])))
		for _, r := range sets[client] {
			enc.WriteVarUint(r.Clock)
			enc.WriteVarUint(r.Len)
		}
	}
	return append([]byte(nil), enc.Bytes()...)
}

// deleteRangesUpdate is an update of no structs whose delete set names ranges
// of client.
func deleteRangesUpdate(client ClientID, ranges ...DeleteRange) []byte {
	return deleteSetUpdate(map[ClientID][]DeleteRange{client: ranges})
}

// applyIn applies the V1 update to doc as V1, or converted to V2.
func applyIn(t *testing.T, doc *Doc, v2 bool, update []byte) error {
	t.Helper()
	if !v2 {
		return ApplyUpdateV1(doc, update, nil)
	}
	converted, err := UpdateV1ToV2(update)
	if err != nil {
		t.Fatal(err)
	}
	return ApplyUpdateV2(doc, converted, nil)
}

var applyFormats = []struct {
	name string
	v2   bool
}{{"V1", false}, {"V2", true}}

// A keystroke costs the same whether or not 100,000 delete ranges of a client
// nobody holds are parked, on a document with no bound and on a bounded one: a
// client's parked ranges are retried only once its structs have moved past the
// first of them. In v1.51.3-sami.5 and its first sami.6 commit every remote
// apply retried every parked range and copied each one into a new set, about
// 1.6 MB for the keystroke against about 1.6 KB with nothing parked.
func TestAKeystrokeCostsNothingForParkedDeleteRanges(t *testing.T) {
	const l, n = 100, 100_000
	state := EncodeStateAsUpdateV1(chainedList(t, l), nil)
	keystroke := typedInto(t, state)
	for _, kind := range []struct {
		name string
		opts []DocOption
	}{
		{"unbounded", nil},
		{"bounded", []DocOption{WithMaxIntegrateScan(testScanBound)}},
	} {
		t.Run(kind.name, func(t *testing.T) {
			var allocated [2]uint64
			for i, parked := range [][]byte{nil, parkedDeletes(98, n)} {
				doc := New(kind.opts...)
				for _, update := range [][]byte{state, parked} {
					if update == nil {
						continue
					}
					if err := ApplyUpdateV1(doc, update, nil); err != nil {
						t.Fatal(err)
					}
				}
				if got, want := doc.PendingStats().DeleteRanges, i*n; got != want {
					t.Fatalf("%d delete ranges are parked, want %d", got, want)
				}
				var took time.Duration
				var err error
				allocated[i], took, err = applyMeasured(doc, keystroke)
				if err != nil {
					t.Fatal(err)
				}
				if got := doc.StateVector().Clock(11); got != 1 {
					t.Fatalf("client 11's clock is %d, want the keystroke applied", got)
				}
				if got, want := doc.PendingStats().DeleteRanges, i*n; got != want {
					t.Fatalf("after the keystroke %d delete ranges are parked, want %d", got, want)
				}
				t.Logf("%d delete ranges parked: allocated %d bytes in %s", i*n, allocated[i], took)
			}
			if without, with := allocated[0], allocated[1]; with > without+64<<10 {
				t.Fatalf("with %d delete ranges parked the keystroke allocated %d bytes, %d with none, want within %d of it",
					n, with, without, 64<<10)
			}
		})
	}
}

// Parked delete ranges are capped as parked items are (WithMaxPendingItems):
// an update whose delete set would leave more ranges parked than the cap is
// refused with ErrInvalidUpdate before it parks anything, and a bounded
// document is left as it was and tells no observer. The count is taken after
// the merge: a delete set sent again adds nothing, and ranges that fill the
// gaps between parked ones join them into one.
func TestParkedDeleteRangesAreCapped(t *testing.T) {
	const limit = 10
	for _, format := range applyFormats {
		for _, kind := range []struct {
			name    string
			opts    []DocOption
			bounded bool
		}{
			{"unbounded", nil, false},
			{"bounded", []DocOption{WithMaxIntegrateScan(testScanBound)}, true},
		} {
			t.Run(format.name+"/"+kind.name, func(t *testing.T) {
				doc := New(append([]DocOption{WithMaxPendingItems(limit)}, kind.opts...)...)
				parked := func() int { return doc.PendingStats().DeleteRanges }
				if err := applyIn(t, doc, format.v2, parkedDeletes(98, limit)); err != nil {
					t.Fatalf("%d ranges, the cap, were refused: %v", limit, err)
				}
				if got := parked(); got != limit {
					t.Fatalf("%d delete ranges are parked, want %d", got, limit)
				}
				if err := applyIn(t, doc, format.v2, parkedDeletes(98, limit)); err != nil {
					t.Fatalf("the same delete set sent again was refused: %v", err)
				}
				if got := parked(); got != limit {
					t.Fatalf("after the same delete set again %d delete ranges are parked, want %d", got, limit)
				}

				before, text := EncodeStateAsUpdateV1(doc, nil), doc.GetText("t").ToString()
				updates := observed(doc)
				err := applyIn(t, doc, format.v2, parkedDeletes(98, limit+1))
				if !errors.Is(err, ErrInvalidUpdate) {
					t.Fatalf("one range past the cap was applied with err %v, want ErrInvalidUpdate", err)
				}
				if got := parked(); got != limit {
					t.Fatalf("after the refusal %d delete ranges are parked, want %d", got, limit)
				}
				if kind.bounded {
					requireUnchanged(t, doc, before, text, updates)
				} else if after := EncodeStateAsUpdateV1(doc, nil); string(after) != string(before) {
					t.Fatal("the refused update changed the document's state")
				}

				// Clocks 1, 3, ..., 2*limit-3 fill every gap between the parked
				// ranges at 0, 2, ..., 2*limit-2, which join into one range.
				gaps := make([]DeleteRange, 0, limit-1)
				for i := range limit - 1 {
					gaps = append(gaps, DeleteRange{Clock: uint64(2*i + 1), Len: 1})
				}
				if err := applyIn(t, doc, format.v2, deleteRangesUpdate(98, gaps...)); err != nil {
					t.Fatalf("ranges that join the parked ones were refused: %v", err)
				}
				if got := parked(); got != 1 {
					t.Fatalf("after filling the gaps %d delete ranges are parked, want 1", got)
				}
				if err := applyIn(t, doc, format.v2, parkedDeletes(98, limit+1)); err != nil {
					t.Fatalf("an update leaving 2 ranges parked was refused: %v", err)
				}
				if got := parked(); got != 2 {
					t.Fatalf("%d delete ranges are parked, want 2", got)
				}
			})
		}
	}
}

// A parked delete range applies once the structs it names arrive, part by
// part, also after an apply that brings nothing for its client, and the
// document converges with a peer that saw every update in order.
func TestAParkedDeleteRangeAppliesOnceItsStructsArrive(t *testing.T) {
	writer := New(WithClientID(98))
	written := writer.GetText("t")
	writer.Transact(func(txn *Transaction) { written.Insert(txn, 0, "abc", nil) })
	first := EncodeStateAsUpdateV1(writer, nil)
	sv := writer.StateVector()
	writer.Transact(func(txn *Transaction) { written.Insert(txn, 3, "def", nil) })
	second := EncodeStateAsUpdateV1(writer, sv)
	sv = writer.StateVector()
	writer.Transact(func(txn *Transaction) {
		written.Delete(txn, 4, 1)
		written.Delete(txn, 1, 1)
	})
	deletes := EncodeStateAsUpdateV1(writer, sv)
	other := New(WithClientID(11))
	otherText := other.GetText("u")
	other.Transact(func(txn *Transaction) { otherText.Insert(txn, 0, "x", nil) })
	unrelated := EncodeStateAsUpdateV1(other, nil)

	for _, format := range applyFormats {
		for _, kind := range []struct {
			name string
			opts []DocOption
		}{
			{"unbounded", nil},
			{"bounded", []DocOption{WithMaxIntegrateScan(testScanBound)}},
		} {
			t.Run(format.name+"/"+kind.name, func(t *testing.T) {
				doc := New(append([]DocOption{WithClientID(1)}, kind.opts...)...)
				text := doc.GetText("t")
				for _, step := range []struct {
					what   string
					update []byte
					parked int
					text   string
				}{
					{"the deletes, before their structs", deletes, 2, ""},
					{"an update with nothing of client 98", unrelated, 2, ""},
					{"the first half of the structs", first, 1, "ac"},
					{"the rest of the structs", second, 0, "acdf"},
				} {
					if err := applyIn(t, doc, format.v2, step.update); err != nil {
						t.Fatalf("%s: %v", step.what, err)
					}
					if got := doc.PendingStats().DeleteRanges; got != step.parked {
						t.Fatalf("after %s %d delete ranges are parked, want %d", step.what, got, step.parked)
					}
					if got := text.ToString(); got != step.text {
						t.Fatalf("after %s the text is %q, want %q", step.what, got, step.text)
					}
				}

				inOrder := New(WithClientID(2))
				for _, update := range [][]byte{first, second, deletes, unrelated} {
					if err := ApplyUpdateV1(inOrder, update, nil); err != nil {
						t.Fatal(err)
					}
				}
				if got, want := text.ToString(), inOrder.GetText("t").ToString(); got != want {
					t.Fatalf("the text is %q, the in-order peer's %q", got, want)
				}
				if got, want := doc.StateVector(), inOrder.StateVector(); len(got) != len(want) || got.Clock(98) != want.Clock(98) || got.Clock(11) != want.Clock(11) {
					t.Fatalf("the state vector is %v, the in-order peer's %v", got, want)
				}
				copied := New(WithClientID(3))
				if err := ApplyUpdateV1(copied, EncodeStateAsUpdateV1(doc, nil), nil); err != nil {
					t.Fatal(err)
				}
				if got := copied.GetText("t").ToString(); got != "acdf" {
					t.Fatalf("the document's state decodes to the text %q, want %q", got, "acdf")
				}
			})
		}
	}
}

// A bounded document refuses an update whose delete set both deletes text it
// holds and would park more ranges than the cap, and takes back the deletes it
// had already applied: its state, its text and its observers are as they were.
// An unbounded twin, which keeps what a refused update did, shows the deletes
// ran before the refusal. The same deletes without the ranges past the cap
// then apply.
func TestABoundedDocumentRefusingParkedDeletesTakesBackTheDeletesItApplied(t *testing.T) {
	const limit = 10
	writer := New(WithClientID(5))
	written := writer.GetText("t")
	writer.Transact(func(txn *Transaction) { written.Insert(txn, 0, "hello", nil) })
	state := EncodeStateAsUpdateV1(writer, nil)
	over := make([]DeleteRange, 0, limit+1)
	for i := range limit + 1 {
		over = append(over, DeleteRange{Clock: uint64(2 * i), Len: 1})
	}
	deletesText := []DeleteRange{{Clock: 1, Len: 2}}
	refused := deleteSetUpdate(map[ClientID][]DeleteRange{5: deletesText, 98: over})
	for _, format := range applyFormats {
		t.Run(format.name, func(t *testing.T) {
			twin := New(WithClientID(2), WithMaxPendingItems(limit))
			if err := ApplyUpdateV1(twin, state, nil); err != nil {
				t.Fatal(err)
			}
			if err := applyIn(t, twin, format.v2, refused); !errors.Is(err, ErrInvalidUpdate) {
				t.Fatalf("the unbounded twin applied the update with err %v, want ErrInvalidUpdate", err)
			}
			if got := twin.GetText("t").ToString(); got != "hlo" {
				t.Fatalf("the unbounded twin's text is %q after the refusal, want %q: its deletes ran first", got, "hlo")
			}

			doc := New(WithClientID(1), WithMaxPendingItems(limit), WithMaxIntegrateScan(testScanBound))
			if err := ApplyUpdateV1(doc, state, nil); err != nil {
				t.Fatal(err)
			}
			text := doc.GetText("t")
			before, textBefore := EncodeStateAsUpdateV1(doc, nil), text.ToString()
			updates := observed(doc)
			err := applyIn(t, doc, format.v2, refused)
			if !errors.Is(err, ErrInvalidUpdate) {
				t.Fatalf("the update was applied with err %v, want ErrInvalidUpdate", err)
			}
			requireUnchanged(t, doc, before, textBefore, updates)
			if got := doc.PendingStats().DeleteRanges; got != 0 {
				t.Fatalf("after the refusal %d delete ranges are parked, want none", got)
			}
			if err := applyIn(t, doc, format.v2, deleteRangesUpdate(5, deletesText...)); err != nil {
				t.Fatalf("the deletes alone were refused: %v", err)
			}
			if got := text.ToString(); got != "hlo" {
				t.Fatalf("after the deletes alone the text is %q, want %q", got, "hlo")
			}
		})
	}
}
