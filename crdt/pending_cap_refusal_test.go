package crdt

import (
	"errors"
	"math"
	"math/rand/v2"
	"strconv"
	"testing"
)

// parkingEdit is an update of client 2 that inserts text into "t" and sets n
// keys on a map client 1 created, which the receiver never got, so every
// entry parks waiting for its parent while the text integrates. author is the
// state the receiver lacks.
func parkingEdit(t *testing.T, n int) (update, author []byte) {
	t.Helper()
	creator := New(WithClientID(1))
	root := creator.GetMap("m")
	creator.Transact(func(txn *Transaction) { root.Set(txn, "nested", NewMapPrelim()) })
	author = EncodeStateAsUpdateV1(creator, nil)
	editor := New(WithClientID(2))
	if err := ApplyUpdateV1(editor, author, nil); err != nil {
		t.Fatal(err)
	}
	v, _ := editor.GetMap("m").Get("nested")
	nested, ok := v.(*YMap)
	if !ok {
		t.Fatalf("nested is %T", v)
	}
	text := editor.GetText("t")
	editor.Transact(func(txn *Transaction) {
		text.Insert(txn, 0, "hello", nil)
		for i := range n {
			nested.Set(txn, "k"+strconv.Itoa(i), i)
		}
	})
	return EncodeStateAsUpdateV1(editor, creator.StateVector()), author
}

// A document refuses an update that would park more items than the cap before
// it changes anything, with no scan bound as with one: an update that inserts
// text and parks five map entries under a parent the document lacks leaves a
// document capped at two as it was, with nothing parked and no observer told.
// Before, the refusal came after the text had integrated, and only a bounded
// document took it back: a document with no bound (Dispatch's) kept the text,
// fired OnUpdate, and returned ErrInvalidUpdate. The same edit applies once
// the cap has room for what it parks, and its entries integrate once their
// parent arrives.
func TestARefusedParkedItemUpdateLeavesTheDocumentUnchanged(t *testing.T) {
	const limit, parked = 2, 5
	update, author := parkingEdit(t, parked)
	for _, format := range applyFormats {
		for _, kind := range []struct {
			name string
			opts []DocOption
		}{
			{"unbounded", nil},
			{"bounded", []DocOption{WithMaxIntegrateScan(testScanBound)}},
		} {
			t.Run(format.name+"/"+kind.name, func(t *testing.T) {
				doc := New(kind.opts...)
				doc.SetMaxPendingItems(limit)
				before, text := EncodeStateAsUpdateV1(doc, nil), doc.GetText("t").ToString()
				updates := observed(doc)
				if err := applyIn(t, doc, format.v2, update); !errors.Is(err, ErrInvalidUpdate) {
					t.Fatalf("the update was applied with err %v, want ErrInvalidUpdate", err)
				}
				requireUnchanged(t, doc, before, text, updates)
				if got := doc.PendingStats().Items; got != 0 {
					t.Fatalf("after the refusal %d items are parked, want none", got)
				}
				doc.SetMaxPendingItems(parked)
				if err := applyIn(t, doc, format.v2, update); err != nil {
					t.Fatalf("with room for its %d items the update was refused: %v", parked, err)
				}
				if got := doc.GetText("t").ToString(); got != "hello" {
					t.Fatalf("the text is %q, want %q", got, "hello")
				}
				if got := doc.PendingStats().Items; got != parked {
					t.Fatalf("%d items are parked, want %d", got, parked)
				}
				if err := ApplyUpdateV1(doc, author, nil); err != nil {
					t.Fatal(err)
				}
				if got := doc.PendingStats().Items; got != 0 {
					t.Fatalf("once the parent arrived %d items are still parked", got)
				}
			})
		}
	}
}

// The cap on parked items refuses exactly the updates that would park more
// than it has room for, before they change anything. For random updates with
// gaps and cross-client dependencies (as in
// TestTheParkedDeletesCheckedAreWhatTheApplyParks), an update that parks p
// items applies on a document whose cap leaves room for p (and for the delete
// ranges it parks, which the same cap counts), and is refused on one whose cap
// leaves room for p-1, which it leaves unchanged. The second is tried where
// the update parks fewer than p delete ranges, so only its items pass the cap,
// and p is more than one, since a cap of zero is the default.
func TestTheParkedItemCapRefusesExactlyWhatWouldPassIt(t *testing.T) {
	const rounds = 300
	rng := rand.New(rand.NewPCG(338, 2526))
	refused := 0
	for round := range rounds {
		held, update := gappedUpdate(t, rng)
		for _, format := range applyFormats {
			load := func(limit int) *Doc {
				doc := New()
				if err := ApplyUpdateV1(doc, held, nil); err != nil {
					t.Fatal(err)
				}
				doc.SetMaxPendingItems(limit)
				return doc
			}
			free := load(math.MaxInt)
			if err := applyIn(t, free, format.v2, update); err != nil {
				t.Fatalf("round %d %s: with the cap lifted the update was refused: %v", round, format.name, err)
			}
			p, d := free.PendingStats().Items, free.PendingStats().DeleteRanges
			if p == 0 {
				continue
			}
			fits := load(max(p, d))
			if err := applyIn(t, fits, format.v2, update); err != nil {
				t.Fatalf("round %d %s: an update parking %d items and %d delete ranges was refused with room for %d: %v",
					round, format.name, p, d, max(p, d), err)
			}
			if string(EncodeStateAsUpdateV1(fits, nil)) != string(EncodeStateAsUpdateV1(free, nil)) {
				t.Fatalf("round %d %s: with room for its items the update applied differently", round, format.name)
			}
			if p == 1 || d >= p {
				continue
			}
			over := load(p - 1)
			before := EncodeStateAsUpdateV1(over, nil)
			updates := observed(over)
			if err := applyIn(t, over, format.v2, update); !errors.Is(err, ErrInvalidUpdate) {
				t.Fatalf("round %d %s: an update parking %d items applied with room for %d (err %v)", round, format.name, p, p-1, err)
			}
			if string(EncodeStateAsUpdateV1(over, nil)) != string(before) || *updates != 0 {
				t.Fatalf("round %d %s: the refused update changed the document or told its observers", round, format.name)
			}
			refused++
		}
	}
	if refused < rounds/4 {
		t.Fatalf("only %d of %d applies were refused a cap one short of the items they park", refused, 2*rounds)
	}
	t.Logf("%d of %d applies were refused a cap one short of the items they park", refused, 2*rounds)
}
