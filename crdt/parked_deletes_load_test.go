package crdt

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// A stored state holding more parked delete ranges than the cap loads into a
// document whose cap is set once the state is in (SetMaxPendingItems), as a
// room loads its stored state; a document capped first refuses it, so a room
// loaded that way would never load. After the load, an update that adds no
// range applies, and one that adds a range is refused.
func TestAStoredStateOverTheCapLoadsWhenTheCapIsSetAfterIt(t *testing.T) {
	const limit = 10
	stored := New(WithClientID(1), WithMaxPendingItems(math.MaxInt))
	if err := ApplyUpdateV1(stored, parkedDeletes(98, limit+5), nil); err != nil {
		t.Fatal(err)
	}
	state := EncodeStateAsUpdateV1(stored, nil)
	if err := ApplyUpdateV1(New(WithMaxPendingItems(limit)), state, nil); !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("a document capped at %d loaded the state with err %v, want ErrInvalidUpdate", limit, err)
	}

	doc := New(WithClientID(2), WithMaxPendingItems(math.MaxInt))
	if err := ApplyUpdateV1(doc, state, nil); err != nil {
		t.Fatal(err)
	}
	doc.SetMaxPendingItems(limit)
	if got := doc.PendingStats().DeleteRanges; got != limit+5 {
		t.Fatalf("the loaded document parks %d delete ranges, want %d", got, limit+5)
	}
	if err := ApplyUpdateV1(doc, state, nil); err != nil {
		t.Fatalf("the state sent again, which adds no range, was refused: %v", err)
	}
	if err := ApplyUpdateV1(doc, parkedDeletes(97, 1), nil); !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("an update adding a range past the cap was applied with err %v, want ErrInvalidUpdate", err)
	}
	if got := doc.PendingStats().DeleteRanges; got != limit+5 {
		t.Fatalf("the document parks %d delete ranges, want %d", got, limit+5)
	}
}

// A document that loaded more parked items than its cap, with the cap set
// after the load, still applies an update whose items wait on each other
// within it and all resolve, as a multi-client sync does: the pending budget
// starts at none rather than below it. An update that would park another item
// is still refused.
func TestAStoredStateOverTheItemCapStillAppliesWhatResolvesWithinAnUpdate(t *testing.T) {
	const limit, n, m = 10, 15, 8
	stored := New(WithClientID(1000), WithMaxPendingItems(math.MaxInt))
	if err := ApplyUpdateV1(stored, parkedBehindAGap(n), nil); err != nil {
		t.Fatal(err)
	}
	state := EncodeStateAsUpdateV1(stored, nil)

	// Client 77's clock 1, behind a gap at clock 0: it would park.
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1)
	enc.WriteVarUint(1)
	enc.WriteVarUint(77)
	enc.WriteVarUint(1)
	enc.WriteUint8(wireString | flagHasOrigin)
	enc.WriteVarUint(5)
	enc.WriteVarUint(0)
	enc.WriteVarString("x")
	enc.WriteVarUint(0)
	parksAnother := enc.Bytes()

	for _, version := range []int{1, 2} {
		apply := ApplyUpdateV1
		if version == 2 {
			apply = ApplyUpdateV2
		}
		doc := New(WithClientID(1001), WithMaxPendingItems(math.MaxInt))
		if err := ApplyUpdateV1(doc, state, nil); err != nil {
			t.Fatal(err)
		}
		doc.SetMaxPendingItems(limit)
		if got := doc.PendingStats().Items; got != n {
			t.Fatalf("V%d: the loaded document parks %d items, want %d", version, got, n)
		}
		// Each client's one item names the next group's (V1) or the previous
		// group's (V2) item, so every item but one waits within the update.
		if err := apply(doc, pendingReverseChain(version, m), nil); err != nil {
			t.Fatalf("V%d: an update whose items resolve within it was refused: %v", version, err)
		}
		if got, want := doc.GetText("text").ToString(), strings.Repeat("x", m); got != want {
			t.Fatalf("V%d: the text is %q, want %q", version, got, want)
		}
		if got := doc.PendingStats().Items; got != n {
			t.Fatalf("V%d: after the update %d items are parked, want %d", version, got, n)
		}
		if err := ApplyUpdateV1(doc, parksAnother, nil); !errors.Is(err, ErrInvalidUpdate) {
			t.Fatalf("V%d: an update parking another item was applied with err %v, want ErrInvalidUpdate", version, err)
		}
		if got := doc.PendingStats().Items; got != n {
			t.Fatalf("V%d: after the refusal %d items are parked, want %d", version, got, n)
		}
	}
}
