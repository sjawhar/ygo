package crdt

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// Tests for what a refused split leaves behind, for the splits a drain of the
// pending queue makes, and for the permissions a parked write keeps.

// applyEither applies update to doc as V1 or as its V2 re-encoding, so one
// table row covers both decoders.
func applyEither(t *testing.T, doc *Doc, update []byte, v2 bool) error {
	t.Helper()
	if !v2 {
		return ApplyUpdateV1(doc, update, nil)
	}
	asV2, err := UpdateV1ToV2(update)
	if err != nil {
		t.Fatalf("re-encode the update as V2: %v", err)
	}
	return ApplyUpdateV2(doc, asV2, nil)
}

// clockDeleteUpdate is an update of no structs whose delete set deletes
// length clocks of client from clock. It is built at the clock level on
// purpose: YText.Delete deletes whole characters and so never asks for a
// split inside one, while a delete set off the wire names clocks and can.
func clockDeleteUpdate(client ClientID, clock, length uint64) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(0)              // no structs
	enc.WriteVarUint(1)              // one client in the delete set
	enc.WriteVarUint(uint64(client)) // the client that wrote the text
	enc.WriteVarUint(1)              // one range
	enc.WriteVarUint(clock)
	enc.WriteVarUint(length)
	return bytes.Clone(enc.Bytes())
}

// docBytes is doc's whole state as a V1 update: what a refused apply must
// leave byte for byte as it was.
func docBytes(doc *Doc) string {
	return string(EncodeStateAsUpdateV1(doc, nil))
}

// A refused split leaves the document exactly as it was: the same text, the
// same structs, the same bytes.
//
// The emoji is the point. "A😀B" is one run of four UTF-16 clocks, and a
// delete set that names clock 2 asks for a split inside the surrogate pair,
// which no text API can ask for and a sender can. A refusal raised after the
// content had been cut left each half holding one half of the pair, and the
// rollback's merge of the halves put two U+FFFD where the emoji had been:
// refused, no observer fired, the state vector unchanged, so no later apply
// of the room's whole state could repair it and every reader saw the
// mojibake from then on.
//
// This is surface the Split budget created, not a defect the fork had: until
// Split existed countSplit only counted and could refuse nothing, so there
// was no refusal part way through a split to go wrong. What closes it is the
// position of the count, at the very top of splitItem (see the comment
// there).
func TestUnit_ApplyBudget_ARefusedSplitLeavesTheTextAndTheStoreExactlyAsTheyWere(t *testing.T) {
	const base = "A😀B" // four UTF-16 clocks: A, the pair, B
	for name, c := range map[string]struct {
		update []byte
		v2     bool
	}{
		"a delete inside the pair, V1": {update: clockDeleteUpdate(1, 2, 1)},
		"a delete inside the pair, V2": {update: clockDeleteUpdate(1, 2, 1), v2: true},
		"an insert inside the pair, V1": {
			update: gapStringUpdate(0, "X", &ID{Client: 1, Clock: 1}),
		},
		"an insert inside the pair, V2": {
			update: gapStringUpdate(0, "X", &ID{Client: 1, Clock: 1}), v2: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			doc := budgetDoc(t, base)
			if got := doc.GetText("t").ToString(); got != base {
				t.Fatalf("the base text reads %q, want %q", got, base)
			}
			before, beforeBytes := stateOf(doc), docBytes(doc)

			doc.SetApplyBudget(func(any) ApplyBudget {
				return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: 0}
			})
			requireRefused(t, doc, before, applyEither(t, doc, c.update, c.v2), ErrApplyBudget)
			if got := doc.GetText("t").ToString(); got != base {
				t.Fatalf("the refused update left %q, want the %q it found", got, base)
			}
			if got := docBytes(doc); got != beforeBytes {
				t.Fatalf("the refused update changed the document's encoded state: %d bytes, want the %d before it",
					len(got), len(beforeBytes))
			}
			if got := doc.StructCount(); got != 1 {
				t.Fatalf("the refused update left %d structs, want the one run it found", got)
			}

			// The control: the same update with the splits allowed applies,
			// so what the refusal above stopped is a split that would have
			// happened.
			open := budgetDoc(t, base)
			open.SetApplyBudget(func(any) ApplyBudget {
				return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: -1}
			})
			if err := applyEither(t, open, c.update, c.v2); err != nil {
				t.Fatalf("the same update with splits allowed was refused: %v", err)
			}
			if open.StructCount() == 1 {
				t.Fatal("the allowed update split nothing, so the refusal above proves nothing")
			}
		})
	}
}

// A refused split adds no struct to a document, however many times it is
// refused, including when the item it would have split carries an undo
// link.
//
// The link is what made it visible. A refusal raised once the right half was
// built left that half in the store: the halves are merged back at commit
// only when they match, and an item whose deletion has been undone carries a
// redone ID that the half does not, so a sender refused again and again grew
// the document a struct at a time - 2, then 3, then 4 - under a budget whose
// whole purpose is to stop exactly that.
func TestUnit_ApplyBudget_ARefusedSplitOfARedoneItemAddsNoStruct(t *testing.T) {
	const base = "hello world"
	doc := budgetDoc(t, base)
	// The run carries an undo link, as one whose deletion was undone does.
	doc.store.clients[1][0].redone = &ID{Client: 99, Clock: 0}
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: 0}
	})

	before, beforeBytes := stateOf(doc), docBytes(doc)
	for attempt := 1; attempt <= 3; attempt++ {
		err := ApplyUpdateV1(doc, clockDeleteUpdate(1, 3, 5), nil)
		if !errors.Is(err, ErrApplyBudget) {
			t.Fatalf("attempt %d answered %v, want the split budget's refusal", attempt, err)
		}
		if got := doc.StructCount(); got != before.structs {
			t.Fatalf("after %d refusals the document holds %d structs, want the %d it started with",
				attempt, got, before.structs)
		}
		if got := docBytes(doc); got != beforeBytes {
			t.Fatalf("after %d refusals the document's encoded state changed", attempt)
		}
	}
}

// parkedSplitters is an update of n one-character structs for client 2 at
// clocks 1 to n, each pinned between two neighbouring clocks of the run
// client 1 wrote: clock 0 of client 2 is never sent, so every one of them
// parks, and the drain that lands them cuts that run into n+1 pieces that
// stay cut, since each has one of these characters between it and the next.
func parkedSplitters(n int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(uint64(n))
	enc.WriteVarUint(2) // client
	enc.WriteVarUint(1) // first clock: clock 0 is the gap
	for i := range n {
		enc.WriteUint8(wireString | flagHasOrigin | flagHasRightOrigin)
		enc.WriteVarUint(1)         // the left origin's client: the run's writer
		enc.WriteVarUint(uint64(i)) // a different clock inside the run for each
		enc.WriteVarUint(1)         // the right origin's client
		enc.WriteVarUint(uint64(i + 1))
		enc.WriteVarString("x")
	}
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// storeBytes is doc's stored structs and delete set as a V1 update, without
// the structs waiting in its pending queue, which the whole-state encode
// appends (withParked). It is how a test says "the store gained exactly
// this and nothing else" while a backlog sits in the queue.
func storeBytes(doc *Doc) string {
	doc.mu.Lock()
	defer doc.mu.Unlock()
	return string(encodeV1Locked(doc, nil, buildDeleteSet(doc.store)))
}

// The splits a drain of the pending queue makes are charged to the apply
// that lands them, so Structs and Split together bound what one apply adds
// - and the apply itself is not refused for them.
//
// A parked struct is charged in full to the apply that brought it, so the
// drain charges it nothing. A split half is nobody's until the drain makes
// it: it is cut from an item the document already held, at the moment the
// parked struct lands, and nothing charged it at park. Uncharged, ten
// parked one-character inserts landing in one run left the document ten
// structs larger under a budget of Structs: 1, Bytes: 1 and Split: 0 - the
// count the budget reports and the structs the document gains no longer the
// same thing.
//
// What the charge refuses is the drain. The update that filled the gap
// commits with what it carried itself: the backlog whose splits passed the
// budget is another sender's, which the one that filled the gap can neither
// see nor send less of (drainParked).
func TestUnit_ApplyBudget_ADrainsSplitsAreChargedToTheApplyThatLandsThem(t *testing.T) {
	const base, n = "hello world", 10 // ten interior clocks of the run
	for name, v2 := range map[string]bool{"V1": false, "V2": true} {
		t.Run(name, func(t *testing.T) {
			fill := gapStringUpdate(0, "A", nil)

			// What the store holds once the fill alone has been applied,
			// with no backlog to drain and nothing refusing anything: the
			// document the test below must end with, parked backlog aside.
			plain := budgetDoc(t, base)
			if err := applyEither(t, plain, fill, v2); err != nil {
				t.Fatalf("the fill on a document with no backlog was refused: %v", err)
			}
			wantStore := storeBytes(plain)

			doc := budgetDoc(t, base)
			if err := ApplyUpdateV1(doc, parkedSplitters(n), nil); err != nil {
				t.Fatalf("the parking update was refused: %v", err)
			}
			if got := doc.PendingStats().Items; got != n {
				t.Fatalf("%d structs parked, want the %d the update carried", got, n)
			}
			beforeQueue := parkedShape(doc)

			// One split short of what the drain needs: the drain is taken
			// back and the fill stands.
			doc.SetApplyBudget(func(any) ApplyBudget {
				return ApplyBudget{Decoded: -1, Structs: 1, Bytes: 1, Split: n - 1}
			})
			if err := applyEither(t, doc, fill, v2); err != nil {
				t.Fatalf("the fill was refused for a drain that is not its own: %v", err)
			}
			if got := doc.PendingStats().Items; got != n {
				t.Fatalf("%d structs are parked, want the %d the taken-back drain left", got, n)
			}
			if got := parkedShape(doc); got != beforeQueue {
				t.Fatalf("the taken-back drain left the queue as\n%s\nwant\n%s", got, beforeQueue)
			}
			if got := storeBytes(doc); got != wantStore {
				t.Fatalf("the store gained more than the fill's own structs: %d bytes, want %d",
					len(got), len(wantStore))
			}
			if got := doc.StructCount(); got != 2 {
				t.Fatalf("the store holds %d structs, want the run it found and the one the fill carried", got)
			}

			// Exactly what the drain needs: it lands every parked struct,
			// so n is the split count the charge is counting.
			open := budgetDoc(t, base)
			if err := ApplyUpdateV1(open, parkedSplitters(n), nil); err != nil {
				t.Fatal(err)
			}
			open.SetApplyBudget(func(any) ApplyBudget {
				return ApplyBudget{Decoded: -1, Structs: 1, Bytes: 1, Split: n}
			})
			if err := applyEither(t, open, fill, v2); err != nil {
				t.Fatalf("the fill with the drain's splits allowed was refused: %v", err)
			}
			if got := open.PendingStats().Items; got != 0 {
				t.Fatalf("%d structs are still parked after the fill", got)
			}
			// The run is now n+1 pieces, one of these characters between
			// each pair, so none of the halves merges back: the document
			// holds the n+1 pieces, the fill and the n it landed. The pair
			// of budgets above is what pins the split count at exactly n.
			if got, want := open.StructCount(), (n+1)+1+n; got != want {
				t.Fatalf("the fill left %d structs, want the %d the run was cut into, the fill and the %d it landed",
					got, want, n)
			}
		})
	}
}

// parkedRunAndSplitter is an update of two structs for client 2 behind the
// gap at clock 0: "BCD" at clocks 1 to 3, written at the head of the root
// text, and one character at clock 4 written beside clock 1 of it. Filling
// the gap drains both, and the second cuts the first in two.
func parkedRunAndSplitter() []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(2) // two structs
	enc.WriteVarUint(2) // client
	enc.WriteVarUint(1) // first clock: clock 0 is the gap
	enc.WriteUint8(wireString | flagHasOrigin)
	enc.WriteVarUint(2) // left origin: its own client
	enc.WriteVarUint(0) // at clock 0, the gap
	enc.WriteVarString("BCD")
	enc.WriteUint8(wireString | flagHasOrigin)
	enc.WriteVarUint(2) // left origin: the run above
	enc.WriteVarUint(1) // at its first clock, so the split falls inside it
	enc.WriteVarString("X")
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// parkedShape is what the pending queue holds, as a refused apply must leave
// it: each struct's ID and the content it carries, in queue order.
func parkedShape(doc *Doc) string {
	var b strings.Builder
	if p := doc.store.pending; p != nil {
		for _, item := range p.items {
			if item == nil {
				continue
			}
			fmt.Fprintf(&b, "%v=%v;", item.ID, item.Content)
		}
	}
	return b.String()
}

// fillUnderRoot is a gap filler of one struct for client 2 at clock 0 under
// the named root: the clock a parked backlog of that client waits on, with
// content of its own under a root the test chooses.
func fillUnderRoot(root, value string) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(2) // client
	enc.WriteVarUint(0) // the clock the backlog waits on
	enc.WriteUint8(wireString)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteVarString(value)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// guardedParked is a document holding the two roots guardedState writes,
// with parkedRunAndSplitter's two structs waiting behind the gap at clock 0
// of client 2: a backlog whose drain splits, on a document whose
// allowed-roots list has a root to forbid.
func guardedParked(t *testing.T, base string) *Doc {
	t.Helper()
	doc := guardedDoc(t, base)
	if err := ApplyUpdateV1(doc, parkedRunAndSplitter(), nil); err != nil {
		t.Fatalf("the parking update was refused: %v", err)
	}
	if got := doc.PendingStats().Items; got != 2 {
		t.Fatalf("%d structs parked, want the two the update carried", got)
	}
	return doc
}

// A split refused inside a drain parks the item the drain had integrated
// back whole, and the two refusals a split can raise fall in two different
// places.
//
// This is where the rules meet. The drain integrates a parked item and then
// splits it for the next one, and the split is the first thing that can now
// refuse (countSplit). The take-back must give the integrated item back as
// it was - its content whole, not the first half of itself.
//
// Where the refusal lands then depends on whose split it was. A split the
// drain made refuses the drain alone: the backlog stays parked whole and
// the update that filled the gap commits with what it carried itself,
// because that content is another sender's and its sender can do nothing
// about it (drainParked). A split the update made itself refuses the
// update, as every other measure of its budget does, and leaves the
// document byte for byte as it was.
func TestUnit_ApplyBudget_ARefusedSplitInADrainParksTheItemBackWhole(t *testing.T) {
	const base = "hello world"
	fill := gapStringUpdate(0, "A", nil)
	// The same fill, written beside clock 5 of the run the document holds,
	// so the apply splits that run itself before its drain runs at all.
	fillAndSplit := gapStringUpdate(0, "A", &ID{Client: 1, Clock: 5})

	for name, v2 := range map[string]bool{"V1": false, "V2": true} {
		t.Run(name, func(t *testing.T) {
			parked := func() (*Doc, string, string) {
				doc := budgetDoc(t, base)
				if err := ApplyUpdateV1(doc, parkedRunAndSplitter(), nil); err != nil {
					t.Fatalf("the parking update was refused: %v", err)
				}
				if got := doc.PendingStats().Items; got != 2 {
					t.Fatalf("%d structs parked, want the two the update carried", got)
				}
				return doc, docBytes(doc), parkedShape(doc)
			}
			noSplits := func(any) ApplyBudget {
				return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: 0}
			}

			t.Run("the drain's own split", func(t *testing.T) {
				// What the store holds once the fill alone has landed.
				plain := budgetDoc(t, base)
				if err := applyEither(t, plain, fill, v2); err != nil {
					t.Fatal(err)
				}
				wantStore := storeBytes(plain)

				doc, _, beforeQueue := parked()
				doc.SetApplyBudget(noSplits)
				if err := applyEither(t, doc, fill, v2); err != nil {
					t.Fatalf("the fill was refused for a drain that is not its own: %v", err)
				}
				if got := parkedShape(doc); got != beforeQueue {
					t.Fatalf("the taken-back drain left the queue as\n%s\nwant\n%s", got, beforeQueue)
				}
				if got := storeBytes(doc); got != wantStore {
					t.Fatalf("the store gained more than the fill's own structs: %d bytes, want %d",
						len(got), len(wantStore))
				}
				if got := doc.GetText("t").ToString(); got != base+"A" {
					t.Fatalf("the document reads %q, want the run it found and the fill's own character", got)
				}
			})

			t.Run("the apply's own split", func(t *testing.T) {
				doc, beforeBytes, beforeQueue := parked()
				before := stateOf(doc)
				doc.SetApplyBudget(noSplits)
				requireRefused(t, doc, before, applyEither(t, doc, fillAndSplit, v2), ErrApplyBudget)
				if got := parkedShape(doc); got != beforeQueue {
					t.Fatalf("the refused apply left the queue as\n%s\nwant\n%s", got, beforeQueue)
				}
				if got := docBytes(doc); got != beforeBytes {
					t.Fatal("the refused apply changed the document's encoded state")
				}
			})

			// The two gates are separate, and only one of them moved. A
			// filler whose own content writes under a root its list
			// forbids is still refused, while one whose own content is
			// allowed lands even though the drain it starts is refused.
			t.Run("the filler's own root is still checked", func(t *testing.T) {
				onlyText := func(any) []string { return []string{"t"} }

				legal := guardedParked(t, base)
				legal.SetAllowedRoots(onlyText)
				legal.SetApplyBudget(noSplits)
				legalQueue := parkedShape(legal)
				if err := applyEither(t, legal, fill, v2); err != nil {
					t.Fatalf("a filler whose own content is allowed was refused: %v", err)
				}
				if got := parkedShape(legal); got != legalQueue {
					t.Fatalf("the taken-back drain left the queue as\n%s\nwant\n%s", got, legalQueue)
				}

				forbidden := guardedParked(t, base)
				forbidden.SetAllowedRoots(onlyText)
				forbidden.SetApplyBudget(noSplits)
				before, beforeBytes := stateOf(forbidden), docBytes(forbidden)
				refused := requireRefused(t, forbidden, before,
					applyEither(t, forbidden, fillUnderRoot("m", "A"), v2), ErrRootNotAllowed)
				if refused.Root != "m" {
					t.Fatalf("the refusal reports root %q, want \"m\"", refused.Root)
				}
				if got := docBytes(forbidden); got != beforeBytes {
					t.Fatal("the refused filler changed the document's encoded state")
				}
			})

			// The control: the same fill with the split allowed lands both
			// parked structs, so the drain above really did integrate one
			// and split it.
			open, _, _ := parked()
			if err := applyEither(t, open, fill, v2); err != nil {
				t.Fatalf("the fill with splits allowed was refused: %v", err)
			}
			if got := open.PendingStats().Items; got != 0 {
				t.Fatalf("%d structs are still parked after the fill", got)
			}
			if got := open.GetText("t").ToString(); !strings.Contains(got, "BXCD") {
				t.Fatalf("the drained text reads %q, want the run split by the character beside it", got)
			}
		})
	}
}

// privateContainerUpdate is an update of one struct for client 5: a YText
// container under the named root, which a trusted peer brings.
func privateContainerUpdate(root string) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(5) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireType)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteUint8(2) // YText
	enc.WriteVarUint(0)
	return bytes.Clone(enc.Bytes())
}

// forgedChildUpdate is an update of one struct for client 2 written inside
// the container item of privateContainerUpdate, by its ID. It names no root
// on the wire, so no check in the decode sees one, and while the container
// has not arrived the struct has no parent to read a root off either: it
// parks with nothing about it to judge.
func forgedChildUpdate(value string) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(2) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireString)
	enc.WriteUint8(0)   // parent info: the container's ID follows
	enc.WriteVarUint(5) // the container's client
	enc.WriteVarUint(0) // its clock
	enc.WriteVarString(value)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// A parked write is judged by the allowed-roots list of the apply that
// parked it, not by the list of whatever apply later supplies its
// container.
//
// A struct whose container has not arrived has no parent, and the check at
// park time can read no root off it. Judged only when the drain integrates
// it, it borrowed the permissions of the update that filled the gap: a
// sender allowed nothing but "public" parked a child of a container under
// "private" and waited for a trusted peer to bring that container, and the
// drain, running under the trusted peer's list, stored the forged write.
// The list of the parking apply travels with the struct and must hold too
// (notePark, checkParkedRoot).
func TestUnit_AllowedRoots_AParkedWriteKeepsItsOwnSendersPermissions(t *testing.T) {
	const base = "hello world"
	for name, v2 := range map[string]bool{"V1": false, "V2": true} {
		t.Run(name, func(t *testing.T) {
			doc := budgetDoc(t, base)
			doc.GetText("public") // the one root this sender may write under

			doc.SetAllowedRoots(func(any) []string { return []string{"public"} })
			if err := applyEither(t, doc, forgedChildUpdate("forged"), v2); err != nil {
				t.Fatalf("the parking write was refused at park: %v", err)
			}
			if got := doc.PendingStats().Items; got != 1 {
				t.Fatalf("%d structs parked, want the one the sender wrote", got)
			}

			// A trusted peer brings the container, under a list that allows
			// the private root. The drain must not take the forged write
			// with it.
			doc.SetAllowedRoots(func(any) []string { return []string{"public", "private"} })
			if err := applyEither(t, doc, privateContainerUpdate("private"), v2); err != nil {
				t.Fatalf("the trusted container was refused: %v", err)
			}
			if got := len(doc.store.clients[2]); got != 0 {
				t.Fatalf("the drain stored %d structs of the sender that may not write under \"private\"", got)
			}
			if got := doc.PendingStats().Items; got != 0 {
				t.Fatalf("%d structs are still parked, want the forged one dropped", got)
			}

			// The control their reviewer ran: with the container already
			// there, the same write is refused outright, because its parent
			// resolves in the decode and the containment check sees it.
			held := budgetDoc(t, base)
			held.GetText("public")
			if err := applyEither(t, held, privateContainerUpdate("private"), v2); err != nil {
				t.Fatal(err)
			}
			held.SetAllowedRoots(func(any) []string { return []string{"public"} })
			heldBefore := stateOf(held)
			requireRefused(t, held, heldBefore,
				applyEither(t, held, forgedChildUpdate("forged"), v2), ErrRootNotAllowed)

			// The second control: a write parked by a sender the list does
			// allow lands, so what the drain refuses is the permission and
			// not the parking.
			open := budgetDoc(t, base)
			open.GetText("public")
			open.SetAllowedRoots(func(any) []string { return []string{"public", "private"} })
			if err := applyEither(t, open, forgedChildUpdate("allowed"), v2); err != nil {
				t.Fatal(err)
			}
			if err := applyEither(t, open, privateContainerUpdate("private"), v2); err != nil {
				t.Fatal(err)
			}
			if got := len(open.store.clients[2]); got != 1 {
				t.Fatalf("the allowed sender's parked write left %d structs, want the one it wrote", got)
			}
		})
	}
}

// namedRootInsert is an update of one string struct for client 9 under the
// named root, which the apply creates when the document does not hold it.
func namedRootInsert(root, value string) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireString)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteVarString(value)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// Bytes counts the name of a root the update creates. It is a string the
// sender sized and the document keeps for as long as it holds the root, as
// a map key and an XML node name are, and it was the one such string no
// count saw: Bytes: 1 admitted and retained a root named with a megabyte
// while a map key of that size was refused. An allowed-roots list keeps such
// a name out too, when the owner sets one, but a budget alone has to bound
// it.
func TestUnit_ApplyBudget_BytesCountsARootNameTheUpdateCreates(t *testing.T) {
	const base, value = "hello world", "x"
	root := strings.Repeat("r", 1_000_000)
	update := namedRootInsert(root, value)
	want := len(root) + len(value)

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: want - 1, Split: -1}
	})
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)
	if refused.Bytes != want {
		t.Fatalf("the refusal reports %d bytes, want the %d of the root name and its one character", refused.Bytes, want)
	}
	if _, held := doc.share[root]; held {
		t.Fatal("the refused update left the root it named on the document")
	}

	fits := budgetDoc(t, base)
	fits.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: want, Split: -1}
	})
	requireApplied(t, fits, stateOf(fits), ApplyUpdateV1(fits, update, nil))
	if _, held := fits.share[root]; !held {
		t.Fatal("the applied update did not leave the root it named, so the name costs nothing")
	}

	// A root the document already holds is charged nothing: one update that
	// names it in struct after struct pays for it once.
	again := budgetDoc(t, base)
	again.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: len(value), Split: -1}
	})
	if err := ApplyUpdateV1(again, namedRootInsert("t", value), nil); err != nil {
		t.Fatalf("a write under the root the document already holds was refused: %v", err)
	}
}

// gcStructUpdate is an update of one GC struct for client at clock: a
// placeholder of length clocks whose content is gone, which is what the wire
// carries for collected content.
func gcStructUpdate(client ClientID, clock, length uint64) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(uint64(client))
	enc.WriteVarUint(clock)
	enc.WriteUint8(0) // a GC struct: a length and nothing else
	enc.WriteVarUint(length)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// parkedClocks is how many clocks the pending queue holds.
func parkedClocks(doc *Doc) uint64 {
	n := uint64(0)
	if p := doc.store.pending; p != nil {
		for _, item := range p.items {
			if item != nil {
				n += item.Content.Len()
			}
		}
	}
	return n
}

// A deleted range a park adds to a GC struct the queue already held costs no
// bytes. The grown clocks are a range the document holds as a range record, a
// pair of integers whatever span they name, not content; a delete's cost is
// the number of ranges it carries, not their length. The clocks are compact,
// so there is no heap an owner can be driven out of, and a budget of Bytes: 0
// admits the growth.
func TestUnit_ApplyBudget_ADeletedRangeAParkGrowsCostsNoBytes(t *testing.T) {
	const grows = 1_000_000
	park := func() *Doc {
		doc := New(WithClientID(7))
		if err := ApplyUpdateV1(doc, gcStructUpdate(9, 1, 1), nil); err != nil {
			t.Fatalf("the first GC struct was refused: %v", err)
		}
		if got := doc.PendingStats().Items; got != 1 {
			t.Fatalf("%d structs parked, want the one GC struct behind the gap at clock 0", got)
		}
		return doc
	}

	// Bytes: 0 no longer refuses the growth: a grown deleted range adds no
	// content, only a wider range record the document holds compactly.
	doc := park()
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: 0, Bytes: 0, Split: -1}
	})
	if err := ApplyUpdateV1(doc, gcStructUpdate(9, 2, grows), nil); err != nil {
		t.Fatalf("a growth of %d clocks was refused under Bytes: 0, but a deleted range is not content: %v", grows, err)
	}
	if got := doc.PendingStats().Items; got != 1 {
		t.Fatalf("the queue holds %d structs, want the one the arrival merged into", got)
	}
	if got := parkedClocks(doc); got != 1+grows {
		t.Fatalf("the queue holds %d clocks, want the %d the merge grew it to", got, 1+grows)
	}

	// The control, so this test is an instrument and not a bare assertion:
	// the arriving GC struct is still bounded, by Decoded, and a budget that
	// refuses it does. This fires only when the tally is live, so it proves
	// the Bytes: 0 pass above is "not charged" rather than "not enforced".
	bounded := park()
	bounded.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: 0, Structs: -1, Bytes: -1, Split: -1}
	})
	before := stateOf(bounded)
	requireRefused(t, bounded, before, ApplyUpdateV1(bounded, gcStructUpdate(9, 2, grows), nil), ErrApplyBudget)
	if got := parkedClocks(bounded); got != 1 {
		t.Fatalf("the refused growth left %d clocks in the queue, want the one it found", got)
	}
}
