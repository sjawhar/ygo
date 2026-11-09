package crdt

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/reearth/ygo/encoding"
)

// baseState is the whole state of a document whose root "t" holds base,
// written by client 1 in one transaction, so every document and peer of these
// tests starts from the same structs.
func baseState(t *testing.T, base string) []byte {
	t.Helper()
	writer := New(WithClientID(1))
	text := writer.GetText("t")
	writer.Transact(func(txn *Transaction) { text.Insert(txn, 0, base, nil) })
	return EncodeStateAsUpdateV1(writer, nil)
}

// budgetDoc is a document holding base as the text of root "t", with no bound
// of any kind: a test sets one on it after it has loaded the state it trusts,
// as an owner that bounds only what arrives after that does.
func budgetDoc(t *testing.T, base string) *Doc {
	t.Helper()
	doc := New(WithClientID(7))
	if err := ApplyUpdateV1(doc, baseState(t, base), nil); err != nil {
		t.Fatalf("the base state was refused: %v", err)
	}
	return doc
}

// editUpdate is a V1 update of what edit writes on a peer holding base, so a
// document holding base takes it as a remote update of that edit alone.
func editUpdate(t *testing.T, base string, edit func(txn *Transaction, text *YText)) []byte {
	t.Helper()
	return editUpdateFrom(t, base, 2, edit)
}

// editUpdateFrom is editUpdate from the peer client, for a test that applies
// two such updates to one document: a second update from the same client would
// carry clocks the document already holds, which integrate nothing.
func editUpdateFrom(t *testing.T, base string, client ClientID, edit func(txn *Transaction, text *YText)) []byte {
	t.Helper()
	peer := New(WithClientID(client))
	if err := ApplyUpdateV1(peer, baseState(t, base), nil); err != nil {
		t.Fatalf("the peer refused the base state: %v", err)
	}
	sv := peer.StateVector()
	text := peer.GetText("t")
	peer.Transact(func(txn *Transaction) { edit(txn, text) })
	return EncodeStateAsUpdateV1(peer, sv)
}

// mapSetUpdate is an update of one struct that names root "m" on the wire: a
// key a peer set on a map root with nothing in it, which the decode resolves
// by name because the struct has no neighbour to inherit its parent from, so
// it is refused by the name check in the decode (refuseRootOnBoundedDoc)
// rather than by the containment check over what the apply adds
// (checkItemRoot).
func mapSetUpdate(t *testing.T, base string, client ClientID) []byte {
	t.Helper()
	peer := New(WithClientID(client))
	if err := ApplyUpdateV1(peer, baseState(t, base), nil); err != nil {
		t.Fatalf("the peer refused the base state: %v", err)
	}
	sv := peer.StateVector()
	m := peer.GetMap("m")
	peer.Transact(func(txn *Transaction) { m.Set(txn, "k", "v") })
	return EncodeStateAsUpdateV1(peer, sv)
}

// threeInserts is an update of three one-character items, written at places
// far enough apart in base that none of them merges with another.
func threeInserts(t *testing.T, base string) []byte {
	t.Helper()
	return editUpdate(t, base, func(txn *Transaction, text *YText) {
		text.Insert(txn, 0, "a", nil)
		text.Insert(txn, 4, "b", nil)
		text.Insert(txn, 8, "c", nil)
	})
}

// appliedText is the text a document holding base holds once update is applied
// to it with no bound at all: what a document that takes the same update under
// a budget must hold too.
func appliedText(t *testing.T, base string, update []byte) string {
	t.Helper()
	twin := budgetDoc(t, base)
	if err := ApplyUpdateV1(twin, update, nil); err != nil {
		t.Fatalf("the unbounded twin refused the update: %v", err)
	}
	return twin.GetText("t").ToString()
}

// docState is everything a refused apply must leave as it was: the state
// vector, the delete set, the pending queue (its parked structs and its
// parked delete ranges), the rendered text, the entry of the map root the
// allowed-roots tests guard, and the struct count.
type docState struct {
	sv            string
	deletes       string
	parked        int
	parkedDeletes int
	text          string
	entry         string
	structs       int
}

// stateOf reads doc's state. It is called outside any transaction, where
// nothing is writing the store. Reading the roots "t" and "m" creates them on
// a document that does not hold them, which adds no struct and leaves every
// field below as it was.
func stateOf(doc *Doc) docState {
	parked := 0
	if p := doc.store.pending; p != nil {
		for _, item := range p.items {
			if item != nil {
				parked++
			}
		}
	}
	deletes := buildDeleteSet(doc.store)
	entry, _ := doc.GetMap("m").Get("k")
	return docState{
		sv:            fmt.Sprint(doc.StateVector()),
		deletes:       fmt.Sprint(deletes.orderedClients()),
		parked:        parked,
		parkedDeletes: doc.store.pendingDsRanges,
		text:          doc.GetText("t").ToString(),
		entry:         fmt.Sprint(entry),
		structs:       doc.StructCount(),
	}
}

// requireRefused requires err to be the refusal a bounded document returns for
// sentinel, and doc to read as before had it: a refused update changes
// nothing, so the state vector, the delete set, the pending queue, the text
// and the struct count are all as they were.
func requireRefused(t *testing.T, doc *Doc, before docState, err error, sentinel error) *RefusedUpdateError {
	t.Helper()
	if !errors.Is(err, sentinel) || !errors.Is(err, ErrInvalidUpdate) {
		t.Fatalf("the update was applied with err %v, want %v", err, sentinel)
	}
	var refused *RefusedUpdateError
	if !errors.As(err, &refused) {
		t.Fatalf("the error is not a *RefusedUpdateError: %v", err)
	}
	if after := stateOf(doc); after != before {
		t.Fatalf("the refused update changed the document:\nbefore %+v\nafter  %+v", before, after)
	}
	return refused
}

// requireApplied requires update to have been applied and doc to have changed,
// so that a control proves the budget and not the update refused the others.
func requireApplied(t *testing.T, doc *Doc, before docState, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("the update was refused: %v", err)
	}
	if after := stateOf(doc); after == before {
		t.Fatal("the applied update changed nothing, so it is no control")
	}
}

// A document refuses a remote update that integrates more structs of its own
// than the apply's budget allows, and applies the same update when its budget
// holds it: the refusal is ErrApplyBudget inside a RefusedUpdateError
// reporting the structs the apply integrated and the splits it made, it tells
// no observer, and it leaves the state vector, the delete set, the pending
// queue and the rendered text as they were.
func TestUnit_ApplyBudget_RefusesAnUpdateThatIntegratesMoreStructsThanItAllows(t *testing.T) {
	const base = "hello world"
	update := threeInserts(t, base)

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: 2, Bytes: -1, Split: -1} })
	before := stateOf(doc)
	updates := observed(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)
	if refused.Structs != 3 {
		t.Fatalf("the refusal reports %d structs, want the 3 the update carries", refused.Structs)
	}
	if refused.Split == 0 {
		t.Fatal("the refusal reports no split, want the splits three inserts into one run make")
	}
	if *updates != 0 {
		t.Fatalf("the refused update was handed to %d observers", *updates)
	}

	fits := budgetDoc(t, base)
	fits.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: 3, Bytes: -1, Split: -1} })
	fitsBefore := stateOf(fits)
	requireApplied(t, fits, fitsBefore, ApplyUpdateV1(fits, update, nil))
	if got, want := fits.GetText("t").ToString(), appliedText(t, base, update); got != want {
		t.Fatalf("the applied update left %q, want %q", got, want)
	}
	if fits.StructCount() <= fitsBefore.structs {
		t.Fatalf("the applied update left %d structs, want more than the %d before it", fits.StructCount(), fitsBefore.structs)
	}
}

// A document refuses a remote update that decodes more values than the apply's
// budget allows, from inside the decode, before any of its structs integrate;
// a document whose Decoded holds the update applies it.
func TestUnit_ApplyBudget_RefusesAnUpdateThatDecodesMoreValuesThanItAllows(t *testing.T) {
	const base = "hello world"
	update := threeInserts(t, base)

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 2, Structs: -1, Bytes: -1, Split: -1} })
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)
	if refused.Decoded != 3 {
		t.Fatalf("the refusal reports %d decoded values, want the 3 it reached", refused.Decoded)
	}
	if refused.Structs != 0 {
		t.Fatalf("the refusal reports %d structs, want none: it was refused as it decoded", refused.Structs)
	}

	fits := budgetDoc(t, base)
	fits.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 3, Structs: -1, Bytes: -1, Split: -1} })
	requireApplied(t, fits, stateOf(fits), ApplyUpdateV1(fits, update, nil))

	// The delete ranges an update carries are values it decodes too, since
	// each of them can split a struct the document holds, and a split adds a
	// struct no budget bounds (countDecodedRanges).
	deleteOnly := editUpdate(t, base, func(txn *Transaction, text *YText) { text.Delete(txn, 3, 5) })
	ranges := budgetDoc(t, base)
	ranges.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 0, Structs: -1, Bytes: -1, Split: -1} })
	rangesBefore := stateOf(ranges)
	refusedRanges := requireRefused(t, ranges, rangesBefore, ApplyUpdateV1(ranges, deleteOnly, nil), ErrApplyBudget)
	if refusedRanges.Decoded != 1 {
		t.Fatalf("the refusal reports %d decoded values, want the one delete range it carries", refusedRanges.Decoded)
	}
	fitsRanges := budgetDoc(t, base)
	fitsRanges.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 1, Structs: -1, Bytes: -1, Split: -1} })
	requireApplied(t, fitsRanges, stateOf(fitsRanges), ApplyUpdateV1(fitsRanges, deleteOnly, nil))
}

// A document refuses a remote update that integrates more bytes of content
// than the apply's budget allows - the creep no count of structs can see,
// since one struct holds a whole paste - and takes the same update under a
// budget of exactly its bytes.
func TestUnit_ApplyBudget_RefusesAnUpdateThatIntegratesMoreBytesThanItAllows(t *testing.T) {
	const base, paste = "hello world", "pasted text"
	update := editUpdate(t, base, func(txn *Transaction, text *YText) {
		text.Insert(txn, 5, paste, nil)
	})

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: len(paste) - 1, Split: -1}
	})
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)
	if refused.Bytes != len(paste) {
		t.Fatalf("the refusal reports %d bytes, want the %d the paste carries", refused.Bytes, len(paste))
	}
	if refused.Structs != 1 {
		t.Fatalf("the refusal reports %d structs, want the one struct the paste is", refused.Structs)
	}

	fits := budgetDoc(t, base)
	fits.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: len(paste), Split: -1}
	})
	requireApplied(t, fits, stateOf(fits), ApplyUpdateV1(fits, update, nil))
}

// Structs: 0 is "this update may integrate no item of its own", and says it
// exactly: an update that only deletes a range of the text the document holds
// is applied, though the deletion splits the run it holds and so adds structs,
// and an update that integrates a single character is refused, its
// RefusedUpdateError reporting the one struct it integrated and the split the
// apply made.
func TestUnit_ApplyBudget_StructsZeroAdmitsADeleteAndRefusesOneCharacter(t *testing.T) {
	const base = "hello world"
	none := func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: 0, Bytes: -1, Split: -1} }
	deleteOnly := editUpdate(t, base, func(txn *Transaction, text *YText) { text.Delete(txn, 3, 5) })
	oneCharacter := editUpdate(t, base, func(txn *Transaction, text *YText) { text.Insert(txn, 3, "x", nil) })

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(none)
	before := stateOf(doc)
	requireApplied(t, doc, before, ApplyUpdateV1(doc, deleteOnly, nil))
	if got, want := doc.GetText("t").ToString(), appliedText(t, base, deleteOnly); got != want {
		t.Fatalf("the delete left %q, want %q", got, want)
	}
	if doc.StructCount() <= before.structs {
		t.Fatalf("the delete left %d structs, want more than the %d before it: its splits add structs",
			doc.StructCount(), before.structs)
	}

	insert := budgetDoc(t, base)
	insert.SetApplyBudget(none)
	insertBefore := stateOf(insert)
	refused := requireRefused(t, insert, insertBefore, ApplyUpdateV1(insert, oneCharacter, nil), ErrApplyBudget)
	if refused.Structs != 1 {
		t.Fatalf("the refusal reports %d structs, want the one character it integrated", refused.Structs)
	}
	if refused.Split != 1 {
		t.Fatalf("the refusal reports %d splits, want the one the insert into the run made", refused.Split)
	}
}

// Bytes: 0 is "this update may integrate no content byte": an update that only
// deletes held text is applied, and one that integrates a single character is
// refused.
func TestUnit_ApplyBudget_BytesZeroAdmitsADeleteAndRefusesOneCharacter(t *testing.T) {
	const base = "hello world"
	none := func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: -1, Bytes: 0, Split: -1} }
	deleteOnly := editUpdate(t, base, func(txn *Transaction, text *YText) { text.Delete(txn, 3, 5) })
	oneCharacter := editUpdate(t, base, func(txn *Transaction, text *YText) { text.Insert(txn, 3, "x", nil) })

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(none)
	requireApplied(t, doc, stateOf(doc), ApplyUpdateV1(doc, deleteOnly, nil))

	insert := budgetDoc(t, base)
	insert.SetApplyBudget(none)
	before := stateOf(insert)
	refused := requireRefused(t, insert, before, ApplyUpdateV1(insert, oneCharacter, nil), ErrApplyBudget)
	if refused.Bytes != 1 {
		t.Fatalf("the refusal reports %d bytes, want the one character's byte", refused.Bytes)
	}
}

// The zero value of ApplyBudget allows nothing, and a document with no budget
// callback is bounded in no measure: the same update is refused under the zero
// value and applied once the callback is removed.
func TestUnit_ApplyBudget_TheZeroValueAllowsNothingAndNoCallbackAllowsEverything(t *testing.T) {
	const base = "hello world"
	update := threeInserts(t, base)

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{} })
	before := stateOf(doc)
	requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)

	doc.SetApplyBudget(nil)
	requireApplied(t, doc, before, ApplyUpdateV1(doc, update, nil))
}

// A document with an allowed-roots list refuses a remote update naming a root
// outside it, reporting the name, and takes the same update when the list
// names it. A root the document already holds is subject to the list too: the
// list is what the owner allows the update to name, not what the document
// holds. A nil callback, and one answering nothing, allow every root.
func TestUnit_AllowedRoots_RefusesAnUpdateNamingARootTheDocumentDoesNotAllow(t *testing.T) {
	const base = "hello world"
	// A peer that writes into a second root, which the document does not hold.
	peer := New(WithClientID(2))
	if err := ApplyUpdateV1(peer, baseState(t, base), nil); err != nil {
		t.Fatal(err)
	}
	sv := peer.StateVector()
	other := peer.GetText("other")
	peer.Transact(func(txn *Transaction) { other.Insert(txn, 0, "elsewhere", nil) })
	update := EncodeStateAsUpdateV1(peer, sv)

	doc := budgetDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{"t"} })
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrRootNotAllowed)
	if refused.Root != "other" {
		t.Fatalf("the refusal reports root %q, want \"other\"", refused.Root)
	}
	if _, held := doc.share["other"]; held {
		t.Fatal("the refused update left the root it named in the document")
	}

	allowed := budgetDoc(t, base)
	allowed.SetAllowedRoots(func(any) []string { return []string{"t", "other"} })
	requireApplied(t, allowed, stateOf(allowed), ApplyUpdateV1(allowed, update, nil))
	if got, want := allowed.GetText("other").ToString(), "elsewhere"; got != want {
		t.Fatalf("the allowed root holds %q, want %q", got, want)
	}

	// A root the document holds already is refused when the list omits it;
	// an update that stays inside a root the list names is applied, whether
	// or not it names that root on the wire (checkItemRoot).
	held := budgetDoc(t, base)
	held.GetMap("m") // the document holds the root the update names
	held.SetAllowedRoots(func(any) []string { return []string{"t"} })
	heldBefore := stateOf(held)
	refusedHeld := requireRefused(t, held, heldBefore, ApplyUpdateV1(held, mapSetUpdate(t, base, 3), nil), ErrRootNotAllowed)
	if refusedHeld.Root != "m" {
		t.Fatalf("the refusal reports root %q, want \"m\"", refusedHeld.Root)
	}
	if err := ApplyUpdateV1(held, threeInserts(t, base), nil); err != nil {
		t.Fatalf("an update inside the allowed root was refused: %v", err)
	}

	for name, roots := range map[string]func(any) []string{
		"a nil callback": nil,
		"a nil answer":   func(any) []string { return nil },
		"an empty answer": func(any) []string {
			return []string{}
		},
	} {
		open := budgetDoc(t, base)
		open.SetAllowedRoots(roots)
		if err := ApplyUpdateV1(open, update, nil); err != nil {
			t.Fatalf("%s refused the update: %v", name, err)
		}
	}
}

// StructCount is the structs the document holds: the items of a document
// written by hand, the items an applied update adds, and, after a refused
// apply, exactly what it was.
func TestUnit_StructCount_CountsTheStructsTheDocumentHolds(t *testing.T) {
	if n := New().StructCount(); n != 0 {
		t.Fatalf("a new document holds %d structs, want none", n)
	}
	for _, l := range []int{1, 4, 25} {
		if n := chainedList(t, l).StructCount(); n != l {
			t.Fatalf("a document of %d hand-written items holds %d structs", l, n)
		}
	}

	const base = "hello world"
	doc := budgetDoc(t, base)
	if n := doc.StructCount(); n != 1 {
		t.Fatalf("the base state left %d structs, want the one run it is", n)
	}
	if err := ApplyUpdateV1(doc, threeInserts(t, base), nil); err != nil {
		t.Fatal(err)
	}
	applied := doc.StructCount()
	if applied < 4 {
		t.Fatalf("the applied update left %d structs, want at least the one held and the three it carries", applied)
	}

	doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: 0, Bytes: -1, Split: -1} })
	more := editUpdateFrom(t, base, 3, func(txn *Transaction, text *YText) { text.Insert(txn, 2, "zz", nil) })
	if err := ApplyUpdateV1(doc, more, nil); !errors.Is(err, ErrApplyBudget) {
		t.Fatalf("the update was applied with err %v", err)
	}
	if n := doc.StructCount(); n != applied {
		t.Fatalf("the refused update left %d structs, want the %d the document held", n, applied)
	}
}

// The V2 decoder is bounded exactly as the V1 decoder is: each of the three
// counts, and the allowed-roots list, refuses the V2 form of the same update,
// and a budget that holds it applies it.
func TestUnit_ApplyBudget_BoundsTheV2DecoderToo(t *testing.T) {
	const base = "hello world"
	v1 := threeInserts(t, base)
	v2, err := UpdateV1ToV2(v1)
	if err != nil {
		t.Fatal(err)
	}

	for name, budget := range map[string]ApplyBudget{
		"decoded": {Decoded: 2, Structs: -1, Bytes: -1, Split: -1},
		"structs": {Decoded: -1, Structs: 2, Bytes: -1, Split: -1},
		"bytes":   {Decoded: -1, Structs: -1, Bytes: 2, Split: -1},
		"split":   {Decoded: -1, Structs: -1, Bytes: -1, Split: 0},
	} {
		t.Run(name, func(t *testing.T) {
			doc := budgetDoc(t, base)
			doc.SetApplyBudget(func(any) ApplyBudget { return budget })
			before := stateOf(doc)
			requireRefused(t, doc, before, ApplyUpdateV2(doc, v2, nil), ErrApplyBudget)
		})
	}

	namesARoot, err := UpdateV1ToV2(mapSetUpdate(t, base, 3))
	if err != nil {
		t.Fatal(err)
	}
	roots := budgetDoc(t, base)
	roots.SetAllowedRoots(func(any) []string { return []string{"t"} })
	rootsBefore := stateOf(roots)
	refused := requireRefused(t, roots, rootsBefore, ApplyUpdateV2(roots, namesARoot, nil), ErrRootNotAllowed)
	if refused.Root != "m" {
		t.Fatalf("the V2 refusal reports root %q, want \"m\"", refused.Root)
	}

	fits := budgetDoc(t, base)
	fits.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 3, Structs: 3, Bytes: 3, Split: -1} })
	fitsBefore := stateOf(fits)
	requireApplied(t, fits, fitsBefore, ApplyUpdateV2(fits, v2, nil))
	if got, want := fits.GetText("t").ToString(), appliedText(t, base, v1); got != want {
		t.Fatalf("the applied V2 update left %q, want %q", got, want)
	}
}

// A budget or allowed-roots callback that panics refuses the update and leaves
// the document as it was, as a panicking scan budget does: the panic is
// reported as an invalid update naming the callback, and the document goes on
// taking updates once the callback is removed.
func TestUnit_ApplyBudget_APanickingCallbackRefusesTheUpdateAndChangesNothing(t *testing.T) {
	const base = "hello world"
	update := threeInserts(t, base)

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget { panic("boom in the apply budget") })
	before := stateOf(doc)
	err := ApplyUpdateV1(doc, update, nil)
	requireRefused(t, doc, before, err, ErrInvalidUpdate)
	if msg := err.Error(); !strings.Contains(msg, "WithApplyBudget callback") || !strings.Contains(msg, "boom in the apply budget") {
		t.Fatalf("the error does not name the callback and its panic: %v", err)
	}

	doc.SetApplyBudget(nil)
	doc.SetAllowedRoots(func(any) []string { panic("boom in the roots") })
	err = ApplyUpdateV1(doc, update, nil)
	requireRefused(t, doc, before, err, ErrInvalidUpdate)
	if msg := err.Error(); !strings.Contains(msg, "WithAllowedRoots callback") || !strings.Contains(msg, "boom in the roots") {
		t.Fatalf("the error does not name the callback and its panic: %v", err)
	}

	doc.SetAllowedRoots(nil)
	requireApplied(t, doc, before, ApplyUpdateV1(doc, update, nil))
}

// denseAnyValues is an update of structs items for client 9, each a lib0 Any
// content of vals null values parented to root "dense": one byte of frame for
// each value, and an interface word and a slot of a []any for each once
// decoded. No item names an origin, so each integrates at the head of the root
// and its conflict scan stops at the first item it meets (same client, same
// origins).
func denseAnyValues(structs, vals int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(uint64(structs))
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	for range structs {
		enc.WriteUint8(wireAny)
		enc.WriteUint8(1) // parent info: a named root follows
		enc.WriteVarString("dense")
		enc.WriteVarUint(uint64(vals))
		for range vals {
			enc.WriteUint8(126) // lib0 Any: null
		}
	}
	enc.WriteVarUint(0) // delete set: no clients
	return append([]byte(nil), enc.Bytes()...)
}

// A frame dense in lib0 Any values is refused by Decoded as it decodes, before
// the values it carries are in memory: the refused apply allocates a small
// fraction of what the same frame allocates on a document with no budget.
// Measured on this frame (2,000 structs of 50 null values each, 118,006
// bytes) the refused apply allocated 21,320 bytes against the unbudgeted
// apply's 3,933,056: a ratio of about 184 to 1, and 0.1 ms against 6 ms. The
// test requires a factor of 20, which a check made after the decode could not
// reach - it would have allocated every value of the frame first.
func TestUnit_ApplyBudget_ADenseFrameOfAnyValuesIsRefusedBeforeItAllocates(t *testing.T) {
	const base = "hello world"
	const structs, vals = 2_000, 50
	update := denseAnyValues(structs, vals)

	open := budgetDoc(t, base)
	openAllocated, openTook, err := applyMeasured(open, update)
	if err != nil {
		t.Fatalf("the document with no budget refused the dense frame: %v", err)
	}
	if got := open.StructCount(); got != 1+structs {
		t.Fatalf("the unbudgeted apply left %d structs, want %d", got, 1+structs)
	}

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 100, Structs: -1, Bytes: -1, Split: -1} })
	before := stateOf(doc)
	allocated, took, err := applyMeasured(doc, update)
	t.Logf("a dense frame of %d bytes: the refused apply allocated %d bytes in %s, the unbudgeted one %d bytes in %s",
		len(update), allocated, took, openAllocated, openTook)
	requireRefused(t, doc, before, err, ErrApplyBudget)
	if allocated*20 > openAllocated {
		t.Fatalf("the refused dense frame allocated %d bytes, want far less than the %d the same frame allocates unbudgeted",
			allocated, openAllocated)
	}
}

// A document whose only bound is an apply budget, or only an allowed-roots
// list, is a bounded document: it keeps the journal a refusal needs, so a
// refused apply is rolled back whole rather than left half integrated. The
// update refused here has integrated all three of its structs by the time the
// count is checked, and the document holds none of them afterwards.
func TestUnit_ApplyBudget_ADocumentWithOnlyABudgetRollsARefusedApplyBack(t *testing.T) {
	const base = "hello world"
	update := threeInserts(t, base)

	budget := budgetDoc(t, base)
	budget.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: 1, Bytes: -1, Split: -1} })
	before := stateOf(budget)
	refused := requireRefused(t, budget, before, ApplyUpdateV1(budget, update, nil), ErrApplyBudget)
	if refused.Structs != 3 {
		t.Fatalf("the refusal reports %d structs, want the 3 it integrated before the check", refused.Structs)
	}

	roots := budgetDoc(t, base)
	roots.SetAllowedRoots(func(any) []string { return []string{"nothing"} })
	rootsBefore := stateOf(roots)
	requireRefused(t, roots, rootsBefore, ApplyUpdateV1(roots, mapSetUpdate(t, base, 3), nil), ErrRootNotAllowed)

	// Both documents go on taking what a peer sends once the bound is lifted.
	want := appliedText(t, base, update)
	for _, doc := range []*Doc{budget, roots} {
		doc.SetApplyBudget(nil)
		doc.SetAllowedRoots(nil)
		if err := ApplyUpdateV1(doc, update, nil); err != nil {
			t.Fatalf("the document refused the update with no bound set: %v", err)
		}
		if got := doc.GetText("t").ToString(); got != want {
			t.Fatalf("the document holds %q, want %q", got, want)
		}
	}
}

// The counts are kept on every bounded document, whatever refuses an apply: an
// update a scan budget refuses reports the values it had decoded and the
// structs it had integrated by then, and a document with no bound at all keeps
// no tally and refuses nothing.
func TestUnit_ApplyBudget_AScanRefusalReportsWhatTheApplyDecoded(t *testing.T) {
	const l, n = 2_000, 20
	doc := chainedList(t, l)
	doc.SetIntegrateScanBudget(func(any) int { return 1 })
	before := stateOf(doc)
	update := originAtTheStart(n)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrIntegrateScanBudget)
	if refused.Decoded != n {
		t.Fatalf("the refusal reports %d decoded values, want the %d structs the update carries", refused.Decoded, n)
	}
	if refused.Structs != 0 {
		t.Fatalf("the refusal reports %d structs, want none: the first item's scan passed the limit", refused.Structs)
	}
	if refused.Scanned != 1 {
		t.Fatalf("the refusal reports %d items scanned, want the limit of 1", refused.Scanned)
	}

	doc.SetIntegrateScanBudget(nil)
	requireApplied(t, doc, before, ApplyUpdateV1(doc, update, nil))
}

// guardedState is the whole state of a document with two roots written by
// client 1: the text "t" holding base, and the map "m" holding one entry. An
// update that writes beside either names no root on the wire - it inherits
// its parent from the item its origin names, and a delete range names nothing
// at all - so what keeps it out of a root is the containment check over what
// the apply adds and deletes (checkItemRoot), not the name check in the
// decode.
func guardedState(t *testing.T, base string) []byte {
	t.Helper()
	writer := New(WithClientID(1))
	text, m := writer.GetText("t"), writer.GetMap("m")
	writer.Transact(func(txn *Transaction) {
		text.Insert(txn, 0, base, nil)
		m.Set(txn, "k", "old")
	})
	return EncodeStateAsUpdateV1(writer, nil)
}

// guardedDoc is a document holding guardedState, with no bound.
func guardedDoc(t *testing.T, base string) *Doc {
	t.Helper()
	doc := New(WithClientID(7))
	if err := ApplyUpdateV1(doc, guardedState(t, base), nil); err != nil {
		t.Fatalf("the guarded state was refused: %v", err)
	}
	return doc
}

// guardedEdit is a V1 update of what edit writes on a peer holding
// guardedState.
func guardedEdit(t *testing.T, base string, client ClientID, edit func(txn *Transaction, text *YText, m *YMap)) []byte {
	t.Helper()
	peer := New(WithClientID(client))
	if err := ApplyUpdateV1(peer, guardedState(t, base), nil); err != nil {
		t.Fatalf("the peer refused the guarded state: %v", err)
	}
	sv := peer.StateVector()
	text, m := peer.GetText("t"), peer.GetMap("m")
	peer.Transact(func(txn *Transaction) { edit(txn, text, m) })
	return EncodeStateAsUpdateV1(peer, sv)
}

// An allowed-roots list is a containment rule over the whole update, not a
// rule about the names the update writes: an update that overwrites a key of
// a root outside the list, deletes an item under it, or writes beside text it
// holds, is refused and rolled back whole, though none of those updates names
// the root on the wire. Each case is controlled by the same update applied
// with no list, which changes the document.
func TestUnit_AllowedRoots_RefusesWhatAnUpdateAddsOrDeletesUnderADisallowedRoot(t *testing.T) {
	const base = "hello world"
	for name, c := range map[string]struct {
		allowed []string
		edit    func(txn *Transaction, text *YText, m *YMap)
		root    string
	}{
		"an overwrite of a key of the map": {
			allowed: []string{"t"},
			edit:    func(txn *Transaction, _ *YText, m *YMap) { m.Set(txn, "k", "new") },
			root:    "m",
		},
		"a delete of the key the map holds": {
			allowed: []string{"t"},
			edit:    func(txn *Transaction, _ *YText, m *YMap) { m.Delete(txn, "k") },
			root:    "m",
		},
		"text written beside the text it holds": {
			allowed: []string{"m"},
			edit:    func(txn *Transaction, text *YText, _ *YMap) { text.Insert(txn, 3, "xyz", nil) },
			root:    "t",
		},
		"a delete of a range of the text it holds": {
			allowed: []string{"m"},
			edit:    func(txn *Transaction, text *YText, _ *YMap) { text.Delete(txn, 2, 4) },
			root:    "t",
		},
	} {
		t.Run(name, func(t *testing.T) {
			update := guardedEdit(t, base, 2, c.edit)

			doc := guardedDoc(t, base)
			doc.SetAllowedRoots(func(any) []string { return c.allowed })
			before := stateOf(doc)
			updates := observed(doc)
			refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrRootNotAllowed)
			if refused.Root != c.root {
				t.Fatalf("the refusal reports root %q, want %q", refused.Root, c.root)
			}
			if *updates != 0 {
				t.Fatalf("the refused update was handed to %d observers", *updates)
			}

			// The control: the same update with no list changes the document.
			open := guardedDoc(t, base)
			requireApplied(t, open, stateOf(open), ApplyUpdateV1(open, update, nil))
		})
	}
}

// An update that overwrites a key, deletes an item and writes beside held
// text, all under roots the list names, is applied whole and the document
// holds the result.
func TestUnit_AllowedRoots_AppliesAnUpdateThatStaysInsideTheRootsItNames(t *testing.T) {
	const base = "hello world"
	update := guardedEdit(t, base, 2, func(txn *Transaction, text *YText, m *YMap) {
		m.Set(txn, "k", "new")
		text.Insert(txn, 5, "XY", nil)
		text.Delete(txn, 0, 2)
	})

	doc := guardedDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{"t", "m"} })
	before := stateOf(doc)
	requireApplied(t, doc, before, ApplyUpdateV1(doc, update, nil))

	open := guardedDoc(t, base)
	if err := ApplyUpdateV1(open, update, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := stateOf(doc), stateOf(open); got != want {
		t.Fatalf("the document under the list reads\n%+v\nand the same update with no list leaves\n%+v", got, want)
	}
	if entry, _ := doc.GetMap("m").Get("k"); entry != "new" {
		t.Fatalf("the map holds %v, want the value the update set", entry)
	}
}

// An update that touches two roots, one the list names and one it does not,
// is refused whole: neither root changes, though the allowed root's items
// integrate before the refused one is reached.
func TestUnit_AllowedRoots_RefusesAnUpdateWholeWhenOneOfItsRootsIsNotAllowed(t *testing.T) {
	const base = "hello world"
	update := guardedEdit(t, base, 2, func(txn *Transaction, text *YText, m *YMap) {
		text.Insert(txn, 5, "XY", nil)
		m.Set(txn, "k", "new")
	})

	doc := guardedDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{"t"} })
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrRootNotAllowed)
	if refused.Root != "m" {
		t.Fatalf("the refusal reports root %q, want \"m\"", refused.Root)
	}

	open := guardedDoc(t, base)
	requireApplied(t, open, stateOf(open), ApplyUpdateV1(open, update, nil))
}

// A long run of items under one allowed parent applies: the containment check
// walks each item's parent chain once and answers the rest from the map it
// memoises them in, so a run costs a lookup an item.
func TestUnit_AllowedRoots_ARunUnderOneAllowedParentApplies(t *testing.T) {
	const base, n = "hello world", 2_000
	update := guardedEdit(t, base, 2, func(txn *Transaction, text *YText, _ *YMap) {
		for i := range n {
			text.Insert(txn, i, "x", nil)
		}
	})

	doc := guardedDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{"t"} })
	before := stateOf(doc)
	requireApplied(t, doc, before, ApplyUpdateV1(doc, update, nil))
	if got := doc.GetText("t").Len(); got != len(base)+n {
		t.Fatalf("the text is %d characters, want the %d it held and the %d the run added", got, len(base), n)
	}

	open := guardedDoc(t, base)
	if err := ApplyUpdateV1(open, update, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := doc.GetText("t").ToString(), open.GetText("t").ToString(); got != want {
		t.Fatal("the document under the list differs from one that took the run with no list")
	}
}

// A root the document does not hold when the apply begins is allowed by name:
// the update that creates it passes the name check in the decode, and every
// later item under it passes the containment check too, within the same
// apply.
func TestUnit_AllowedRoots_ARootTheUpdateCreatesIsAllowedByName(t *testing.T) {
	const base = "hello world"
	peer := New(WithClientID(2))
	if err := ApplyUpdateV1(peer, guardedState(t, base), nil); err != nil {
		t.Fatal(err)
	}
	sv := peer.StateVector()
	fresh := peer.GetText("fresh")
	peer.Transact(func(txn *Transaction) { fresh.Insert(txn, 0, "first", nil) })
	peer.Transact(func(txn *Transaction) { fresh.Insert(txn, 5, " and more", nil) })
	update := EncodeStateAsUpdateV1(peer, sv)

	doc := guardedDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{"fresh"} })
	if err := ApplyUpdateV1(doc, update, nil); err != nil {
		t.Fatalf("an update under a root the list names was refused: %v", err)
	}
	if got, want := doc.GetText("fresh").ToString(), "first and more"; got != want {
		t.Fatalf("the new root holds %q, want %q", got, want)
	}

	// The same update under a list that does not name it is refused at the
	// decode, by the name check.
	other := guardedDoc(t, base)
	other.SetAllowedRoots(func(any) []string { return []string{"t"} })
	before := stateOf(other)
	refused := requireRefused(t, other, before, ApplyUpdateV1(other, update, nil), ErrRootNotAllowed)
	if refused.Root != "fresh" {
		t.Fatalf("the refusal reports root %q, want \"fresh\"", refused.Root)
	}
}

// gapStringUpdate is one struct of client 2 at clock, holding value: parented
// to the root text "t" where origin is nil, and written beside the item at
// origin where it is not. It is hand-encoded because no writer document
// produces the shape the test below needs - a struct whose left origin is its
// own client's earlier clock, which a document that has not seen that clock
// parks and the drain later trims in place.
func gapStringUpdate(clock uint64, value string, origin *ID) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // one client
	enc.WriteVarUint(1) // one struct of it
	enc.WriteVarUint(2) // client 2
	enc.WriteVarUint(clock)
	if origin == nil {
		enc.WriteUint8(4) // no origins, a named parent
		enc.WriteVarUint(1)
		enc.WriteVarString("t")
	} else {
		enc.WriteUint8(0x84) // a left origin, string content
		enc.WriteVarUint(uint64(origin.Client))
		enc.WriteVarUint(origin.Clock)
	}
	enc.WriteVarString(value)
	enc.WriteVarUint(0) // an empty delete set
	return bytes.Clone(enc.Bytes())
}

// A refusal must never cost the document content it had already accepted.
// The apply that fills a gap drains the pending queue, and the drain changes
// the parked items it integrates in place: here the parked "BC" at clock 1,
// whose left origin is clock 0 of its own client, is trimmed against the
// arriving "AB", which carries clocks 0 and 1. A refusal raised after that
// drain had succeeded could not give the trim back - the whole-apply rollback
// restores the queue's pointers, and the content is gone - so this node would
// keep a document every peer disagrees with, a divergence no memory graph
// shows.
//
// The gap is then filled with LESS than the refused update carried, one
// character at clock 0, which is what makes the loss visible: the trimmed
// item now waits on a clock that fill never supplies, so "BC" can never land
// and the text stays "A". Re-applying the whole "AB" would cover the trimmed
// item's new dependency and hide the defect, which is why the fill is "A".
//
// The fix is that the struct and byte budget is read before the drain runs
// (drainAfterApply) and endRemoteApply raises no refusal of its own. The
// shape is the sjawhar/ygo release reviewer's, who found it at the first
// patch.
func TestUnit_ApplyBudget_ARefusalAfterTheDrainKeepsWhatTheDocumentHeld(t *testing.T) {
	for _, shape := range []struct {
		name string
		v2   bool
	}{{name: "v1"}, {name: "v2", v2: true}} {
		t.Run(shape.name, func(t *testing.T) {
			apply := func(doc *Doc, update []byte) error {
				if !shape.v2 {
					return ApplyUpdateV1(doc, update, "peer")
				}
				asV2, err := UpdateV1ToV2(update)
				if err != nil {
					t.Fatalf("re-encode the update as V2: %v", err)
				}
				return ApplyUpdateV2(doc, asV2, "peer")
			}

			doc := New(WithClientID(7), WithGC(false))
			if err := apply(doc, gapStringUpdate(1, "BC", &ID{Client: 2, Clock: 0})); err != nil {
				t.Fatalf("park the update behind the gap: %v", err)
			}
			if parked := doc.PendingStats(); parked.Items == 0 {
				t.Fatal("the update behind the gap did not park, so this drives nothing")
			}

			doc.SetApplyBudget(func(any) ApplyBudget {
				return ApplyBudget{Decoded: -1, Structs: 0, Bytes: -1, Split: -1}
			})
			if err := apply(doc, gapStringUpdate(0, "AB", nil)); !errors.Is(err, ErrApplyBudget) {
				t.Fatalf("the apply answered %v, want the struct budget's refusal", err)
			}

			doc.SetApplyBudget(nil)
			if err := apply(doc, gapStringUpdate(0, "A", nil)); err != nil {
				t.Fatalf("the fill of the gap: %v", err)
			}
			if got := doc.GetText("t").ToString(); got != "ABC" {
				t.Fatalf("the document reads %q, want \"ABC\": the refused apply's drain kept its trim", got)
			}
		})
	}
}

// A frame whose ContentAny declares a length is refused on that length,
// before the list is allocated for it. The declared count used to be read
// only after make([]any, n) had run and after the first value was in memory,
// so a budget of one value refused a frame of two million of them having
// already taken 32 MB - the out-of-memory path the budget exists to close.
//
// Measured here, on this tree, with the release reviewer's harness: a frame
// declaring 1 value, refused, allocates 2,144 bytes; one declaring 2,000,000,
// refused, allocates 2,368 bytes, against the 32,008,272 the same frame
// allocated before the fix (1,776 for the one-value frame). The test
// requires it under 1,000,000, the reviewer's threshold: a check made after
// the allocation cannot reach it.
func TestUnit_ApplyBudget_ADeclaredLengthIsRefusedBeforeItIsAllocated(t *testing.T) {
	const base = "hello world"
	const many = 2_000_000
	oneValue := ApplyBudget{Decoded: 1, Structs: -1, Bytes: -1, Split: -1}

	one := budgetDoc(t, base)
	one.SetApplyBudget(func(any) ApplyBudget { return oneValue })
	oneBefore := stateOf(one)
	oneAllocated, oneTook, oneErr := applyMeasured(one, denseAnyValues(1, 1))
	requireRefused(t, one, oneBefore, oneErr, ErrApplyBudget)

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget { return oneValue })
	before := stateOf(doc)
	allocated, took, err := applyMeasured(doc, denseAnyValues(1, many))
	t.Logf("a frame declaring %d values was refused having allocated %d bytes in %s; one declaring a single value allocated %d bytes in %s",
		many, allocated, took, oneAllocated, oneTook)
	requireRefused(t, doc, before, err, ErrApplyBudget)
	if allocated >= 1_000_000 {
		t.Fatalf("the refused frame allocated %d bytes, want under 1,000,000: the declared length was allocated before it was charged", allocated)
	}

	// The control: the same shape at a length the budget holds applies, so
	// what refuses the frames above is the length and not the shape.
	fits := budgetDoc(t, base)
	fits.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 5, Structs: -1, Bytes: -1, Split: -1} })
	fitsBefore := stateOf(fits)
	requireApplied(t, fits, fitsBefore, ApplyUpdateV1(fits, denseAnyValues(1, 4), nil))
	if got := fits.StructCount(); got != fitsBefore.structs+1 {
		t.Fatalf("the applied frame left %d structs, want the %d before it and its own", got, fitsBefore.structs)
	}
}

// nestedTypeChain is an update of n structs for client 9, each a YMap held
// under the key "c" of the one before it, the first of them under the root
// name: a chain of n types nested n deep, which is what makes the
// allowed-roots walk up the parents repeat itself.
func nestedTypeChain(root string, n int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(uint64(n))
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	for i := range n {
		enc.WriteUint8(wireType | flagHasParentSub)
		if i == 0 {
			enc.WriteUint8(1) // a named root follows
			enc.WriteVarString(root)
		} else {
			enc.WriteUint8(0) // the parent is the struct before it, by ID
			enc.WriteVarUint(9)
			enc.WriteVarUint(uint64(i - 1))
		}
		enc.WriteVarString("c")
		enc.WriteUint8(1) // YMap
	}
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// An update of types nested inside one another costs the allowed-roots check
// one walk, not one for each item. The check memoises the root above each
// type it answers, and the walk stops at the first ancestor already answered
// and records the root for every type it passed, so the whole chain is
// answered once.
//
// Before that it recorded only the type it started from, so each new child
// walked to the root again: the release reviewer measured a 213 KB update of
// 32,768 nested types holding the document's write lock for 11.235 s against
// 34.55 ms with no list, a factor of 325, bounded by neither the struct
// count nor the scan budget. Measured here, on this tree, on the 278,408-byte
// update below: 146.3 ms with no list and 272.4 ms under one, a factor of
// 1.9. The test allows a factor of 5 and 200 ms on top of it, loose enough
// not to flake on a loaded machine and far under what the repeated walk
// takes.
func TestUnit_AllowedRoots_AChainOfNestedTypesIsWalkedOnce(t *testing.T) {
	const base, nested = "hello world", 32_768
	update := nestedTypeChain("deep", nested)

	open := budgetDoc(t, base)
	_, off, err := applyMeasured(open, update)
	if err != nil {
		t.Fatalf("the document with no list refused the chain: %v", err)
	}
	if got, want := open.StructCount(), 1+nested; got != want {
		t.Fatalf("the chain left %d structs, want %d", got, want)
	}

	doc := budgetDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{"deep"} })
	before := stateOf(doc)
	_, on, err := applyMeasured(doc, update)
	t.Logf("a chain of %d nested types in a %d-byte update: %s with no list, %s under one", nested, len(update), off, on)
	requireApplied(t, doc, before, err)
	if got, want := doc.StructCount(), 1+nested; got != want {
		t.Fatalf("the chain under the list left %d structs, want %d", got, want)
	}
	if limit := 5*off + 200*time.Millisecond; on > limit {
		t.Fatalf("the chain under the list took %s, want within %s of the %s it takes with no list", on, limit, off)
	}
}

// subdocState is a document of client 7 holding two roots: the map "t", whose
// key "doc" holds child, and the map "m", whose key "k" holds a value. A
// remote update that deletes both deletes the subdocument's item first, since
// it carries the lower clock, so a refusal raised at the second delete is
// raised with the subdocument already detached.
func subdocState(t *testing.T) (*Doc, *Doc) {
	t.Helper()
	doc := New(WithClientID(7))
	child := New(WithGUID("child"), WithShouldLoad(false))
	holder, m := doc.GetMap("t"), doc.GetMap("m")
	doc.Transact(func(txn *Transaction) {
		holder.Set(txn, "doc", child)
		m.Set(txn, "k", "old")
	})
	return doc, child
}

// deleteBothKeys is a V1 update of a peer deleting the key "doc" of the map
// "t" and the key "k" of the map "m" of doc, which holds neither structs of
// its own nor a root name: two delete ranges and nothing else.
func deleteBothKeys(t *testing.T, doc *Doc) []byte {
	t.Helper()
	peer := New(WithClientID(2))
	if err := ApplyUpdateV1(peer, EncodeStateAsUpdateV1(doc, nil), nil); err != nil {
		t.Fatalf("the peer refused the state: %v", err)
	}
	sv := peer.StateVector()
	// The roots are opened outside the transaction: GetMap takes the
	// document's lock, which the transaction already holds.
	holder, m := peer.GetMap("t"), peer.GetMap("m")
	peer.Transact(func(txn *Transaction) {
		holder.Delete(txn, "doc")
		m.Delete(txn, "k")
	})
	return EncodeStateAsUpdateV1(peer, sv)
}

// loadNotifies reports whether child.Load() tells its parent, which is what a
// subdocument still attached to the document does and a detached one cannot.
func loadNotifies(parent, child *Doc) bool {
	loaded := 0
	stop := parent.OnSubdocs(func(ev SubdocsEvent) {
		for _, sub := range ev.Loaded {
			if sub == child {
				loaded++
			}
		}
	})
	defer stop()
	child.Load()
	return loaded == 1
}

// A refused update that had already deleted an item carrying a subdocument
// leaves the subdocument where it was. The delete detaches it from its parent
// (Item.delete), and the rollback used to revive the item without giving the
// subdocument its item, its parent and its attached state back, so the
// document went on showing a subdocument whose Load told nobody.
//
// The refusal here is the allowed-roots rule's, raised from inside the delete
// of the second key while the first key's delete has already detached the
// subdocument. The apply budget cannot reach this: it is read before the
// delete set runs (refuseOverBudget).
func TestUnit_AllowedRoots_ARefusalKeepsADeletedSubdocumentAttached(t *testing.T) {
	doc, child := subdocState(t)
	update := deleteBothKeys(t, doc)

	doc.SetAllowedRoots(func(any) []string { return []string{"t"} })
	err := ApplyUpdateV1(doc, update, nil)
	if !errors.Is(err, ErrRootNotAllowed) {
		t.Fatalf("the update was applied with err %v, want the roots rule's refusal", err)
	}
	if entry, held := doc.GetMap("t").Get("doc"); !held || entry != child {
		t.Fatalf("the refused update left %v under the key the subdocument held", entry)
	}
	if !loadNotifies(doc, child) {
		t.Fatal("the subdocument's Load told its parent nothing: the rollback left it detached")
	}

	// The control: the same update with no list deletes both keys, and the
	// subdocument it removed is detached, so its Load tells no one.
	open, openChild := subdocState(t)
	if err := ApplyUpdateV1(open, deleteBothKeys(t, open), nil); err != nil {
		t.Fatalf("the document with no list refused the update: %v", err)
	}
	if _, held := open.GetMap("t").Get("doc"); held {
		t.Fatal("the applied update kept the key, so it is no control")
	}
	if loadNotifies(open, openChild) {
		t.Fatal("a removed subdocument still told its parent, so the check above proves nothing")
	}
}

// repeatedKeyMap is an update of one struct for client 9 whose ContentAny
// holds one lib0 map of n entries, every one of them the key "k": the Go map
// the decode builds holds one of them, and the frame carried n.
func repeatedKeyMap(n int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireAny)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString("dense")
	enc.WriteVarUint(1) // one value
	enc.WriteUint8(118) // lib0 Any: a map
	enc.WriteVarUint(uint64(n))
	for range n {
		enc.WriteVarString("k")
		enc.WriteUint8(126) // lib0 Any: null
	}
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// adjacentDeletes is an update of no structs whose delete set names n
// one-clock ranges of client, each touching the one before it, which the
// decode sorts and merges into a single range before it returns.
func adjacentDeletes(client ClientID, n int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(0)              // no structs
	enc.WriteVarUint(1)              // one client in the delete set
	enc.WriteVarUint(uint64(client)) // the client that wrote the base text
	enc.WriteVarUint(uint64(n))
	for i := range n {
		enc.WriteVarUint(uint64(i)) // clock
		enc.WriteVarUint(1)         // one clock long
	}
	return bytes.Clone(enc.Bytes())
}

// Decoded counts what the apply read off the frame, not what survived the
// decode. Entries of a lib0 Any map that share a key overwrite one another in
// the Go map the decode builds, and delete ranges that touch are merged by
// the decode's own sort, so a count taken afterwards counted a hundred
// thousand map entries as one and eleven delete ranges as one: a frame of
// 100,000 entries was admitted under Decoded: 3.
func TestUnit_ApplyBudget_CountsEachEntryAndEachRangeAsItIsRead(t *testing.T) {
	const base = "hello world"
	const entries = 100_000

	t.Run("map entries that share a key", func(t *testing.T) {
		update := repeatedKeyMap(entries)

		doc := budgetDoc(t, base)
		doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 3, Structs: -1, Bytes: -1, Split: -1} })
		before := stateOf(doc)
		requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)

		// The control: a budget that holds the struct, the map and all of its
		// entries applies the same frame, and what it leaves is the one entry
		// the repeated key decodes to - which is what the count used to see.
		fits := budgetDoc(t, base)
		fits.SetApplyBudget(func(any) ApplyBudget {
			return ApplyBudget{Decoded: 2 + entries, Structs: -1, Bytes: -1, Split: -1}
		})
		fitsBefore := stateOf(fits)
		requireApplied(t, fits, fitsBefore, ApplyUpdateV1(fits, update, nil))
		held, ok := fits.store.clients[9][0].Content.(*ContentAny)
		if !ok {
			t.Fatalf("the struct holds %T, want a ContentAny", fits.store.clients[9][0].Content)
		}
		decoded, ok := held.Vals[0].(map[string]any)
		if !ok || len(decoded) != 1 {
			t.Fatalf("the frame decoded to %v, want the one entry the repeated key leaves", held.Vals[0])
		}
	})

	t.Run("delete ranges that merge", func(t *testing.T) {
		// The base text is eleven clocks of client 1, deleted one clock at a
		// time: eleven ranges on the wire and one once the decode has sorted
		// and merged them.
		update := adjacentDeletes(1, len(base))

		doc := budgetDoc(t, base)
		doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 5, Structs: -1, Bytes: -1, Split: -1} })
		before := stateOf(doc)
		refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)
		if refused.Decoded != len(base) {
			t.Fatalf("the refusal reports %d decoded values, want the %d ranges the frame carried", refused.Decoded, len(base))
		}

		fits := budgetDoc(t, base)
		fits.SetApplyBudget(func(any) ApplyBudget {
			return ApplyBudget{Decoded: len(base), Structs: -1, Bytes: -1, Split: -1}
		})
		fitsBefore := stateOf(fits)
		requireApplied(t, fits, fitsBefore, ApplyUpdateV1(fits, update, nil))
		if got := fits.GetText("t").ToString(); got != "" {
			t.Fatalf("the applied ranges left %q, want the empty text they delete", got)
		}
	})
}

// xmlElementUpdate is an update of one struct for client 9: an XML element
// under the root name, whose node name is n bytes. The name is the sender's
// to choose and the document keeps it for as long as it holds the element.
func xmlElementUpdate(root string, n int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireType)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteUint8(3) // YXmlElement
	enc.WriteVarString(strings.Repeat("x", n))
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// Bytes counts the name an XML element or hook carries. Every ContentType
// used to count as no bytes at all - a type is an empty container whose
// structs are counted as they integrate - but an element and a hook keep a
// name their sender sized, so a one-million-byte node name was admitted and
// retained under Bytes: 0.
func TestUnit_ApplyBudget_BytesCountsAnXMLTypeName(t *testing.T) {
	const base, name, root = "hello world", 1_000_000, "xml"
	update := xmlElementUpdate(root, name)

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: 1, Structs: 1, Bytes: 0, Split: -1} })
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)
	// The root the update creates is a sender-chosen string the document
	// keeps too, and is charged beside the node name (noteRootName).
	if want := name + len(root); refused.Bytes != want {
		t.Fatalf("the refusal reports %d bytes, want the %d of the node name and the root it names", refused.Bytes, want)
	}
	if _, held := doc.share["xml"]; held {
		t.Fatal("the refused update left its root on the document")
	}

	// The control: the same update with no budget applies and the document
	// keeps the name, which is what makes it a cost worth counting.
	open := budgetDoc(t, base)
	if err := ApplyUpdateV1(open, update, nil); err != nil {
		t.Fatalf("the document with no budget refused the element: %v", err)
	}
	ct, ok := open.store.clients[9][0].Content.(*ContentType)
	if !ok {
		t.Fatalf("the struct holds %T, want a ContentType", open.store.clients[9][0].Content)
	}
	elem, ok := ct.Type.owner.(*YXmlElement)
	if !ok || len(elem.NodeName) != name {
		t.Fatalf("the applied element holds a name of %T, want the %d bytes it was sent", ct.Type.owner, name)
	}
}

// A refused update leaves the type it formatted plain. Integrating a
// ContentFormat raises hasFormatting and cleanFormatting and bumps fmtGen on
// the type it hangs under, and nothing lowers them again, so a refused update
// used to move a plain text onto the formatting-aware path for the rest of
// the document's life - every later Delete paying for the cleanup walk a
// plain text skips.
//
// The update formats text under an allowed root and then overwrites a key of
// a root the list does not name. The overwrite carries an origin and names no
// root on the wire, so it is refused by the containment check as it
// integrates, after the format items have integrated and raised the flags.
func TestUnit_AllowedRoots_ARefusalPutsTheFormattingFlagsBack(t *testing.T) {
	const base = "hello world"
	update := guardedEdit(t, base, 2, func(txn *Transaction, text *YText, m *YMap) {
		text.Format(txn, 0, 5, Attributes{"b": true})
		m.Set(txn, "k", "new")
	})

	doc := guardedDoc(t, base)
	text := doc.GetText("t")
	if text.hasFormatting || text.cleanFormatting || text.fmtGen != 0 {
		t.Fatalf("the base text is already on the formatting path: %v %v %d", text.hasFormatting, text.cleanFormatting, text.fmtGen)
	}
	doc.SetAllowedRoots(func(any) []string { return []string{"t"} })
	before := stateOf(doc)
	requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrRootNotAllowed)
	if text.hasFormatting || text.cleanFormatting || text.fmtGen != 0 {
		t.Fatalf("the refused update left the text on the formatting path: %v %v %d", text.hasFormatting, text.cleanFormatting, text.fmtGen)
	}

	// The control: the same update with no list applies and moves the text
	// onto that path, so the flags above were the refusal's to put back.
	open := guardedDoc(t, base)
	requireApplied(t, open, stateOf(open), ApplyUpdateV1(open, update, nil))
	applied := open.GetText("t")
	if !applied.hasFormatting || !applied.cleanFormatting || applied.fmtGen == 0 {
		t.Fatalf("the applied update left the text plain: %v %v %d", applied.hasFormatting, applied.cleanFormatting, applied.fmtGen)
	}
}

// rootNamedUpdate is an update of two string structs for client 9 under the
// root name: the first names the root on the wire, and the second carries a
// left origin and names nothing at all, so what must let the second through
// is the containment check over what the apply adds and not the name check
// in the decode.
func rootNamedUpdate(root, first, second string) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(2) // two structs
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireString)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteVarString(first)
	enc.WriteUint8(wireString | flagHasOrigin)
	enc.WriteVarUint(9)                      // the left origin's client
	enc.WriteVarUint(uint64(len(first) - 1)) // its clock: the first struct's last
	enc.WriteVarString(second)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// A root the document does not hold yet is allowed by its name, and the empty
// name is a name like any other. The containment check used to read the empty
// name as "this type is no root of the document" and refuse every item under
// a root named "" that the same update created, so a list that named it could
// not create it.
func TestUnit_AllowedRoots_AnAllowedEmptyRootNameCanBeCreated(t *testing.T) {
	const base = "hello world"
	update := rootNamedUpdate("", "first", " and more")

	doc := guardedDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{""} })
	if err := ApplyUpdateV1(doc, update, nil); err != nil {
		t.Fatalf("an update under the empty root name the list allows was refused: %v", err)
	}
	if got, want := doc.GetText("").ToString(), "first and more"; got != want {
		t.Fatalf("the root named %q holds %q, want %q", "", got, want)
	}

	// The control: a list that does not name it refuses the same update, at
	// the name check in the decode, and names the root it refused.
	other := guardedDoc(t, base)
	other.SetAllowedRoots(func(any) []string { return []string{"t"} })
	before := stateOf(other)
	refused := requireRefused(t, other, before, ApplyUpdateV1(other, update, nil), ErrRootNotAllowed)
	if refused.Root != "" {
		t.Fatalf("the refusal reports root %q, want the empty name the update wrote", refused.Root)
	}

	// A named root takes the same two structs, so the shape itself applies
	// under a list that allows it whatever the name is.
	named := guardedDoc(t, base)
	named.SetAllowedRoots(func(any) []string { return []string{"fresh"} })
	if err := ApplyUpdateV1(named, rootNamedUpdate("fresh", "first", " and more"), nil); err != nil {
		t.Fatalf("the same shape under a named root was refused: %v", err)
	}
	if got, want := named.GetText("fresh").ToString(), "first and more"; got != want {
		t.Fatalf("the root \"fresh\" holds %q, want %q", got, want)
	}
}

// anyValueUpdate is an update of one struct for client 9 whose ContentAny
// holds the lib0 Any value write puts on the wire and then two nulls.
//
// The two nulls are what make a count the scan stops short of visible: a
// counter that cannot read the value, or reads the wrong number of bytes of
// it, has already charged for the value itself and would otherwise agree
// with the decoder by accident. Stopping before the nulls undercounts by
// two, whatever the tag.
func anyValueUpdate(write func(enc *encoding.Encoder)) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireAny)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString("dense")
	enc.WriteVarUint(3) // the value below and two nulls after it
	write(enc)
	enc.WriteUint8(126)
	enc.WriteUint8(126)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// decodedValues is how many values v is, counted as the budget counts them:
// v itself, each element of an array and each entry of a map, at any depth.
// It reads the value the decoder built, so it is the number the counter that
// reads the wire has to agree with.
func decodedValues(v any) int {
	n := 1
	switch v := v.(type) {
	case []any:
		for _, e := range v {
			n += decodedValues(e)
		}
	case map[string]any:
		for _, e := range v {
			n += decodedValues(e)
		}
	}
	return n
}

// anyValuesOf is the values the struct client 9 wrote decoded to, read off
// the document that applied it with no budget.
func anyValuesOf(t *testing.T, doc *Doc) []any {
	t.Helper()
	held := doc.store.clients[9]
	if len(held) != 1 {
		t.Fatalf("the document holds %d structs of client 9, want 1", len(held))
	}
	content, ok := held[0].Content.(*ContentAny)
	if !ok {
		t.Fatalf("the struct holds %T, want a ContentAny", held[0].Content)
	}
	return content.Vals
}

// Decoded counts a lib0 Any frame as exactly the values the decoder builds
// from it, for every tag of the format and for values nested inside one
// another.
//
// This is what holds the counter to the decoder. The count is taken off the
// frame's bytes by countAnyValue, a second reader of the lib0 Any grammar
// beside anycodec.readAny, kept there because counting off the decoded value
// cannot see a map's repeated keys or a declared length before it is
// allocated for (countAnyRun). Two readers can drift, and the dangerous
// drift is silent: a tag the decode accepts and the scan does not ends the
// scan early, so the apply is charged for less than it decoded and the
// budget admits more than it says, with no error raised anywhere. A frame
// the decode REJECTS is safe - the decode reports the fault - and is not
// what this guards.
//
// So every tag is covered by name, and the expected count is read off the
// values the decode produced rather than written down: the apply must be
// admitted by a budget of exactly that many values and refused by one value
// less. A tag added to isAnyTag with no row here fails the coverage check
// below rather than going unnoticed.
func TestUnit_ApplyBudget_CountsEveryLib0AnyTagAsTheDecoderReadsIt(t *testing.T) {
	const base = "hello world"
	rows := map[string]struct {
		tag   byte
		write func(enc *encoding.Encoder)
	}{
		"undefined":              {127, func(e *encoding.Encoder) { e.WriteUint8(127) }},
		"null":                   {126, func(e *encoding.Encoder) { e.WriteUint8(126) }},
		"true":                   {120, func(e *encoding.Encoder) { e.WriteUint8(120) }},
		"false":                  {121, func(e *encoding.Encoder) { e.WriteUint8(121) }},
		"an integer":             {125, func(e *encoding.Encoder) { e.WriteUint8(125); e.WriteVarInt(-42) }},
		"a 32-bit float":         {124, func(e *encoding.Encoder) { e.WriteUint8(124); e.WriteFloat32(1.5) }},
		"a 64-bit float":         {123, func(e *encoding.Encoder) { e.WriteUint8(123); e.WriteFloat64(2.5) }},
		"a big integer":          {122, func(e *encoding.Encoder) { e.WriteUint8(122); e.WriteBigInt64(1 << 40) }},
		"a string":               {119, func(e *encoding.Encoder) { e.WriteUint8(119); e.WriteVarString("hello") }},
		"binary":                 {116, func(e *encoding.Encoder) { e.WriteUint8(116); e.WriteVarBytes([]byte{1, 2, 3}) }},
		"an array of three":      {117, func(e *encoding.Encoder) { writeAnyArray(e, 3) }},
		"a map of three entries": {118, func(e *encoding.Encoder) { writeAnyMap(e, 3) }},
		"an array holding a map": {117, func(e *encoding.Encoder) {
			e.WriteUint8(117)
			e.WriteVarUint(2)
			e.WriteUint8(126)
			writeAnyMap(e, 3)
		}},
		"a map holding an array": {118, func(e *encoding.Encoder) {
			e.WriteUint8(118)
			e.WriteVarUint(2)
			e.WriteVarString("a")
			e.WriteUint8(126)
			e.WriteVarString("b")
			writeAnyArray(e, 3)
		}},
	}

	covered := map[byte]struct{}{}
	for name, row := range rows {
		covered[row.tag] = struct{}{}
		t.Run(name, func(t *testing.T) {
			update := anyValueUpdate(row.write)

			// What the decoder built, with no budget to refuse it: one
			// struct and every value of its content.
			open := budgetDoc(t, base)
			if err := ApplyUpdateV1(open, update, nil); err != nil {
				t.Fatalf("the document with no budget refused the frame: %v", err)
			}
			want := 1
			for _, v := range anyValuesOf(t, open) {
				want += decodedValues(v)
			}

			fits := budgetDoc(t, base)
			fits.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: want, Structs: -1, Bytes: -1, Split: -1} })
			fitsBefore := stateOf(fits)
			if err := ApplyUpdateV1(fits, update, nil); err != nil {
				t.Fatalf("a budget of the %d values the decode produced refused the frame, so the count is above it: %v", want, err)
			}
			requireApplied(t, fits, fitsBefore, nil)

			tight := budgetDoc(t, base)
			tight.SetApplyBudget(func(any) ApplyBudget { return ApplyBudget{Decoded: want - 1, Structs: -1, Bytes: -1, Split: -1} })
			tightBefore := stateOf(tight)
			err := ApplyUpdateV1(tight, update, nil)
			if err == nil {
				t.Fatalf("a budget of %d values applied a frame of %d, so the count is below it: the counter and the decoder disagree", want-1, want)
			}
			requireRefused(t, tight, tightBefore, err, ErrApplyBudget)
		})
	}

	for tag := range 256 {
		if b := byte(tag); isAnyTag(b) {
			if _, held := covered[b]; !held {
				t.Fatalf("lib0 Any tag %d has no row here: the counter is unpinned for it", b)
			}
		}
	}
}

// writeAnyArray writes a lib0 Any array of n nulls, and writeAnyMap a lib0
// Any map of n entries under distinct keys, so that what the decoder builds
// holds every value the frame carried and the two counts must agree by
// construction.
func writeAnyArray(enc *encoding.Encoder, n int) {
	enc.WriteUint8(117)
	enc.WriteVarUint(uint64(n))
	for range n {
		enc.WriteUint8(126)
	}
}

func writeAnyMap(enc *encoding.Encoder, n int) {
	enc.WriteUint8(118)
	enc.WriteVarUint(uint64(n))
	for i := range n {
		enc.WriteVarString(fmt.Sprintf("k%d", i))
		enc.WriteUint8(126)
	}
}

// guardedHandoff is the whole state of a peer that holds guardedState and
// then ran edit: an update naming every root the document holds, as a client
// handing a document's whole state back does, carrying whatever edit wrote
// on top of it.
func guardedHandoff(t *testing.T, base string, client ClientID, edit func(txn *Transaction, text *YText, m *YMap)) []byte {
	t.Helper()
	peer := New(WithClientID(client))
	if err := ApplyUpdateV1(peer, guardedState(t, base), nil); err != nil {
		t.Fatalf("the peer refused the guarded state: %v", err)
	}
	text, m := peer.GetText("t"), peer.GetMap("m")
	peer.Transact(func(txn *Transaction) { edit(txn, text, m) })
	return EncodeStateAsUpdateV1(peer, nil)
}

// An update naming a root the document already holds is left to the
// containment rule: the name check refuses only a root the document does not
// hold, since creating one is what that check is for.
//
// Without the division a legitimate client is refused. A peer handing back
// the whole state of a document names every root the document holds,
// including one the owner allows no writes under, because the decode
// resolves each struct's root as it reads the struct - before the integrate
// discovers the struct is one the document already has and skips it. Such a
// handoff integrates nothing and deletes nothing under that root, and used
// to be refused all the same.
func TestUnit_AllowedRoots_AHandoffNamingAHeldRootItDoesNotWriteUnderApplies(t *testing.T) {
	const base = "hello world"
	onlyText := func(any) []string { return []string{"t"} }

	doc := guardedDoc(t, base)
	doc.SetAllowedRoots(onlyText)
	before := stateOf(doc)
	if err := ApplyUpdateV1(doc, guardedState(t, base), nil); err != nil {
		t.Fatalf("a handoff of the state the document already holds was refused: %v", err)
	}
	if after := stateOf(doc); after != before {
		t.Fatalf("the handoff changed the document:\nbefore %+v\nafter  %+v", before, after)
	}

	// The same handoff carrying one new item under the root the list does
	// not name is refused by the containment rule, and so is one deleting an
	// item under it: what the name check stopped doing, containment does.
	for name, c := range map[string]struct {
		edit func(txn *Transaction, text *YText, m *YMap)
	}{
		"one new item under the held root": {func(txn *Transaction, _ *YText, m *YMap) { m.Set(txn, "k", "new") }},
		"a delete under the held root":     {func(txn *Transaction, _ *YText, m *YMap) { m.Delete(txn, "k") }},
	} {
		t.Run(name, func(t *testing.T) {
			update := guardedHandoff(t, base, 2, c.edit)

			guarded := guardedDoc(t, base)
			guarded.SetAllowedRoots(onlyText)
			guardedBefore := stateOf(guarded)
			refused := requireRefused(t, guarded, guardedBefore, ApplyUpdateV1(guarded, update, nil), ErrRootNotAllowed)
			if refused.Root != "m" {
				t.Fatalf("the refusal reports root %q, want \"m\"", refused.Root)
			}

			// The control: the same update with no list changes the document.
			open := guardedDoc(t, base)
			requireApplied(t, open, stateOf(open), ApplyUpdateV1(open, update, nil))
		})
	}

	// A root the document does not hold at all is still refused where the
	// decode resolves its name, before anything under it is read.
	fresh := guardedDoc(t, base)
	fresh.SetAllowedRoots(onlyText)
	freshBefore := stateOf(fresh)
	refused := requireRefused(t, fresh, freshBefore,
		ApplyUpdateV1(fresh, rootNamedUpdate("fresh", "first", " and more"), nil), ErrRootNotAllowed)
	if refused.Root != "fresh" {
		t.Fatalf("the refusal reports root %q, want \"fresh\"", refused.Root)
	}
	if refused.Structs != 0 {
		t.Fatalf("the refusal reports %d structs, want none: the name check refuses before one integrates", refused.Structs)
	}
}

// parkedStructs is an update of n one-character structs for client 2 at
// clocks 1 to n, each naming the one before it as its left origin. Clock 0 is
// never sent, so a document that has not seen it parks every one of them.
func parkedStructs(n int) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(uint64(n))
	enc.WriteVarUint(2) // client
	enc.WriteVarUint(1) // first clock: clock 0 is the gap
	for i := range n {
		enc.WriteUint8(wireString | flagHasOrigin)
		enc.WriteVarUint(2)         // the left origin's client
		enc.WriteVarUint(uint64(i)) // its clock, the struct before this one
		enc.WriteVarString("x")
	}
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// pendingState is everything an update refused by the pending cap must leave
// as it was: the document's whole encoded state and what it holds parked.
type pendingState struct {
	state   string
	items   int
	ranges  int
	structs int
}

func pendingStateOf(doc *Doc) pendingState {
	stats := doc.PendingStats()
	return pendingState{
		state:   string(EncodeStateAsUpdateV1(doc, nil)),
		items:   stats.Items,
		ranges:  stats.DeleteRanges,
		structs: doc.StructCount(),
	}
}

// The pending cap refuses with ErrPendingCap, which is not the refusal
// unreadable bytes get. The two want opposite answers from a server: bytes
// that do not read mean the sender is broken and may be dropped for good,
// while the cap means only that this update waits on clocks the document has
// not seen, which is transient - the sender stays connected and the update
// lands once its gap is filled. Both still wrap ErrInvalidUpdate, so a
// caller that only asks whether the update was taken reads the same answer
// as before.
func TestUnit_PendingCap_IsItsOwnRefusalAndStillAnInvalidUpdate(t *testing.T) {
	const limit = 5

	t.Run("structs past the cap", func(t *testing.T) {
		doc := New(WithClientID(7), WithMaxPendingItems(limit))
		if err := ApplyUpdateV1(doc, parkedStructs(limit), nil); err != nil {
			t.Fatalf("%d structs, the cap, were refused: %v", limit, err)
		}
		before := pendingStateOf(doc)
		if before.items != limit {
			t.Fatalf("%d structs are parked, want the %d the cap holds", before.items, limit)
		}

		err := ApplyUpdateV1(doc, parkedStructs(4*limit), nil)
		if !errors.Is(err, ErrPendingCap) {
			t.Fatalf("the update was refused with %v, want ErrPendingCap", err)
		}
		if !errors.Is(err, ErrInvalidUpdate) {
			t.Fatalf("ErrPendingCap does not wrap ErrInvalidUpdate: %v", err)
		}
		if after := pendingStateOf(doc); after != before {
			t.Fatalf("the refused update changed the document:\nbefore %+v\nafter  %+v", before, after)
		}
	})

	t.Run("delete ranges past the cap", func(t *testing.T) {
		doc := New(WithClientID(7), WithMaxPendingItems(limit))
		if err := ApplyUpdateV1(doc, parkedDeletes(98, limit), nil); err != nil {
			t.Fatalf("%d ranges, the cap, were refused: %v", limit, err)
		}
		before := pendingStateOf(doc)
		if before.ranges != limit {
			t.Fatalf("%d delete ranges are parked, want the %d the cap holds", before.ranges, limit)
		}

		err := ApplyUpdateV1(doc, parkedDeletes(99, limit+1), nil)
		if !errors.Is(err, ErrPendingCap) {
			t.Fatalf("the delete set was refused with %v, want ErrPendingCap", err)
		}
		if !errors.Is(err, ErrInvalidUpdate) {
			t.Fatalf("ErrPendingCap does not wrap ErrInvalidUpdate: %v", err)
		}
		if after := pendingStateOf(doc); after != before {
			t.Fatalf("the refused delete set changed the document:\nbefore %+v\nafter  %+v", before, after)
		}
	})

	t.Run("bytes that do not read are not the cap", func(t *testing.T) {
		// Each of these carries too little to park anything, so what refuses
		// them can only be that they do not read. A frame that declares more
		// structs than the room and is cut after them is refused by the cap
		// first, and rightly: as far as it reads, it would park past it.
		one := parkedStructs(1)
		for name, update := range map[string][]byte{
			"a frame cut at its header": one[:1],
			"a struct cut short":        one[:len(one)-2],
		} {
			t.Run(name, func(t *testing.T) {
				doc := New(WithClientID(7), WithMaxPendingItems(limit))
				err := ApplyUpdateV1(doc, update, nil)
				if !errors.Is(err, ErrInvalidUpdate) {
					t.Fatalf("the malformed update was applied with err %v", err)
				}
				if errors.Is(err, ErrPendingCap) {
					t.Fatalf("a malformed update was refused as the pending cap: %v", err)
				}
			})
		}
	})
}

// Split bounds the structs a delete makes of items the document already
// held, which Structs excludes and no count used to bound. A document whose
// structs are near the count its next load can decode needs it: a delete
// integrates nothing of its own, so Structs: 0 takes it, and the document is
// left with structs it did not have. Measured here on the one run of "hello
// world": a delete of five characters inside it cuts that one item into
// three, two structs more than the document held, from an update whose
// Structs count is 0.
func TestUnit_ApplyBudget_RefusesADeleteWhoseSplitsPassWhatItAllows(t *testing.T) {
	const base = "hello world"
	deleteOnly := editUpdate(t, base, func(txn *Transaction, text *YText) { text.Delete(txn, 3, 5) })

	doc := budgetDoc(t, base)
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: 1}
	})
	before := stateOf(doc)
	updates := observed(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, deleteOnly, nil), ErrApplyBudget)
	if refused.Split != 2 {
		t.Fatalf("the refusal reports %d splits, want the 2 the delete's edges make", refused.Split)
	}
	if refused.Structs != 0 {
		t.Fatalf("the refusal reports %d structs, want none: a delete integrates nothing of its own", refused.Structs)
	}
	if *updates != 0 {
		t.Fatalf("the refused update was handed to %d observers", *updates)
	}

	// A budget that holds the splits takes the same delete, and the document
	// keeps the structs they added, which is the cost Split bounds.
	fits := budgetDoc(t, base)
	fits.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: 2}
	})
	fitsBefore := stateOf(fits)
	requireApplied(t, fits, fitsBefore, ApplyUpdateV1(fits, deleteOnly, nil))
	if got, want := fits.GetText("t").ToString(), appliedText(t, base, deleteOnly); got != want {
		t.Fatalf("the applied delete left %q, want %q", got, want)
	}
	if got, want := fits.StructCount(), fitsBefore.structs+2; got != want {
		t.Fatalf("the applied delete left %d structs, want the %d it held and the 2 its splits added", got, want)
	}

	// Split: 0 is "this update may split nothing", and an update that splits
	// nothing is taken under it: the delete above is refused, and an insert
	// at the head of the run, which splits nothing, applies.
	none := func(any) ApplyBudget { return ApplyBudget{Decoded: -1, Structs: -1, Bytes: -1, Split: 0} }
	strict := budgetDoc(t, base)
	strict.SetApplyBudget(none)
	strictBefore := stateOf(strict)
	requireRefused(t, strict, strictBefore, ApplyUpdateV1(strict, deleteOnly, nil), ErrApplyBudget)
	head := editUpdateFrom(t, base, 3, func(txn *Transaction, text *YText) { text.Insert(txn, 0, "X", nil) })
	requireApplied(t, strict, strictBefore, ApplyUpdateV1(strict, head, nil))
}

// A struct of the update that parks counts against Structs and its content
// against Bytes where it parks, and the apply that fills the gap is charged
// nothing for it.
//
// A parked struct used to cost nothing in any measure, so a sender could
// hand a document any number of structs of any size by writing them behind
// a clock it never sent: the document holds each of them in memory, returns
// them in its encoded state and integrates them as soon as the gap is
// filled, all of it under a budget of Structs: 0 and Bytes: 0. The charge is
// the arriving update's alone, so the peer that later fills the gap is not
// refused for what somebody else parked.
func TestUnit_ApplyBudget_CountsTheStructsAnUpdateParks(t *testing.T) {
	const n = 5 // n one-character structs of client 2, behind the gap at clock 0
	update := parkedStructs(n)

	structs := New(WithClientID(7))
	structs.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: n - 1, Bytes: -1, Split: -1}
	})
	before := stateOf(structs)
	refused := requireRefused(t, structs, before, ApplyUpdateV1(structs, update, nil), ErrApplyBudget)
	if refused.Structs != n {
		t.Fatalf("the refusal reports %d structs, want the %d the update parked", refused.Structs, n)
	}
	if structs.PendingStats().Items != 0 {
		t.Fatalf("the refused update left %d structs parked, want none", structs.PendingStats().Items)
	}

	content := New(WithClientID(7))
	content.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: -1, Bytes: n - 1, Split: -1}
	})
	contentBefore := stateOf(content)
	refusedBytes := requireRefused(t, content, contentBefore, ApplyUpdateV1(content, update, nil), ErrApplyBudget)
	if refusedBytes.Bytes != n {
		t.Fatalf("the refusal reports %d bytes, want the %d bytes the %d parked structs carry", refusedBytes.Bytes, n, n)
	}

	// A budget that holds them parks them, and the update that fills the gap
	// is charged for its own one struct and byte alone, though the drain it
	// runs integrates all n parked structs.
	doc := New(WithClientID(7))
	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: n, Bytes: n, Split: -1}
	})
	fitsBefore := stateOf(doc)
	requireApplied(t, doc, fitsBefore, ApplyUpdateV1(doc, update, nil))
	if got := doc.PendingStats().Items; got != n {
		t.Fatalf("%d structs parked, want the %d the update carried", got, n)
	}

	doc.SetApplyBudget(func(any) ApplyBudget {
		return ApplyBudget{Decoded: -1, Structs: 1, Bytes: 1, Split: -1}
	})
	if err := ApplyUpdateV1(doc, gapStringUpdate(0, "A", nil), nil); err != nil {
		t.Fatalf("the update that fills the gap was refused: %v", err)
	}
	if got := doc.PendingStats().Items; got != 0 {
		t.Fatalf("%d structs are still parked, want the drain to have taken them all", got)
	}
	if got, want := doc.GetText("t").ToString(), "A"+strings.Repeat("x", n); got != want {
		t.Fatalf("the document reads %q, want %q", got, want)
	}
}

// parkedUnderRoot is an update of one string struct for client 2 at clock 1,
// parented to the named root: clock 0 is never sent, so a document that has
// not seen it parks the struct with its parent already resolved. The root is
// one the document holds, so the name check in the decode lets it through
// (refuseRootOnBoundedDoc) and what must refuse it is the containment check
// over what the apply adds.
func parkedUnderRoot(root, value string) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(2) // client
	enc.WriteVarUint(1) // first clock: clock 0 is the gap
	enc.WriteUint8(wireString)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteVarString(value)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// An allowed-roots list keeps a sender out of a root whether its struct
// integrates or parks. A struct that parks is stored, encoded into the
// document's state and integrated as soon as its gap is filled, so a check
// made only where a struct integrates let a sender write into a root the
// owner allows it nothing under: it had only to park the struct behind a
// clock it never sent.
func TestUnit_AllowedRoots_RefusesAStructThatParksUnderADisallowedRoot(t *testing.T) {
	const base = "hello world"
	update := parkedUnderRoot("m", "forged")

	doc := guardedDoc(t, base)
	doc.SetAllowedRoots(func(any) []string { return []string{"t"} })
	before := stateOf(doc)
	refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrRootNotAllowed)
	if refused.Root != "m" {
		t.Fatalf("the refusal reports root %q, want \"m\"", refused.Root)
	}
	if got := doc.PendingStats().Items; got != 0 {
		t.Fatalf("the refused update left %d structs parked", got)
	}
	// Nothing of it is in the document's stored state either, so no later
	// load of that state can bring it back.
	reloaded := New(WithClientID(8))
	if err := ApplyUpdateV1(reloaded, EncodeStateAsUpdateV1(doc, nil), nil); err != nil {
		t.Fatalf("the stored state of the refused document does not load: %v", err)
	}
	if got := reloaded.PendingStats().Items; got != 0 {
		t.Fatalf("a load of the stored state brought back %d parked structs", got)
	}

	// The control: with no list the same struct parks, and the document
	// carries it in its stored state to every peer that loads it - which is
	// what the refusal above keeps out.
	open := guardedDoc(t, base)
	if err := ApplyUpdateV1(open, update, nil); err != nil {
		t.Fatalf("the document with no list refused the update: %v", err)
	}
	if got := open.PendingStats().Items; got != 1 {
		t.Fatalf("%d structs parked with no list, want the one the update carried", got)
	}
	carried := New(WithClientID(8))
	if err := ApplyUpdateV1(carried, EncodeStateAsUpdateV1(open, nil), nil); err != nil {
		t.Fatalf("the stored state of the open document does not load: %v", err)
	}
	if got := carried.PendingStats().Items; got != 1 {
		t.Fatalf("the stored state carried %d parked structs, want the one it parked", got)
	}

	// A struct that parks under a root the list names is taken, so what the
	// check refuses is the root and not the parking.
	allowed := guardedDoc(t, base)
	allowed.SetAllowedRoots(func(any) []string { return []string{"t"} })
	if err := ApplyUpdateV1(allowed, parkedUnderRoot("t", "later"), nil); err != nil {
		t.Fatalf("a struct parking under the allowed root was refused: %v", err)
	}
	if got := allowed.PendingStats().Items; got != 1 {
		t.Fatalf("%d structs parked under the allowed root, want one", got)
	}
}

// deletedRangeUpdate is an update of one struct for client 9 under the root
// name: a deleted run of length clocks, which carries no content at all and
// leaves the document holding a range of that length in its delete set.
func deletedRangeUpdate(root string, length uint64) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireDeleted)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteVarUint(length)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// Bytes counts a map key an item hangs under, which the document keeps for as
// long as the entry lives, but NOT the span of a deleted run, which carries no
// content and costs the document a range record of two integers whatever span
// it names. A delete's cost is the number of ranges it carries (Decoded), not
// their length.
func TestUnit_ApplyBudget_BytesCountsAMapKeyButNotADeletedRange(t *testing.T) {
	const base = "hello world"

	t.Run("a map key", func(t *testing.T) {
		const key, value = 1_000, "v"
		long := strings.Repeat("k", key)
		update := guardedEdit(t, base, 2, func(txn *Transaction, _ *YText, m *YMap) {
			m.Set(txn, long, value)
		})
		want := key + len(value)

		doc := guardedDoc(t, base)
		doc.SetApplyBudget(func(any) ApplyBudget {
			return ApplyBudget{Decoded: -1, Structs: -1, Bytes: want - 1, Split: -1}
		})
		before := stateOf(doc)
		refused := requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrApplyBudget)
		if refused.Bytes != want {
			t.Fatalf("the refusal reports %d bytes, want the %d of the key and its value", refused.Bytes, want)
		}

		fits := guardedDoc(t, base)
		fits.SetApplyBudget(func(any) ApplyBudget {
			return ApplyBudget{Decoded: -1, Structs: -1, Bytes: want, Split: -1}
		})
		requireApplied(t, fits, stateOf(fits), ApplyUpdateV1(fits, update, nil))
		if got, _ := fits.GetMap("m").Get(long); got != value {
			t.Fatalf("the applied update left %v under the long key, want %q", got, value)
		}
	})

	t.Run("a deleted range is not charged its span", func(t *testing.T) {
		// A deleted run of a span far past any byte budget: it carries no
		// content, so a byte budget of zero must still admit it, and the span
		// it claims is not charged.
		const span = 1 << 40
		update := deletedRangeUpdate("t", span)

		fits := guardedDoc(t, base)
		fits.SetApplyBudget(func(any) ApplyBudget {
			return ApplyBudget{Decoded: -1, Structs: -1, Bytes: 0, Split: -1}
		})
		fitsBefore := stateOf(fits)
		requireApplied(t, fits, fitsBefore, ApplyUpdateV1(fits, update, nil))
		if got := fitsBefore.deletes; got == stateOf(fits).deletes {
			t.Fatalf("the applied run left the delete set at %s, so nothing was deleted", got)
		}
	})
}
