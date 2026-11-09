package crdt

import (
	"bytes"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// Tests for WithRefusedContent, the set of content kinds a remote update may
// not carry.

// kindUpdate is an update of one struct for client 9 under the root name,
// carrying the content tag writes. It is hand-encoded because the kinds this
// guards are ones no writer in this package produces: a browser sends them
// and nothing here does.
func kindUpdate(root string, tag byte, writes func(enc *encoding.Encoder)) []byte {
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(1) // one struct
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(tag)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	writes(enc)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// embedUpdate is an update of one embed under the root, the shape a browser
// sends for a node its editor allows and another owner's schema has none of.
func embedUpdate(root string) []byte {
	return kindUpdate(root, wireEmbed, func(enc *encoding.Encoder) {
		enc.WriteVarString(`{"image":"x.png"}`) // V1 writes an embed as JSON text
	})
}

// binaryUpdate is an update of one binary struct under the root.
func binaryUpdate(root string) []byte {
	return kindUpdate(root, wireBinary, func(enc *encoding.Encoder) {
		enc.WriteVarBytes([]byte{1, 2, 3, 4})
	})
}

// stringUpdate is an update of one string struct under the root: a kind the
// sets below do not name, so what it proves is that they refuse the kinds
// they name and nothing else.
func stringUpdate(root string) []byte {
	return kindUpdate(root, wireString, func(enc *encoding.Encoder) {
		enc.WriteVarString("plain")
	})
}

// stringThenEmbedUpdate is an update of two structs for client 9 under the
// root name: the string "plain", and an embed written beside it, whose left
// origin is the string's last clock. The string integrates before the decode
// reads the embed, so an update refused for the embed is one the apply had
// already begun.
func stringThenEmbedUpdate(root string) []byte {
	const plain = "plain"
	enc := encoding.NewEncoder()
	enc.WriteVarUint(1) // clients
	enc.WriteVarUint(2) // two structs
	enc.WriteVarUint(9) // client
	enc.WriteVarUint(0) // first clock
	enc.WriteUint8(wireString)
	enc.WriteUint8(1) // parent info: a named root follows
	enc.WriteVarString(root)
	enc.WriteVarString(plain)
	enc.WriteUint8(wireEmbed | flagHasOrigin)
	enc.WriteVarUint(9)                      // the left origin's client
	enc.WriteVarUint(uint64(len(plain) - 1)) // its clock: the string's last
	enc.WriteVarString(`{"image":"x.png"}`)
	enc.WriteVarUint(0) // delete set: no clients
	return bytes.Clone(enc.Bytes())
}

// A document refuses a remote update carrying a kind of content its owner
// does not take, and leaves itself as it was.
//
// No count can do this. One embed is one struct of a few bytes, within any
// budget an owner can set, and once it is in the document every reader of
// that document is shown content its schema has no node for: one crafted
// edit takes a document out of editing for everyone until a person repairs
// it. The refusal is the whole apply's, as every refusal of a bounded
// document is, so nothing the update carried is kept, and the counts are
// left to bound what they bound.
func TestUnit_RefusedContent_RefusesTheKindsTheOwnerNames(t *testing.T) {
	const base = "hello world"
	noEmbedOrBinary := func(any) []ContentKind {
		return []ContentKind{ContentKindEmbed, ContentKindBinary}
	}

	for name, update := range map[string][]byte{
		"an embed": embedUpdate("t"),
		"binary":   binaryUpdate("t"),
	} {
		t.Run(name, func(t *testing.T) {
			doc := budgetDoc(t, base)
			doc.SetRefusedContent(noEmbedOrBinary)
			before := stateOf(doc)
			updates := observed(doc)
			requireRefused(t, doc, before, ApplyUpdateV1(doc, update, nil), ErrContentNotAllowed)
			if *updates != 0 {
				t.Fatalf("the refused update was handed to %d observers", *updates)
			}

			// The V2 form of the same update is refused the same way.
			v2, err := UpdateV1ToV2(update)
			if err != nil {
				t.Fatal(err)
			}
			v2doc := budgetDoc(t, base)
			v2doc.SetRefusedContent(noEmbedOrBinary)
			v2Before := stateOf(v2doc)
			requireRefused(t, v2doc, v2Before, ApplyUpdateV2(v2doc, v2, nil), ErrContentNotAllowed)

			// The control: the same update with no set applies and the
			// document keeps the struct, which is what the refusal keeps out.
			open := budgetDoc(t, base)
			openBefore := stateOf(open)
			requireApplied(t, open, openBefore, ApplyUpdateV1(open, update, nil))
			if got := open.StructCount(); got != openBefore.structs+1 {
				t.Fatalf("the document with no set holds %d structs, want the %d before it and the one it took",
					got, openBefore.structs)
			}
		})
	}

	// A kind the set does not name is taken under the same set, so what
	// refuses the updates above is the kind and not the shape.
	plain := budgetDoc(t, base)
	plain.SetRefusedContent(noEmbedOrBinary)
	plainBefore := stateOf(plain)
	requireApplied(t, plain, plainBefore, ApplyUpdateV1(plain, stringUpdate("t"), nil))

	// An update is refused for a kind anywhere in it: here the embed follows
	// a string the set allows, written beside it, which has integrated by
	// the time the embed is read. requireRefused is what checks that the
	// string is gone again.
	mixed := budgetDoc(t, base)
	mixed.SetRefusedContent(noEmbedOrBinary)
	mixedBefore := stateOf(mixed)
	requireRefused(t, mixed, mixedBefore, ApplyUpdateV1(mixed, stringThenEmbedUpdate("t"), nil), ErrContentNotAllowed)

	// The control: the same two structs with no set leave both in the
	// document, so the refusal above took back a string that had landed.
	open := budgetDoc(t, base)
	openBefore := stateOf(open)
	requireApplied(t, open, openBefore, ApplyUpdateV1(open, stringThenEmbedUpdate("t"), nil))
	if got := open.StructCount(); got != openBefore.structs+2 {
		t.Fatalf("the document with no set holds %d structs, want the %d before it and the two it took",
			got, openBefore.structs)
	}
}

// A document with no set of refused kinds takes every kind, exactly as one
// without the option does: the zero value of the option is today's
// behaviour. A nil callback and one answering nothing are both that.
func TestUnit_RefusedContent_ADocumentWithNoSetTakesEveryKind(t *testing.T) {
	const base = "hello world"
	for name, kinds := range map[string]func(any) []ContentKind{
		"a nil callback":  nil,
		"a nil answer":    func(any) []ContentKind { return nil },
		"an empty answer": func(any) []ContentKind { return []ContentKind{} },
	} {
		t.Run(name, func(t *testing.T) {
			for what, update := range map[string][]byte{
				"an embed": embedUpdate("t"),
				"binary":   binaryUpdate("t"),
			} {
				doc := budgetDoc(t, base)
				doc.SetRefusedContent(kinds)
				if err := ApplyUpdateV1(doc, update, nil); err != nil {
					t.Fatalf("%s refused %s: %v", name, what, err)
				}

				// It reads exactly as a document that never had the option.
				open := budgetDoc(t, base)
				if err := ApplyUpdateV1(open, update, nil); err != nil {
					t.Fatalf("the document without the option refused %s: %v", what, err)
				}
				if got, want := stateOf(doc), stateOf(open); got != want {
					t.Fatalf("%s left\n%+v\nand a document without the option left\n%+v", name, got, want)
				}
			}
		})
	}
}

// A callback that panics refuses the update and leaves the document as it
// was, as the budget and roots callbacks do.
func TestUnit_RefusedContent_APanickingCallbackRefusesTheUpdate(t *testing.T) {
	const base = "hello world"
	doc := budgetDoc(t, base)
	doc.SetRefusedContent(func(any) []ContentKind { panic("boom in the kinds") })
	before := stateOf(doc)
	err := ApplyUpdateV1(doc, stringUpdate("t"), nil)
	requireRefused(t, doc, before, err, ErrInvalidUpdate)
	if msg := err.Error(); !strings.Contains(msg, "WithRefusedContent callback") || !strings.Contains(msg, "boom in the kinds") {
		t.Fatalf("the error does not name the callback and its panic: %v", err)
	}

	doc.SetRefusedContent(nil)
	requireApplied(t, doc, before, ApplyUpdateV1(doc, stringUpdate("t"), nil))
}
