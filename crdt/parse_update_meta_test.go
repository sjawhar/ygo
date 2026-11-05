package crdt

import (
	"errors"
	"maps"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// requireMeta requires ParseUpdateMeta, and its V2 form on the same update
// converted, to answer from and to.
func requireMeta(t *testing.T, update []byte, from, to StateVector) {
	t.Helper()
	gotFrom, gotTo, err := ParseUpdateMeta(update)
	if err != nil {
		t.Fatalf("V1: %v", err)
	}
	if !maps.Equal(gotFrom, from) || !maps.Equal(gotTo, to) {
		t.Fatalf("V1: from %v to %v, want from %v to %v", gotFrom, gotTo, from, to)
	}
	v2, err := UpdateV1ToV2(update)
	if err != nil {
		t.Fatalf("the update does not convert to V2: %v", err)
	}
	gotFrom, gotTo, err = ParseUpdateMetaV2(v2)
	if err != nil {
		t.Fatalf("V2: %v", err)
	}
	if !maps.Equal(gotFrom, from) || !maps.Equal(gotTo, to) {
		t.Fatalf("V2: from %v to %v, want from %v to %v", gotFrom, gotTo, from, to)
	}
}

// requireMetaAgreesWithTheStateVector requires to, which ParseUpdateMeta
// answers, to be the state vector EncodeStateVectorFromUpdate encodes for the
// same update: both read the structs in wire order and keep where each client
// ends, and for an update whose clocks start at zero and hold no Skip that is
// the same answer.
func requireMetaAgreesWithTheStateVector(t *testing.T, update []byte) {
	t.Helper()
	_, to, err := ParseUpdateMeta(update)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeStateVectorFromUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	sv, err := DecodeStateVectorV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(to, sv) {
		t.Fatalf("to is %v, and EncodeStateVectorFromUpdate reports %v", to, sv)
	}
}

// ParseUpdateMeta reports where each client's structs begin and end, for one
// client and for several, and agrees with EncodeStateVectorFromUpdate on where
// they end.
func TestUnit_ParseUpdateMeta_ReportsEachClientsFirstAndLastClock(t *testing.T) {
	one := New(WithClientID(1))
	text := one.GetText("t")
	one.Transact(func(txn *Transaction) { text.Insert(txn, 0, "hello", nil) })
	oneUpdate := EncodeStateAsUpdateV1(one, nil)
	requireMeta(t, oneUpdate, StateVector{1: 0}, StateVector{1: 5})
	requireMetaAgreesWithTheStateVector(t, oneUpdate)

	// A second client that edits the same text, so the merged state carries
	// both clients' structs.
	two := New(WithClientID(2))
	if err := ApplyUpdateV1(two, oneUpdate, nil); err != nil {
		t.Fatal(err)
	}
	twoText := two.GetText("t")
	two.Transact(func(txn *Transaction) { twoText.Insert(txn, 5, " world", nil) })
	both := EncodeStateAsUpdateV1(two, nil)
	requireMeta(t, both, StateVector{1: 0, 2: 0}, StateVector{1: 5, 2: 6})
	requireMetaAgreesWithTheStateVector(t, both)

	// What one client wrote after a state vector starts at the clock it had
	// reached, not at zero.
	sv := two.StateVector()
	two.Transact(func(txn *Transaction) { twoText.Insert(txn, 0, "oh ", nil) })
	later := EncodeStateAsUpdateV1(two, sv)
	requireMeta(t, later, StateVector{2: 6}, StateVector{2: 9})
}

// An update of a document with nothing in it names no client, and is not an
// error.
func TestUnit_ParseUpdateMeta_AnEmptyUpdateNamesNoClient(t *testing.T) {
	empty := EncodeStateAsUpdateV1(New(), nil)
	from, to, err := ParseUpdateMeta(empty)
	if err != nil {
		t.Fatalf("the empty update is an error: %v", err)
	}
	if len(from) != 0 || len(to) != 0 {
		t.Fatalf("the empty update names from %v to %v, want neither", from, to)
	}
	requireMetaAgreesWithTheStateVector(t, empty)
}

// A malformed update is an error, not a panic, in either form: a header that
// names more structs than the bytes hold, a truncated update, and bytes that
// are no update at all.
func TestUnit_ParseUpdateMeta_AMalformedUpdateIsAnError(t *testing.T) {
	doc := New(WithClientID(1))
	text := doc.GetText("t")
	doc.Transact(func(txn *Transaction) { text.Insert(txn, 0, "hello", nil) })
	whole := EncodeStateAsUpdateV1(doc, nil)

	truncated := append([]byte(nil), whole[:len(whole)/2]...)
	manyStructs := encoding.EncodeBytes(func(enc *encoding.Encoder) {
		enc.WriteVarUint(1)        // one client group
		enc.WriteVarUint(1 << 40)  // more structs than any update holds
		enc.WriteVarUint(1)        // client
		enc.WriteVarUint(0)        // first clock
		enc.WriteUint8(wireString) // one struct, whose content follows
		enc.WriteVarString("the group's one and only struct")
	})
	for name, update := range map[string][]byte{
		"a truncated update": truncated,
		"too many structs":   manyStructs,
		"no update at all":   {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
	} {
		if _, _, err := ParseUpdateMeta(update); err == nil {
			t.Fatalf("%s was read as a V1 update", name)
		} else if !errors.Is(err, ErrInvalidUpdate) {
			t.Fatalf("%s gives %v, want an error wrapping ErrInvalidUpdate", name, err)
		}
		if _, _, err := ParseUpdateMetaV2(update); err == nil {
			t.Fatalf("%s was read as a V2 update", name)
		}
	}
}
