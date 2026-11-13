package crdt

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/encoding"
)

// SplitStateAsUpdateV1 sends what SplitUpdateV1 sends for the encoded state,
// byte for byte, in every room below and to every receiver: reading a
// document's own structs changes what the split costs, never what it sends.
func TestUnit_SplitStateAsUpdateV1_MatchesTheSplitOfTheEncodedState(t *testing.T) {
	for _, room := range splitRooms(t) {
		t.Run(room.name, func(t *testing.T) {
			for _, sv := range receiverStates(room.doc) {
				for _, maxBytes := range []int{200, 1 << 10, 64<<10 - 6} {
					want, wantErr := SplitUpdateV1(EncodeStateAsUpdateV1(room.doc, sv), sv, maxBytes)
					got, err := SplitStateAsUpdateV1(room.doc, sv, maxBytes)
					require.Equal(t, wantErr, err, "sv %v, maxBytes %d", sv, maxBytes)
					requireSameSeries(t, want, got, fmt.Sprintf("sv %v, maxBytes %d", sv, maxBytes))
				}
			}
		})
	}
}

// A room holding parked structs and deletions is cut from its encoded state,
// by SplitUpdateV1 and the walk it runs over the structs it decodes: what the
// room parks reaches its state only through withParked, so the walk over the
// room's own structs, which answers every room holding nothing parked, would
// leave it out. The answer is the split of the encoded state and not that
// walk's, and a receiver that applies it holds what the room holds, parked
// structs and deletions included.
func TestUnit_SplitStateAsUpdateV1_CutsARoomHoldingParkedStructsFromItsEncodedState(t *testing.T) {
	room := parkedRoom(t)
	parked := room.PendingStats()
	for _, sv := range receiverStates(room) {
		for _, maxBytes := range []int{200, 1 << 10, 64<<10 - 6} {
			what := fmt.Sprintf("sv %v, maxBytes %d", sv, maxBytes)
			got, err := SplitStateAsUpdateV1(room, sv, maxBytes)
			require.NoError(t, err, what)

			want, err := SplitUpdateV1(EncodeStateAsUpdateV1(room, sv), sv, maxBytes)
			require.NoError(t, err, what)
			requireSameSeries(t, want, got, what)

			queues := make([]splitQueue, 0, len(room.store.clients))
			for c, items := range room.store.clients {
				queues = appendUnsent(queues, c, items, sv)
			}
			own := newUpdateSplitter(queues, buildDeleteSet(room.store), sv, maxBytes, true)
			own.run()
			require.NoError(t, own.err, what)
			require.NotEqual(t, own.out, got, "the walk over the room's own structs leaves out what it parks; %s", what)

			if sv != nil {
				continue
			}
			receiver := New(WithClientID(99))
			for i, u := range got {
				require.NoError(t, ApplyUpdateV1(receiver, u, nil), "update %d of %d; %s", i+1, len(got), what)
			}
			require.Equal(t, room.StateVector(), receiver.StateVector(), what)
			require.Equal(t, room.GetText("t").ToString(), receiver.GetText("t").ToString(), what)
			require.Equal(t, parked, receiver.PendingStats(), "the receiver parks what the room parks; %s", what)
		}
	}
}

// requireSameSeries fails at the first update of got that differs from want,
// naming the byte where it does.
func requireSameSeries(t *testing.T, want, got [][]byte, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d updates in the series, want %d; %s", len(got), len(want), what)
	}
	for i := range want {
		if bytes.Equal(want[i], got[i]) {
			continue
		}
		at := 0
		for at < len(want[i]) && at < len(got[i]) && want[i][at] == got[i][at] {
			at++
		}
		t.Fatalf("update %d of %d differs at byte %d: %d bytes, want %d; %s",
			i+1, len(want), at, len(got[i]), len(want[i]), what)
	}
}

// receiverStates are the state vectors of receivers doc is split for: one
// holding nothing, one holding half of each client's clocks, which falls
// inside a struct wherever a struct spans it, one holding every other client
// whole, one holding more than doc of every client, and one holding only a
// client doc never had.
func receiverStates(doc *Doc) []StateVector {
	full := doc.StateVector()
	clients := make([]ClientID, 0, len(full))
	for c := range full {
		clients = append(clients, c)
	}
	slices.SortFunc(clients, cmp.Compare)
	half, mixed, ahead := StateVector{}, StateVector{}, StateVector{}
	for i, c := range clients {
		half[c] = full[c] / 2
		if i%2 == 0 {
			mixed[c] = full[c]
		}
		ahead[c] = full[c] + 3
	}
	return []StateVector{nil, half, mixed, ahead, {999: 5}}
}

type splitRoom struct {
	name string
	doc  *Doc
}

// splitRooms are documents holding every kind of struct the split reads
// otherwise than the decode of an encoded state would hand it: a struct cut at
// a receiver's clock, a keyed struct with an origin, whose parentSub the
// decode drops, a value the decode reads back as another type, a struct
// longer than an update, parked structs and deletions, and garbage-collected
// content. Each asserts that it holds what it is there for.
func splitRooms(t *testing.T) []splitRoom {
	t.Helper()
	typed := func(deleteEvery int) *Doc {
		doc := New(WithClientID(3_000_000_000))
		txt := doc.GetText("t")
		for i := range 5000 {
			doc.Transact(func(txn *Transaction) { txt.Insert(txn, i/2, "a", nil) })
		}
		for at := 0; deleteEvery > 0 && at+5 < txt.Len(); at += deleteEvery {
			doc.Transact(func(txn *Transaction) { txt.Delete(txn, at, 5) })
		}
		return doc
	}
	return []splitRoom{
		{"characters typed into the middle", typed(0)},
		{"characters typed into the middle, some deleted", typed(37)},
		{"clients writing into one another's text", interleavedRoom(t)},
		{"a struct whose origin is another client's", crossOriginRoom(t)},
		{"structs longer than an update", longStructRoom(t)},
		{"maps, nested types and XML", keyedRoom(t)},
		{"values read back as other types", rereadRoom(t)},
		{"parked structs and deletions", parkedRoom(t)},
		{"collected content", collectedRoom(t)},
	}
}

// interleavedRoom is four clients' text, each round of each client written
// where the others' are, with some of it deleted.
func interleavedRoom(t *testing.T) *Doc {
	docs := make([]*Doc, 4)
	for i := range docs {
		docs[i] = New(WithClientID(ClientID(1000 + i)))
	}
	for round := range 30 {
		for i, d := range docs {
			txt := d.GetText("t")
			d.Transact(func(txn *Transaction) {
				at := 0
				if n := txt.Len(); n > 0 {
					at = (round*7 + i) % n
				}
				txt.Insert(txn, at, strings.Repeat(string(rune('a'+i)), 9), nil)
				if round%5 == 4 {
					txt.Delete(txn, at/2, 4)
				}
			})
		}
		for i, src := range docs {
			u := EncodeStateAsUpdateV1(src, nil)
			for j, dst := range docs {
				if i != j {
					require.NoError(t, ApplyUpdateV1(dst, u, nil))
				}
			}
		}
	}
	require.Len(t, docs[0].StateVector(), len(docs))
	return docs[0]
}

// crossOriginRoom holds a struct of one client whose origin is the other
// client's last clock. Cut at a receiver's clock, it names the clock before
// the cut instead, so a receiver holding half of each client's clocks can
// take it before the other client's rest.
func crossOriginRoom(t *testing.T) *Doc {
	a, doc := New(WithClientID(50)), New(WithClientID(60))
	aText, text := a.GetText("t"), doc.GetText("t")
	a.Transact(func(txn *Transaction) { aText.Insert(txn, 0, strings.Repeat("a", 10), nil) })
	require.NoError(t, ApplyUpdateV1(doc, EncodeStateAsUpdateV1(a, nil), nil))
	doc.Transact(func(txn *Transaction) { text.Insert(txn, 10, strings.Repeat("b", 400), nil) })
	own := doc.store.clients[60]
	require.Len(t, own, 1)
	require.Equal(t, &ID{Client: 50, Clock: 9}, own[0].Origin, "the struct's origin is the other client's")
	return doc
}

// longStructRoom holds a text struct longer than any update, one of
// supplementary characters, which is cut only between characters, and a map
// value longer than any update, which is not cut at all.
func longStructRoom(t *testing.T) *Doc {
	doc := New(WithClientID(7))
	txt, emoji, m := doc.GetText("t"), doc.GetText("e"), doc.GetMap("m")
	doc.Transact(func(txn *Transaction) {
		txt.Insert(txn, 0, strings.Repeat("z", 200_000), nil)
		emoji.Insert(txn, 0, strings.Repeat("😀", 40_000), nil)
		m.Set(txn, "big", strings.Repeat("b", 100_000))
	})
	// The text a receiver holding half of it already has a part of: written
	// right after it, the next run joins the same struct.
	doc.Transact(func(txn *Transaction) { txt.Insert(txn, 200_000, "more", nil) })
	longest := uint64(0)
	for _, it := range doc.store.clients[7] {
		longest = max(longest, it.Content.Len())
	}
	require.Greater(t, longest, uint64(64<<10), "the room holds a struct longer than an update")
	return doc
}

// keyedRoom holds map entries written over, whose structs name an origin and
// a key, nested maps and arrays whose structs name their container, and XML
// elements with attributes written over and text inside. Its last struct is a
// map entry of three clocks with no origin, written by hand, which a cut
// leaves keyed on both sides.
func keyedRoom(t *testing.T) *Doc {
	doc := New(WithClientID(11))
	m := doc.GetMap("m")
	doc.Transact(func(txn *Transaction) {
		for i := range 300 {
			m.Set(txn, fmt.Sprintf("k%d", i%40), fmt.Sprintf("v%d", i))
		}
		inner := NewMapPrelim()
		m.Set(txn, "inner", inner)
		inner.Set(txn, "x", strings.Repeat("y", 50))
		inner.Set(txn, "x", strings.Repeat("z", 50))
		arr := NewArrayPrelim()
		m.Set(txn, "arr", arr)
		arr.Push(txn, []any{1, "two", 3.5, strings.Repeat("w", 300)})
	})
	doc.Transact(func(txn *Transaction) { m.Delete(txn, "k3") })
	frag := doc.GetXmlFragment("x")
	doc.Transact(func(txn *Transaction) {
		for i := range 20 {
			p := NewYXmlElement("p")
			frag.InsertElement(txn, i, p)
			p.SetAttribute(txn, "class", "c")
			p.SetAttribute(txn, "class", fmt.Sprintf("c%d", i))
			p.SetAttributeValue(txn, "level", i)
			text := NewYXmlText()
			p.InsertText(txn, 0, text)
			text.Insert(txn, 0, strings.Repeat("t", 30), nil)
		}
	})
	keyed := encoding.EncodeBytes(func(enc *encoding.Encoder) {
		enc.WriteVarUint(1)  // one struct group
		enc.WriteVarUint(1)  // of one struct
		enc.WriteVarUint(23) // client
		enc.WriteVarUint(0)  // clock
		enc.WriteUint8(wireAny | flagHasParentSub)
		enc.WriteUint8(1) // a root type, by name
		enc.WriteVarString("keyed")
		enc.WriteVarString("k")
		enc.WriteVarUint(3)
		for _, v := range []string{"p", "q", "r"} {
			enc.WriteAny(strings.Repeat(v, 300))
		}
		enc.WriteVarUint(0) // no deletions
	})
	require.NoError(t, ApplyUpdateV1(doc, keyed, nil))

	var withOrigin, cutKeyed bool
	for _, items := range doc.store.clients {
		for _, it := range items {
			withOrigin = withOrigin || it.ParentSub != nil && it.Origin != nil
			cutKeyed = cutKeyed || it.ParentSub != nil && it.Origin == nil && it.Content.Len() > 1
		}
	}
	require.True(t, withOrigin, "the room holds a keyed struct with an origin")
	require.True(t, cutKeyed, "the room holds a keyed struct an update can cut")
	return doc
}

// rereadRoom holds values written through the Go API that the decode reads
// back as other types, which are written in other bytes: an integer past
// int32 that a float32 holds, a JSON number past float64's integers, and a
// subdocument, whose options the decode rebuilds.
func rereadRoom(t *testing.T) *Doc {
	doc := New(WithClientID(13))
	m := doc.GetMap("v")
	txt := doc.GetText("t")
	doc.Transact(func(txn *Transaction) {
		m.Set(txn, "int40", int64(1)<<40)
		m.Set(txn, "uint", uint64(1)<<33)
		m.Set(txn, "int", 7)
		m.Set(txn, "nested", map[string]any{"a": []any{int64(1) << 41, "s", nil, true}, "b": float32(1.5)})
		m.Set(txn, "bytes", []byte{1, 2, 3})
		m.Set(txn, "bigint", encoding.BigInt(1<<60))
		m.Set(txn, "sub", New(WithGUID("subdocument"), WithClientID(14)))
		txt.Insert(txn, 0, "formatted text", nil)
		txt.Format(txn, 0, 9, Attributes{"bold": true, "size": int64(1)<<60 + 1})
		txt.InsertEmbed(txn, 4, map[string]any{"src": "x.png", "width": 640}, nil)
	})
	reread := 0
	for _, items := range doc.store.clients {
		for _, it := range items {
			if !rereadsSame(it.Content) {
				reread++
			}
		}
	}
	require.GreaterOrEqual(t, reread, 4, "structs the decode reads back as other values")
	return doc
}

// parkedRoom holds structs that name a client it does not hold, and
// deletions of that client's clocks, both parked.
func parkedRoom(t *testing.T) *Doc {
	a, b, c := New(WithClientID(31)), New(WithClientID(32)), New(WithClientID(33))
	aText, bText, cText := a.GetText("t"), b.GetText("t"), c.GetText("t")
	a.Transact(func(txn *Transaction) { aText.Insert(txn, 0, strings.Repeat("a", 400), nil) })
	require.NoError(t, ApplyUpdateV1(b, EncodeStateAsUpdateV1(a, nil), nil))
	b.Transact(func(txn *Transaction) { bText.Insert(txn, 200, strings.Repeat("b", 300), nil) })
	require.NoError(t, ApplyUpdateV1(c, EncodeStateAsUpdateV1(b, nil), nil))
	c.Transact(func(txn *Transaction) {
		cText.Insert(txn, 350, strings.Repeat("c", 300), nil)
		cText.Delete(txn, 250, 20)
	})

	doc := New(WithClientID(34))
	require.NoError(t, ApplyUpdateV1(doc, EncodeStateAsUpdateV1(a, nil), nil))
	text := doc.GetText("t")
	doc.Transact(func(txn *Transaction) { text.Insert(txn, 0, strings.Repeat("d", 100), nil) })
	require.NoError(t, ApplyUpdateV1(doc, EncodeStateAsUpdateV1(c, b.StateVector()), nil))
	stats := doc.PendingStats()
	require.NotZero(t, stats.Items, "the room parks structs")
	require.NotZero(t, stats.DeleteRanges, "the room parks deletions")
	return doc
}

// collectedRoom holds deleted text and a deleted nested map, whose content
// garbage collection has dropped.
func collectedRoom(t *testing.T) *Doc {
	doc := New(WithClientID(41))
	m := doc.GetMap("m")
	txt := doc.GetText("t")
	doc.Transact(func(txn *Transaction) {
		inner := NewMapPrelim()
		m.Set(txn, "inner", inner)
		for i := range 30 {
			inner.Set(txn, fmt.Sprintf("k%d", i), strings.Repeat("v", 40))
		}
		txt.Insert(txn, 0, strings.Repeat("x", 3000), nil)
	})
	doc.Transact(func(txn *Transaction) {
		m.Delete(txn, "inner")
		txt.Delete(txn, 100, 2000)
	})
	collected := false
	for _, it := range doc.store.clients[41] {
		_, gone := it.Content.(*ContentDeleted)
		collected = collected || gone
	}
	require.True(t, collected, "the room holds collected content")
	return doc
}
