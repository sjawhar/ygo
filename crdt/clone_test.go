package crdt

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// ── The corpus ───────────────────────────────────────────────────────────────

// cloneCase is one document of the Clone corpus: the document, the options it
// was built with (which a clone and an encode-and-apply copy both take), and a
// name.
type cloneCase struct {
	name string
	doc  *Doc
	opts []DocOption
}

// cloneCorpus is every document TestClone_Equivalence clones: each update the
// repo's fixture corpora hold, applied to a fresh document, and the generated
// documents below.
func cloneCorpus(t *testing.T) []cloneCase {
	t.Helper()
	cases := append(fixtureCloneCases(t), generatedCloneCases(t)...)
	require.NotEmpty(t, cases)
	return cases
}

// fixtureCloneCases applies every update the repo's fixture corpora hold: the
// JSON conformance fixtures in crdt/testdata, the binary fixtures in
// testutil/fixtures and testutil/go_fixtures, and the fuzz corpora in
// crdt/testdata/fuzz. A seed that does not apply cleanly is kept: the document
// it leaves - with the structs the apply parked, or those it integrated before
// failing - is a document a clone must copy too.
func fixtureCloneCases(t *testing.T) []cloneCase {
	t.Helper()
	var cases []cloneCase
	add := func(name string, update []byte, v2 bool) {
		doc := New(WithClientID(7))
		if v2 {
			_ = ApplyUpdateV2(doc, update, nil)
		} else {
			_ = ApplyUpdateV1(doc, update, nil)
		}
		cases = append(cases, cloneCase{name: name, doc: doc, opts: []DocOption{WithClientID(7)}})
	}

	jsonFiles, err := filepath.Glob(filepath.Join("testdata", "*.json"))
	require.NoError(t, err)
	require.NotEmpty(t, jsonFiles)
	for _, file := range jsonFiles {
		raw, err := os.ReadFile(file)
		require.NoError(t, err)
		var tree any
		require.NoError(t, json.Unmarshal(raw, &tree), file)
		base := strings.TrimSuffix(filepath.Base(file), ".json")
		for _, u := range collectHexUpdates(tree, base) {
			add(u.name, u.update, false)
		}
	}

	for _, dir := range []string{
		filepath.Join("..", "testutil", "fixtures"),
		filepath.Join("..", "testutil", "go_fixtures"),
	} {
		bins, err := filepath.Glob(filepath.Join(dir, "*.bin"))
		require.NoError(t, err)
		for _, file := range bins {
			name := strings.TrimSuffix(filepath.Base(file), ".bin")
			raw, err := os.ReadFile(file)
			require.NoError(t, err)
			add(filepath.Base(dir)+"/"+name, raw, strings.HasSuffix(name, "_v2"))
		}
	}

	for _, target := range []struct {
		dir string
		v2  bool
	}{
		{filepath.Join("testdata", "fuzz", "FuzzApplyUpdateV1"), false},
		{filepath.Join("testdata", "fuzz", "FuzzApplyUpdateV2"), true},
	} {
		seeds, err := filepath.Glob(filepath.Join(target.dir, "*"))
		require.NoError(t, err)
		for _, file := range seeds {
			raw, err := os.ReadFile(file)
			require.NoError(t, err)
			for i, update := range goFuzzSeedBytes(string(raw)) {
				add(fmt.Sprintf("fuzz/%s/%s#%d", filepath.Base(target.dir), filepath.Base(file), i), update, target.v2)
			}
		}
	}
	return cases
}

// namedUpdate is one update found in a fixture file.
type namedUpdate struct {
	name   string
	update []byte
}

// collectHexUpdates walks a decoded fixture file and returns every hex-encoded
// update under a "v1" or "updateV1" key, whatever the file's shape: the
// corpora are lists of fixtures, maps of named lists, and lists holding
// per-peer encodings, and a new corpus is picked up without a change here.
func collectHexUpdates(node any, path string) []namedUpdate {
	var out []namedUpdate
	switch v := node.(type) {
	case map[string]any:
		name := path
		if s, ok := v["name"].(string); ok && s != "" {
			name = path + "/" + s
		}
		for _, key := range []string{"v1", "updateV1"} {
			s, ok := v[key].(string)
			if !ok || s == "" {
				continue
			}
			update, err := hex.DecodeString(s)
			if err != nil {
				continue
			}
			out = append(out, namedUpdate{name: name + "/" + key, update: update})
		}
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if k == "v1" || k == "updateV1" {
				continue
			}
			out = append(out, collectHexUpdates(v[k], name+"/"+k)...)
		}
	case []any:
		for i, child := range v {
			out = append(out, collectHexUpdates(child, fmt.Sprintf("%s[%d]", path, i))...)
		}
	}
	return out
}

// goFuzzSeedBytes returns the []byte values of a go test fuzz corpus file.
func goFuzzSeedBytes(file string) [][]byte {
	var out [][]byte
	for _, line := range strings.Split(file, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "[]byte(") || !strings.HasSuffix(line, ")") {
			continue
		}
		s, err := strconv.Unquote(strings.TrimSuffix(strings.TrimPrefix(line, "[]byte("), ")"))
		if err != nil {
			continue
		}
		out = append(out, []byte(s))
	}
	return out
}

// generatedCloneCases builds the documents the fixtures do not cover: every
// shared type, formatting and embeds, replaced map keys, deletes with gc on
// and off, a merge of two clients' concurrent edits, a subdocument, moves, a
// document holding parked structs and parked deletes, and seeded random
// documents built from all of the above.
func generatedCloneCases(t *testing.T) []cloneCase {
	t.Helper()
	var cases []cloneCase
	add := func(name string, opts []DocOption, build func(*Doc)) {
		doc := New(opts...)
		build(doc)
		cases = append(cases, cloneCase{name: "gen/" + name, doc: doc, opts: opts})
	}
	one := []DocOption{WithClientID(1)}
	noGC := []DocOption{WithClientID(1), WithGC(false)}

	add("empty", one, func(d *Doc) {})

	add("text_one_transaction", one, func(d *Doc) {
		txt := d.GetText("t")
		d.Transact(func(tx *Transaction) { txt.Insert(tx, 0, "Hello, 世界 — ✨", nil) })
	})

	// One transaction per keystroke: the source holds one struct per
	// character, which an apply squashes into one. The clone holds the
	// source's structs.
	add("text_keystrokes", one, func(d *Doc) {
		txt := d.GetText("t")
		for range 40 {
			d.Transact(func(tx *Transaction) { txt.Insert(tx, txt.Len(), "x", nil) })
		}
	})

	add("text_formatted", one, func(d *Doc) {
		txt := d.GetText("t")
		d.Transact(func(tx *Transaction) {
			txt.Insert(tx, 0, "the quick brown fox", nil)
			txt.Format(tx, 4, 5, Attributes{"bold": true})
			txt.Format(tx, 10, 5, Attributes{"italic": true, "color": "#f00"})
			txt.InsertEmbed(tx, 3, map[string]any{"image": "a.png"}, Attributes{"alt": "a"})
			txt.Insert(tx, 0, "lead", Attributes{"bold": true})
		})
		d.Transact(func(tx *Transaction) {
			txt.Format(tx, 6, 4, Attributes{"bold": nil})
			txt.Delete(tx, 2, 3)
		})
	})

	add("text_delta", one, func(d *Doc) {
		txt := d.GetText("t")
		d.Transact(func(tx *Transaction) {
			txt.ApplyDelta(tx, []Delta{
				{Insert: "hello ", Attributes: Attributes{"bold": true}},
				{Insert: map[string]any{"embed": 3.5}},
				{Insert: "world"},
			})
		})
		d.Transact(func(tx *Transaction) {
			txt.ApplyDelta(tx, []Delta{
				{Retain: 2},
				{Delete: 3},
				{Retain: 2, Attributes: Attributes{"italic": true}},
			})
		})
	})

	add("map_replaced_keys", one, func(d *Doc) {
		m := d.GetMap("m")
		for round := range 4 {
			d.Transact(func(tx *Transaction) {
				m.Set(tx, "a", round)
				m.Set(tx, "b", fmt.Sprintf("v%d", round))
				m.Set(tx, "", round) // the empty-string key
			})
		}
		d.Transact(func(tx *Transaction) {
			m.Set(tx, "nested", map[string]any{"x": []any{1, 2, 3}})
			m.Delete(tx, "b")
		})
	})

	add("map_nested_types", one, func(d *Doc) {
		m := d.GetMap("m")
		d.Transact(func(tx *Transaction) {
			inner := NewMapPrelim()
			m.Set(tx, "inner", inner)
			inner.Set(tx, "k", "v")
			arr := NewArrayPrelim()
			m.Set(tx, "arr", arr)
			arr.Push(tx, []any{1, "two", true})
			txt := NewTextPrelim()
			m.Set(tx, "txt", txt)
			txt.Insert(tx, 0, "nested", Attributes{"bold": true})
		})
	})

	add("array_mixed", one, func(d *Doc) {
		arr := d.GetArray("a")
		d.Transact(func(tx *Transaction) {
			arr.Push(tx, []any{1, 2.5, "three", true, nil, []any{1, 2}, map[string]any{"k": "v"}})
		})
		d.Transact(func(tx *Transaction) {
			arr.Insert(tx, 2, []any{"inserted"})
			arr.Delete(tx, 0, 2)
		})
		d.Transact(func(tx *Transaction) {
			inner := NewArrayPrelim()
			arr.PushType(tx, inner)
			inner.Push(tx, []any{"deep"})
		})
	})

	add("array_moves", one, func(d *Doc) {
		arr := d.GetArray("a")
		d.Transact(func(tx *Transaction) { arr.Push(tx, []any{"a", "b", "c", "d", "e"}) })
		d.Transact(func(tx *Transaction) { arr.Move(tx, 0, 4) })
		d.Transact(func(tx *Transaction) { arr.Move(tx, 3, 1) })
		d.Transact(func(tx *Transaction) { arr.Delete(tx, 2, 1) })
	})

	add("xml", one, func(d *Doc) {
		frag := d.GetXmlFragment("x")
		d.Transact(func(tx *Transaction) {
			for i := range 5 {
				p := NewYXmlElement("paragraph")
				frag.InsertElement(tx, i, p)
				p.SetAttribute(tx, "id", fmt.Sprintf("p%d", i))
				p.SetAttributeValue(tx, "indent", i)
				txt := NewYXmlText()
				p.InsertText(tx, 0, txt)
				txt.Insert(tx, 0, fmt.Sprintf("paragraph %d text", i), Attributes{"bold": i%2 == 0})
			}
		})
		d.Transact(func(tx *Transaction) { frag.Delete(tx, 1, 1) })
	})

	add("deletes_gc_on", one, buildDeletes)
	add("deletes_gc_off", noGC, buildDeletes)

	add("subdoc", one, func(d *Doc) {
		m := d.GetMap("m")
		kept := New(WithGUID("kept"), WithAutoLoad(true), WithCollectionID("c1"))
		gone := New(WithGUID("gone"))
		d.Transact(func(tx *Transaction) {
			m.Set(tx, "kept", kept)
			m.Set(tx, "gone", gone)
		})
		kept.Transact(func(tx *Transaction) { tx.GetText("inner").Insert(tx, 0, "inner state", nil) })
		d.Transact(func(tx *Transaction) { m.Delete(tx, "gone") })
	})

	cases = append(cases, mergedCloneCases(t)...)
	cases = append(cases, parkedCloneCases(t)...)
	cases = append(cases, randomCloneCases(t, 24)...)
	return cases
}

// buildDeletes writes and deletes across the types whose deletes differ: a
// text run split by a delete, a map key, an array range, and a nested type.
func buildDeletes(d *Doc) {
	txt, arr, m := d.GetText("t"), d.GetArray("a"), d.GetMap("m")
	d.Transact(func(tx *Transaction) {
		txt.Insert(tx, 0, "abcdefghijklmnop", Attributes{"bold": true})
		arr.Push(tx, []any{1, 2, 3, 4, 5})
		m.Set(tx, "k", "v")
		inner := NewMapPrelim()
		m.Set(tx, "inner", inner)
		inner.Set(tx, "deep", 1)
	})
	d.Transact(func(tx *Transaction) {
		txt.Delete(tx, 3, 5)
		arr.Delete(tx, 1, 2)
		m.Delete(tx, "inner")
	})
}

// mergedCloneCases returns the documents two clients editing concurrently and
// syncing leave: both peers' own copies and a third that applied both.
func mergedCloneCases(t *testing.T) []cloneCase {
	t.Helper()
	optsA := []DocOption{WithClientID(11)}
	optsB := []DocOption{WithClientID(22)}
	a := New(optsA...)
	ta := a.GetText("t")
	a.Transact(func(tx *Transaction) { ta.Insert(tx, 0, "shared base", nil) })

	// Both peers start from the same base, so the edits below are concurrent.
	b := New(optsB...)
	require.NoError(t, ApplyUpdateV1(b, EncodeStateAsUpdateV1(a, nil), nil))
	tb := b.GetText("t")

	a.Transact(func(tx *Transaction) {
		ta.Insert(tx, 6, "AAA", Attributes{"bold": true})
		ta.Delete(tx, 0, 2)
	})
	b.Transact(func(tx *Transaction) {
		tb.Insert(tx, 6, "BBB", Attributes{"italic": true})
		tb.Format(tx, 0, 5, Attributes{"underline": true})
	})
	ua, ub := EncodeStateAsUpdateV1(a, nil), EncodeStateAsUpdateV1(b, nil)
	require.NoError(t, ApplyUpdateV1(a, ub, nil))
	require.NoError(t, ApplyUpdateV1(b, ua, nil))
	both := New(WithClientID(33))
	require.NoError(t, ApplyUpdateV1(both, ua, nil))
	require.NoError(t, ApplyUpdateV1(both, ub, nil))

	return []cloneCase{
		{name: "gen/merged_peer_a", doc: a, opts: optsA},
		{name: "gen/merged_peer_b", doc: b, opts: optsB},
		{name: "gen/merged_third", doc: both, opts: []DocOption{WithClientID(33)}},
	}
}

// parkedCloneCases returns documents holding parked structs and parked delete
// ranges: a peer that got the tail of another's log without its head.
func parkedCloneCases(t *testing.T) []cloneCase {
	t.Helper()
	src := New(WithClientID(5))
	txt, m := src.GetText("t"), src.GetMap("m")
	src.Transact(func(tx *Transaction) {
		txt.Insert(tx, 0, "head of the log", nil)
		m.Set(tx, "k", "v")
	})
	head := src.StateVector()
	src.Transact(func(tx *Transaction) {
		txt.Insert(tx, 15, " and its tail", nil)
		txt.Delete(tx, 0, 4) // deletes structs of the head the peer will not have
		m.Set(tx, "k", "w")
	})
	tail := EncodeStateAsUpdateV1(src, head)

	opts := []DocOption{WithClientID(6)}
	parked := New(opts...)
	_ = ApplyUpdateV1(parked, tail, nil)
	stats := parked.PendingStats()
	require.Positive(t, stats.Items, "the fixture must park structs")
	require.Positive(t, stats.DeleteRanges, "the fixture must park delete ranges")

	// The same peer once more, with the same update parked twice.
	parked2 := New(opts...)
	_ = ApplyUpdateV1(parked2, tail, nil)
	_ = ApplyUpdateV1(parked2, tail, nil)

	return []cloneCase{
		{name: "gen/parked", doc: parked, opts: opts},
		{name: "gen/parked_twice", doc: parked2, opts: opts},
	}
}

// randomCloneCases returns n seeded random documents: two clients running a
// random mix of text, map, array and XML operations, syncing at random points,
// so the corpus holds shapes nobody wrote by hand.
func randomCloneCases(t *testing.T, n int) []cloneCase {
	t.Helper()
	cases := make([]cloneCase, 0, n)
	for seed := range n {
		rng := rand.New(rand.NewPCG(uint64(seed), 0x5eed))
		opts := []DocOption{WithClientID(ClientID(100 + seed))}
		if seed%4 == 3 {
			opts = append(opts, WithGC(false))
		}
		a := New(opts...)
		b := New(WithClientID(ClientID(900 + seed)))
		for range 12 {
			randomOps(a, rng)
			randomOps(b, rng)
			if rng.IntN(3) == 0 {
				require.NoError(t, ApplyUpdateV1(a, EncodeStateAsUpdateV1(b, a.StateVector()), nil))
			}
			if rng.IntN(3) == 0 {
				require.NoError(t, ApplyUpdateV1(b, EncodeStateAsUpdateV1(a, b.StateVector()), nil))
			}
		}
		cases = append(cases, cloneCase{name: fmt.Sprintf("gen/random_%d", seed), doc: a, opts: opts})
	}
	return cases
}

// randomOps runs one transaction of random operations on d.
func randomOps(d *Doc, rng *rand.Rand) {
	txt, arr, m, frag := d.GetText("t"), d.GetArray("a"), d.GetMap("m"), d.GetXmlFragment("x")
	d.Transact(func(tx *Transaction) {
		for range 1 + rng.IntN(4) {
			switch rng.IntN(9) {
			case 0:
				txt.Insert(tx, rng.IntN(txt.Len()+1), "lorem ipsum"[:1+rng.IntN(10)], nil)
			case 1:
				txt.Insert(tx, rng.IntN(txt.Len()+1), "bold", Attributes{"bold": true})
			case 2:
				if txt.Len() > 2 {
					at := rng.IntN(txt.Len() - 1)
					txt.Format(tx, at, 1+rng.IntN(txt.Len()-at-1), Attributes{"italic": rng.IntN(2) == 0})
				}
			case 3:
				if txt.Len() > 1 {
					at := rng.IntN(txt.Len() - 1)
					txt.Delete(tx, at, 1+rng.IntN(txt.Len()-at-1))
				}
			case 4:
				txt.InsertEmbed(tx, rng.IntN(txt.Len()+1), map[string]any{"e": rng.IntN(10)}, nil)
			case 5:
				m.Set(tx, fmt.Sprintf("k%d", rng.IntN(5)), rng.IntN(100))
			case 6:
				arr.Push(tx, []any{rng.IntN(100), "s"})
				if arr.Len() > 3 {
					arr.Delete(tx, rng.IntN(arr.Len()-1), 1)
				}
			case 7:
				e := NewYXmlElement("p")
				frag.InsertElement(tx, rng.IntN(frag.Len()+1), e)
				e.SetAttribute(tx, "k", fmt.Sprintf("%d", rng.IntN(10)))
			case 8:
				if frag.Len() > 0 {
					frag.Delete(tx, rng.IntN(frag.Len()), 1)
				}
			}
		}
	})
}

// ── Equivalence ──────────────────────────────────────────────────────────────

// TestClone_Equivalence clones every document of the corpus and checks the
// clone against the encode-and-apply copy of the same document: the encodings
// are byte-identical, the state vectors and the delete sets are equal, and
// both documents read the same - the same live content, in the same order,
// under the same keys, which the encoding alone does not say, since it is
// written from the store and not from the types' lists.
//
// It also checks the clone against its source: the clone shares no object
// with it, the source is left as it was, and - for every document whose
// applied form holds the source's structs, which is every document that was
// not typed a transaction at a time - the clone is isomorphic to the source
// down to every link. The count of each is reported.
func TestClone_Equivalence(t *testing.T) {
	cases := cloneCorpus(t)
	var identical, isomorphic, applyFailed int
	var differ []string
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srcEnc := EncodeStateAsUpdateV1(tc.doc, nil)
			srcSV := tc.doc.StateVector()
			srcDS := deleteSetKey(tc.doc)

			clone := tc.doc.Clone(tc.opts...)
			cloneEnc := EncodeStateAsUpdateV1(clone, nil)
			requireCloneDisjoint(t, tc.doc, clone)
			if cloneKeepsSourceStructs(tc.doc, clone) {
				isomorphic++
				requireCloneIsomorphic(t, tc.doc, clone)
			}

			// The source is untouched by the clone.
			require.Equal(t, srcEnc, EncodeStateAsUpdateV1(tc.doc, nil))
			require.Equal(t, srcSV, tc.doc.StateVector())
			require.Equal(t, srcDS, deleteSetKey(tc.doc))

			decodeCopy := New(tc.opts...)
			if err := ApplyUpdateV1(decodeCopy, srcEnc, nil); err != nil {
				// A fuzz seed can leave a document whose own encoding the
				// decoder refuses; there is no copy to compare against then.
				applyFailed++
				return
			}
			copyEnc := EncodeStateAsUpdateV1(decodeCopy, nil)
			if bytes.Equal(cloneEnc, copyEnc) {
				identical++
			} else {
				differ = append(differ, tc.name)
			}
			require.Equal(t, copyEnc, cloneEnc, "clone encodes as the encode-and-apply copy")
			require.Equal(t, decodeCopy.StateVector(), clone.StateVector(),
				"clone's state vector is the encode-and-apply copy's")
			require.Equal(t, deleteSetKey(decodeCopy), deleteSetKey(clone),
				"clone's delete set is the encode-and-apply copy's")
			require.Equal(t, liveDigest(decodeCopy), liveDigest(clone),
				"clone reads as the encode-and-apply copy")
		})
	}
	t.Logf("cloned %d documents: %d encode byte-identically to the encode-and-apply copy, "+
		"%d differ (%v), %d have an encoding the decoder refuses; "+
		"%d hold their source's structs and were checked isomorphic to it",
		len(cases), identical, len(differ), differ, applyFailed, isomorphic)
}

// cloneKeepsSourceStructs reports whether the applied form a clone holds is
// the source's own structs: the squash merged no run of the source's, and no
// struct lost a parent an update cannot name. Where it does not hold, the
// clone is the encode-and-apply copy of the source rather than a struct-for-
// struct image of it, and the two are compared by their encodings and their
// reads instead.
func cloneKeepsSourceStructs(src, clone *Doc) bool {
	if len(src.store.clients) != len(clone.store.clients) {
		return false
	}
	for client, items := range src.store.clients {
		copies := clone.store.clients[client]
		if len(copies) != len(items) {
			return false
		}
		for i, it := range items {
			if (it.Parent == nil) != (copies[i].Parent == nil) {
				return false
			}
		}
	}
	return true
}

// liveDigest renders what a document reads: every named root that holds
// anything, and under it the live content of each type - its sequence
// elements in list order and its keyed entries by key. Two documents that
// read the same have the same digest, whatever order their lists hold entries
// of different keys in, which integration order decides and nothing reads.
func liveDigest(d *Doc) string {
	var sb strings.Builder
	names := make([]string, 0, len(d.share))
	for name := range d.share {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		t := d.share[name].baseType()
		if t.start == nil && len(t.itemMap) == 0 {
			continue // an empty root, which no update names
		}
		fmt.Fprintf(&sb, "root %q ", name)
		digestType(&sb, t, 0)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// digestType renders one type's live content.
func digestType(sb *strings.Builder, t *abstractType, depth int) {
	if depth > 32 {
		sb.WriteString("{…}")
		return
	}
	fmt.Fprintf(sb, "{len=%d seq=[", t.length)
	for it := t.start; it != nil; it = it.Right {
		if it.Deleted || it.ParentSub != nil {
			continue
		}
		digestItem(sb, it, depth)
		sb.WriteByte(' ')
	}
	sb.WriteString("] keys=[")
	keys := make([]string, 0, len(t.itemMap))
	for key := range t.itemMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		it := t.itemMap[key]
		if it == nil || it.Deleted {
			continue
		}
		fmt.Fprintf(sb, "%q=", key)
		digestItem(sb, it, depth)
		sb.WriteByte(' ')
	}
	sb.WriteString("]}")
}

// digestItem renders one live struct's content.
func digestItem(sb *strings.Builder, it *Item, depth int) {
	switch content := it.Content.(type) {
	case *ContentString:
		fmt.Fprintf(sb, "str(%q)", content.Str)
	case *ContentAny:
		fmt.Fprintf(sb, "any(%v)", content.Vals)
	case *ContentJSON:
		fmt.Fprintf(sb, "json(%v)", content.Vals)
	case *ContentEmbed:
		fmt.Fprintf(sb, "embed(%v)", content.Val)
	case *ContentFormat:
		fmt.Fprintf(sb, "format(%s=%v)", content.Key, content.Val)
	case *ContentBinary:
		fmt.Fprintf(sb, "binary(%x)", content.Data)
	case *ContentMove:
		fmt.Fprintf(sb, "move(%s,%d)", idValue(content.Target), content.TargetLen)
	case *ContentDoc:
		guid := ""
		if content.Doc != nil {
			guid = content.Doc.guid
		}
		fmt.Fprintf(sb, "doc(%s)", guid)
	case *ContentType:
		if content.Type == nil {
			sb.WriteString("type(<nil>)")
			return
		}
		fmt.Fprintf(sb, "type(%T)", content.Type.owner)
		digestType(sb, content.Type, depth+1)
	default:
		fmt.Fprintf(sb, "%T", it.Content)
	}
}

// requireCloneIsomorphic checks that clone holds src's structure - every
// struct, every link between structs, every type and its bookkeeping - in
// objects of its own. It is called for the documents whose applied form is
// the source's own structs (cloneKeepsSourceStructs).
func requireCloneIsomorphic(t *testing.T, src, clone *Doc) {
	t.Helper()

	// types pairs each type of the source with the clone's counterpart, found
	// where the two documents are walked in step: a named root, the parent of
	// a struct, and the type a nested-type content holds. A type is reached
	// by more than one of these, and a source type that resolved to two
	// different types in the clone would be a copy that split it in two.
	types := make(map[*abstractType]*abstractType)
	pair := func(s, c *abstractType) {
		if s == nil || c == nil {
			return
		}
		if prev, ok := types[s]; ok {
			require.Same(t, prev, c, "one source type copied as two")
			return
		}
		types[s] = c
	}

	// The named roots: same names, same classes.
	require.Len(t, clone.share, len(src.share), "root types")
	for name, st := range src.share {
		other, ok := clone.share[name]
		require.True(t, ok, "root %q missing from the clone", name)
		require.Equal(t, fmt.Sprintf("%T", st), fmt.Sprintf("%T", other), "root %q class", name)
		pair(st.baseType(), other.baseType())
	}

	// Every struct, in clock order, and the types its parent and its content
	// name.
	require.Len(t, clone.store.clients, len(src.store.clients), "clients")
	cloneStructs := make(map[*Item]bool)
	for client, items := range src.store.clients {
		copies, ok := clone.store.clients[client]
		require.True(t, ok, "client %d missing from the clone", client)
		require.Len(t, copies, len(items), "client %d struct count", client)
		for i, it := range items {
			requireStructCopy(t, it, copies[i], fmt.Sprintf("client %d struct %d", client, i))
			cloneStructs[copies[i]] = true
			pairTypesOf(pair, it, copies[i])
		}
	}

	// Every link of the clone points at a struct of the clone.
	for _, items := range clone.store.clients {
		for _, it := range items {
			for _, link := range []*Item{it.Left, it.Right, it.MovedBy} {
				if link != nil {
					require.True(t, cloneStructs[link], "a clone's link leaves the clone")
				}
			}
		}
	}

	// The parked structs, which hold types of their own until they integrate.
	if src.store.pending == nil {
		require.Nil(t, clone.store.pending)
	} else {
		require.NotNil(t, clone.store.pending)
		require.Len(t, clone.store.pending.items, len(src.store.pending.items))
		for i, it := range src.store.pending.items {
			cp := clone.store.pending.items[i]
			if it == nil {
				require.Nil(t, cp)
				continue
			}
			requireStructCopy(t, it, cp, fmt.Sprintf("parked struct %d", i))
			pairTypesOf(pair, it, cp)
		}
		require.Equal(t, src.store.pending.missing, clone.store.pending.missing)
	}

	// Every type the walk paired: its own object, on the clone, holding the
	// source type's list, key map and counters.
	for st, cp := range types {
		require.NotSame(t, st, cp, "the clone shares the source's type")
		require.Same(t, clone, cp.doc, "a clone's type belongs to the clone")
		requireTypeCopy(t, st, cp, typeKey(st))
	}

	// The parked delete ranges and the deferred moves.
	require.Equal(t, src.store.pendingDsRanges, clone.store.pendingDsRanges)
	require.Equal(t, src.store.pendingDs.clients, clone.store.pendingDs.clients)
	require.Equal(t, src.store.pendingDsMoved, clone.store.pendingDsMoved)
	require.Len(t, clone.store.pendingMoves, len(src.store.pendingMoves))
	for client, moves := range src.store.pendingMoves {
		require.Len(t, clone.store.pendingMoves[client], len(moves))
		for i, move := range moves {
			require.Equal(t, move.clock, clone.store.pendingMoves[client][i].clock)
			requireStructCopy(t, move.move, clone.store.pendingMoves[client][i].move, "deferred move")
		}
	}
}

// requireCloneDisjoint checks that a clone shares no object its documents
// write through with its source: no struct, no type, no content and no
// subdocument. Strings, ids and the values inside a plain-value content are
// shared on purpose, and are not objects ygo writes through (see Clone).
func requireCloneDisjoint(t *testing.T, src, clone *Doc) {
	t.Helper()
	srcItems, srcTypes, srcContents, srcDocs := docObjects(src)
	cloneItems, cloneTypes, cloneContents, cloneDocs := docObjects(clone)
	for it := range cloneItems {
		require.False(t, srcItems[it], "the clone holds a struct of the source")
	}
	for ty := range cloneTypes {
		require.False(t, srcTypes[ty], "the clone holds a type of the source")
		require.Same(t, clone, ty.doc, "a clone's type belongs to the clone")
	}
	for c := range cloneContents {
		require.False(t, srcContents[c], "the clone holds a content of the source")
	}
	for d := range cloneDocs {
		require.False(t, srcDocs[d], "the clone holds a subdocument of the source")
	}
}

// docObjects returns every struct, type, content and subdocument a document
// holds, integrated or parked.
func docObjects(d *Doc) (map[*Item]bool, map[*abstractType]bool, map[Content]bool, map[*Doc]bool) {
	items := make(map[*Item]bool)
	types := make(map[*abstractType]bool)
	contents := make(map[Content]bool)
	docs := make(map[*Doc]bool)
	for _, st := range d.share {
		types[st.baseType()] = true
	}
	visit := func(list []*Item) {
		for _, it := range list {
			if it == nil {
				continue
			}
			items[it] = true
			contents[it.Content] = true
			if it.Parent != nil {
				types[it.Parent] = true
			}
			switch content := it.Content.(type) {
			case *ContentType:
				if content.Type != nil {
					types[content.Type] = true
				}
			case *ContentDoc:
				if content.Doc != nil {
					docs[content.Doc] = true
				}
			}
		}
	}
	for _, list := range d.store.clients {
		visit(list)
	}
	if d.store.pending != nil {
		visit(d.store.pending.items)
	}
	return items, types, contents, docs
}

// pairTypesOf pairs the types a struct and its copy name: the type they are
// parented to, and the nested type their content holds.
func pairTypesOf(pair func(s, c *abstractType), it, cp *Item) {
	pair(it.Parent, cp.Parent)
	if ct, ok := it.Content.(*ContentType); ok {
		if cct, ok := cp.Content.(*ContentType); ok {
			pair(ct.Type, cct.Type)
		}
	}
}

// requireStructCopy checks that cp is it's copy: the same struct in its own
// object, with every link pointing at the copy of what it pointed at.
func requireStructCopy(t *testing.T, it, cp *Item, where string) {
	t.Helper()
	require.NotSame(t, it, cp, "%s: the clone shares the source's struct", where)
	require.Equal(t, it.ID, cp.ID, "%s: id", where)
	require.Equal(t, it.Deleted, cp.Deleted, "%s: deleted", where)
	require.Equal(t, idValue(it.Origin), idValue(cp.Origin), "%s: origin", where)
	require.Equal(t, idValue(it.OriginRight), idValue(cp.OriginRight), "%s: right origin", where)
	require.Equal(t, idValue(it.parentID), idValue(cp.parentID), "%s: parent id", where)
	require.Equal(t, idValue(it.redone), idValue(cp.redone), "%s: redone", where)
	require.Equal(t, parentSubValue(it.ParentSub), parentSubValue(cp.ParentSub), "%s: parent key", where)
	require.Equal(t, itemKey(it.Left), itemKey(cp.Left), "%s: left", where)
	require.Equal(t, itemKey(it.Right), itemKey(cp.Right), "%s: right", where)
	require.Equal(t, itemKey(it.MovedBy), itemKey(cp.MovedBy), "%s: moved by", where)
	require.Equal(t, typeKey(it.Parent), typeKey(cp.Parent), "%s: parent", where)
	requireContentCopy(t, it.Content, cp.Content, where)
}

// requireContentCopy checks that cp holds what c holds, in its own object.
func requireContentCopy(t *testing.T, c, cp Content, where string) {
	t.Helper()
	require.Equal(t, fmt.Sprintf("%T", c), fmt.Sprintf("%T", cp), "%s: content class", where)
	require.NotSame(t, c, cp, "%s: the clone shares the source's content", where)
	require.Equal(t, c.Len(), cp.Len(), "%s: content length", where)
	switch v := c.(type) {
	case *ContentString:
		require.Equal(t, v.Str, cp.(*ContentString).Str, "%s: string", where)
	case *ContentBinary:
		other := cp.(*ContentBinary)
		require.Equal(t, v.Data, other.Data, "%s: bytes", where)
		if len(v.Data) > 0 {
			require.NotSame(t, &v.Data[0], &other.Data[0], "%s: shared byte array", where)
		}
	case *ContentAny:
		other := cp.(*ContentAny)
		require.Equal(t, v.Vals, other.Vals, "%s: values", where)
		if len(v.Vals) > 0 {
			require.NotSame(t, &v.Vals[0], &other.Vals[0], "%s: shared value slice", where)
		}
	case *ContentJSON:
		other := cp.(*ContentJSON)
		require.Equal(t, v.Vals, other.Vals, "%s: values", where)
		if len(v.Vals) > 0 {
			require.NotSame(t, &v.Vals[0], &other.Vals[0], "%s: shared value slice", where)
		}
	case *ContentEmbed:
		require.Equal(t, v.Val, cp.(*ContentEmbed).Val, "%s: embed", where)
	case *ContentFormat:
		other := cp.(*ContentFormat)
		require.Equal(t, v.Key, other.Key, "%s: format key", where)
		require.Equal(t, v.Val, other.Val, "%s: format value", where)
	case *ContentMove:
		other := cp.(*ContentMove)
		require.Equal(t, idValue(v.Target), idValue(other.Target), "%s: move target", where)
		require.Equal(t, v.TargetLen, other.TargetLen, "%s: move target length", where)
	case *ContentType:
		other := cp.(*ContentType)
		require.Equal(t, typeKey(v.Type), typeKey(other.Type), "%s: nested type", where)
		require.NotSame(t, v.Type, other.Type, "%s: shared nested type", where)
		require.Equal(t, fmt.Sprintf("%T", v.Type.owner), fmt.Sprintf("%T", other.Type.owner),
			"%s: nested type class", where)
	case *ContentDoc:
		other := cp.(*ContentDoc)
		require.NotSame(t, v.Doc, other.Doc, "%s: shared subdocument", where)
		require.Equal(t, v.Doc.guid, other.Doc.guid, "%s: subdocument guid", where)
		require.Equal(t, v.Doc.gc, other.Doc.gc, "%s: subdocument gc", where)
		require.Equal(t, v.Doc.autoLoad, other.Doc.autoLoad, "%s: subdocument autoload", where)
		require.Equal(t, v.Doc.meta, other.Doc.meta, "%s: subdocument meta", where)
		wantCollection := ""
		if other.Doc.parent != nil {
			wantCollection = other.Doc.parent.CollectionID()
		}
		require.Equal(t, wantCollection, other.Doc.CollectionID(), "%s: subdocument inherits clone collection", where)
	}
}

// requireTypeCopy checks a type's own state: its list, its key map, its
// container item and its counters.
func requireTypeCopy(t *testing.T, st, cp *abstractType, where string) {
	t.Helper()
	require.NotSame(t, st, cp, "%s: the clone shares the source's type", where)
	require.Equal(t, st.name, cp.name, "%s: name", where)
	require.Equal(t, st.length, cp.length, "%s: length", where)
	require.Equal(t, itemKey(st.item), itemKey(cp.item), "%s: container item", where)
	require.Equal(t, listKeys(st), listKeys(cp), "%s: list", where)
	require.Equal(t, itemMapKeys(st), itemMapKeys(cp), "%s: key map", where)
	require.Equal(t, st.hasFormatting, cp.hasFormatting, "%s: has formatting", where)
	require.Equal(t, st.cleanFormatting, cp.cleanFormatting, "%s: clean formatting", where)
	if e, ok := st.owner.(*YXmlElement); ok {
		require.Equal(t, e.NodeName, cp.owner.(*YXmlElement).NodeName, "%s: node name", where)
	}
}

// listKeys renders a type's linked list as the ids on it, in order.
func listKeys(t *abstractType) []string {
	var out []string
	for item := t.start; item != nil; item = item.Right {
		out = append(out, itemKey(item))
	}
	return out
}

// itemMapKeys renders a type's key map as key=id pairs, sorted.
func itemMapKeys(t *abstractType) []string {
	out := make([]string, 0, len(t.itemMap))
	for key, item := range t.itemMap {
		out = append(out, key+"="+itemKey(item))
	}
	sort.Strings(out)
	return out
}

// itemKey identifies a struct by id, for comparing two documents' links.
func itemKey(it *Item) string {
	if it == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d:%d", it.ID.Client, it.ID.Clock)
}

// typeKey identifies a type by its root name or by its container item's id.
func typeKey(t *abstractType) string {
	switch {
	case t == nil:
		return "<nil>"
	case t.item != nil:
		return "under " + itemKey(t.item)
	default:
		return "root " + t.name
	}
}

// idValue renders an optional id for comparison.
func idValue(id *ID) string {
	if id == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d:%d", id.Client, id.Clock)
}

// parentSubValue renders an optional map key for comparison: a nil key (a
// sequence element) is not the empty-string key.
func parentSubValue(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return "=" + *s
}

// deleteSetKey renders DeleteSetFromDoc as a comparable string.
func deleteSetKey(d *Doc) string {
	ds := DeleteSetFromDoc(d)
	clients := ds.Clients()
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	var sb strings.Builder
	for _, c := range clients {
		fmt.Fprintf(&sb, "%d:", c)
		for _, r := range ds.Ranges(c) {
			fmt.Fprintf(&sb, "%d+%d,", r.Clock, r.Len)
		}
		sb.WriteByte(';')
	}
	return sb.String()
}

// TestClone_RootTypesAreTheClones checks that the named root types of a clone
// are the clone's own types, of the class the source holds, and that a root
// the source never opened by name is still openable on the clone.
func TestClone_RootTypesAreTheClones(t *testing.T) {
	src := New(WithClientID(1))
	frag, txt, m, arr := src.GetXmlFragment("x"), src.GetText("t"), src.GetMap("m"), src.GetArray("a")
	src.Transact(func(tx *Transaction) {
		e := NewYXmlElement("p")
		frag.InsertElement(tx, 0, e)
		e.SetAttribute(tx, "k", "v")
		txt.Insert(tx, 0, "text", nil)
		m.Set(tx, "k", "v")
		arr.Push(tx, []any{1, 2})
	})

	// A root type the source document itself never opened by name: it exists
	// only as the placeholder an apply creates.
	loaded := New(WithClientID(2))
	require.NoError(t, ApplyUpdateV1(loaded, EncodeStateAsUpdateV1(src, nil), nil))

	for _, doc := range []*Doc{src, loaded} {
		clone := doc.Clone(WithClientID(3))
		require.Equal(t, doc.GetXmlFragment("x").ToXML(), clone.GetXmlFragment("x").ToXML())
		require.Equal(t, doc.GetText("t").ToString(), clone.GetText("t").ToString())
		require.Equal(t, doc.GetMap("m").Entries(), clone.GetMap("m").Entries())
		require.Equal(t, doc.GetArray("a").ToSlice(), clone.GetArray("a").ToSlice())
		require.NotSame(t, doc.GetText("t"), clone.GetText("t"))
		require.NotSame(t, doc.GetXmlFragment("x"), clone.GetXmlFragment("x"))
	}
}

// TestClone_Subdocs checks the subdocument semantics Clone matches: the shells
// an apply builds, registered as an apply registers them, with no content.
func TestClone_Subdocs(t *testing.T) {
	src := New(WithClientID(1))
	m := src.GetMap("m")
	kept := New(WithGUID("kept"), WithAutoLoad(true), WithCollectionID("c1"))
	kept.meta = map[string]any{"kind": "test"}
	gone := New(WithGUID("gone"))
	src.Transact(func(tx *Transaction) {
		m.Set(tx, "kept", kept)
		m.Set(tx, "gone", gone)
	})
	kept.Transact(func(tx *Transaction) { tx.GetText("inner").Insert(tx, 0, "inner", nil) })
	src.Transact(func(tx *Transaction) { m.Delete(tx, "gone") })

	decodeCopy := New(WithClientID(2), WithCollectionID("destination"))
	require.NoError(t, ApplyUpdateV1(decodeCopy, EncodeStateAsUpdateV1(src, nil), nil))
	clone := src.Clone(WithClientID(2), WithCollectionID("destination"))

	require.Equal(t, decodeCopy.GetSubdocGUIDs(), clone.GetSubdocGUIDs())
	require.Equal(t, []string{"kept"}, clone.GetSubdocGUIDs())

	cloned, fromCopy := clone.GetSubdocs()[0], decodeCopy.GetSubdocs()[0]
	require.NotSame(t, kept, cloned)
	require.Equal(t, kept.GUID(), cloned.GUID())
	require.Equal(t, fromCopy.AutoLoad(), cloned.AutoLoad())
	require.Equal(t, fromCopy.ShouldLoad(), cloned.ShouldLoad())
	require.Equal(t, fromCopy.CollectionID(), cloned.CollectionID())
	require.Equal(t, "destination", cloned.CollectionID())
	require.Same(t, clone, cloned.parent)
	require.Equal(t, kept.meta, cloned.meta)
	// The subdocument's own content travels in no update of the parent, so
	// neither copy holds it.
	require.Equal(t, EncodeStateAsUpdateV1(fromCopy, nil), EncodeStateAsUpdateV1(cloned, nil))
	require.Empty(t, cloned.GetText("inner").ToString())
	require.Equal(t, "inner", kept.GetText("inner").ToString())
}

// TestClone_KeepsContentTheSourceStillHolds pins the one place a clone is not
// byte-identical to the encode-and-apply copy: a document with an UndoManager
// attached runs no auto-GC, so it still holds the content of a struct it
// deleted. A clone holds that content too, since copying it needs no
// UndoManager of its own; the copy loses it, because the apply that builds it
// collects the deletion at commit. With no UndoManager attached - every other
// document - the source has collected it already and the two agree.
func TestClone_KeepsContentTheSourceStillHolds(t *testing.T) {
	withUndo := New(WithClientID(1))
	txt := withUndo.GetText("t")
	withUndo.Transact(func(tx *Transaction) { txt.Insert(tx, 0, "abcdef", nil) })
	um := NewUndoManager(withUndo, []SharedType{txt})
	defer um.Destroy()
	withUndo.Transact(func(tx *Transaction) { txt.Delete(tx, 0, 3) })

	srcEnc := EncodeStateAsUpdateV1(withUndo, nil)
	require.Contains(t, string(srcEnc), "abc", "the source must still hold the deleted text")

	clone := withUndo.Clone(WithClientID(2))
	require.Equal(t, srcEnc, EncodeStateAsUpdateV1(clone, nil))

	copyDoc := New(WithClientID(2))
	require.NoError(t, ApplyUpdateV1(copyDoc, srcEnc, nil))
	require.NotContains(t, string(EncodeStateAsUpdateV1(copyDoc, nil)), "abc",
		"the encode-and-apply copy drops the content of the deletion it applies")

	// Both copies still read the same text, and both encode the same deletes.
	require.Equal(t, "def", clone.GetText("t").ToString())
	require.Equal(t, "def", copyDoc.GetText("t").ToString())
	require.Equal(t, deleteSetKey(copyDoc), deleteSetKey(clone))

	// The same document with no UndoManager attached: the deletion is
	// collected as it is made, and clone and copy are byte-identical.
	plain := New(WithClientID(1))
	plainText := plain.GetText("t")
	plain.Transact(func(tx *Transaction) { plainText.Insert(tx, 0, "abcdef", nil) })
	plain.Transact(func(tx *Transaction) { plainText.Delete(tx, 0, 3) })
	plainEnc := EncodeStateAsUpdateV1(plain, nil)
	require.NotContains(t, string(plainEnc), "abc", "a gc document collects the deletion itself")

	plainCopy := New(WithClientID(2))
	require.NoError(t, ApplyUpdateV1(plainCopy, plainEnc, nil))
	require.Equal(t, EncodeStateAsUpdateV1(plainCopy, nil),
		EncodeStateAsUpdateV1(plain.Clone(WithClientID(2)), nil))
}

// ── Independence ─────────────────────────────────────────────────────────────

// TestClone_Independence writes to a clone and to its source and checks that
// neither is visible in the other, for every kind of write: a text insert, a
// delete, a format, a map set and an XML attribute.
func TestClone_Independence(t *testing.T) {
	src := newIndependenceDoc(t)
	srcEnc := EncodeStateAsUpdateV1(src, nil)
	srcSV := src.StateVector()
	clone := src.Clone(WithClientID(2))

	// Every kind of write on the clone leaves the source as it was.
	writeEverything(clone, 2)
	require.Equal(t, srcEnc, EncodeStateAsUpdateV1(src, nil), "writing the clone leaves the source's encoding")
	require.Equal(t, srcSV, src.StateVector(), "writing the clone leaves the source's state vector")

	// And every kind of write on the source leaves the clone as it was.
	cloneEnc := EncodeStateAsUpdateV1(clone, nil)
	cloneSV := clone.StateVector()
	require.NotEqual(t, srcEnc, cloneEnc, "the writes did change the clone")
	writeEverything(src, 1)
	require.Equal(t, cloneEnc, EncodeStateAsUpdateV1(clone, nil), "writing the source leaves the clone's encoding")
	require.Equal(t, cloneSV, clone.StateVector(), "writing the source leaves the clone's state vector")
	require.NotEqual(t, srcEnc, EncodeStateAsUpdateV1(src, nil), "the writes did change the source")
}

// TestClone_IndependenceOfPlainValues checks that a write to one document's
// run of plain values - the content a clone copies as a slice, holding the
// values themselves - is not visible in the other's.
func TestClone_IndependenceOfPlainValues(t *testing.T) {
	src := New(WithClientID(1))
	arr := src.GetArray("a")
	src.Transact(func(tx *Transaction) { arr.Push(tx, []any{"a", "b", "c"}) })
	clone := src.Clone(WithClientID(2))
	cloneArr := clone.GetArray("a")

	// Deleting from the clone's array does not shorten the source's.
	clone.Transact(func(tx *Transaction) { cloneArr.Delete(tx, 1, 1) })
	require.Equal(t, []any{"a", "b", "c"}, arr.ToSlice())
	require.Equal(t, []any{"a", "c"}, cloneArr.ToSlice())

	// Appending to the source's run does not reach the clone's.
	src.Transact(func(tx *Transaction) { arr.Push(tx, []any{"d"}) })
	require.Equal(t, []any{"a", "b", "c", "d"}, arr.ToSlice())
	require.Equal(t, []any{"a", "c"}, cloneArr.ToSlice())
}

// TestClone_IntegratesFutureUpdatesLikeTheCopy applies the same sequence of
// remote updates to a clone and to the encode-and-apply copy of one source,
// and checks their encodings match after every one: a clone integrates what
// arrives later exactly as the copy does.
func TestClone_IntegratesFutureUpdatesLikeTheCopy(t *testing.T) {
	// The source is in applied form, so clone and copy start byte-identical.
	builder := newIndependenceDoc(t)
	src := New(WithClientID(1))
	require.NoError(t, ApplyUpdateV1(src, EncodeStateAsUpdateV1(builder, nil), nil))

	clone := src.Clone(WithClientID(2))
	copyDoc := New(WithClientID(2))
	require.NoError(t, ApplyUpdateV1(copyDoc, EncodeStateAsUpdateV1(src, nil), nil))
	require.Equal(t, EncodeStateAsUpdateV1(copyDoc, nil), EncodeStateAsUpdateV1(clone, nil))

	// A third peer, holding the same state, writes the remote updates.
	peer := New(WithClientID(3))
	require.NoError(t, ApplyUpdateV1(peer, EncodeStateAsUpdateV1(src, nil), nil))
	rng := rand.New(rand.NewPCG(7, 11))
	sv := peer.StateVector()
	for round := range 25 {
		randomOps(peer, rng)
		update := EncodeStateAsUpdateV1(peer, sv)
		sv = peer.StateVector()
		require.NoError(t, ApplyUpdateV1(clone, update, nil))
		require.NoError(t, ApplyUpdateV1(copyDoc, update, nil))
		require.Equal(t, EncodeStateAsUpdateV1(copyDoc, nil), EncodeStateAsUpdateV1(clone, nil),
			"round %d", round)
		require.Equal(t, copyDoc.StateVector(), clone.StateVector(), "round %d", round)
	}
}

// newIndependenceDoc builds a document holding every type a write below
// touches: text with formatting, a map, an array and an XML element.
func newIndependenceDoc(t *testing.T) *Doc {
	t.Helper()
	d := New(WithClientID(1))
	txt, m, arr, frag := d.GetText("t"), d.GetMap("m"), d.GetArray("a"), d.GetXmlFragment("x")
	d.Transact(func(tx *Transaction) {
		txt.Insert(tx, 0, "the quick brown fox", nil)
		txt.Format(tx, 4, 5, Attributes{"bold": true})
		m.Set(tx, "k", "v")
		arr.Push(tx, []any{1, 2, 3})
		e := NewYXmlElement("p")
		frag.InsertElement(tx, 0, e)
		e.SetAttribute(tx, "id", "p0")
	})
	return d
}

// writeEverything runs one of each kind of write on d.
func writeEverything(d *Doc, tag int) {
	txt, m, arr, frag := d.GetText("t"), d.GetMap("m"), d.GetArray("a"), d.GetXmlFragment("x")
	d.Transact(func(tx *Transaction) {
		txt.Insert(tx, 0, fmt.Sprintf("w%d ", tag), nil)
		txt.Delete(tx, 5, 3)
		txt.Format(tx, 1, 4, Attributes{"italic": true})
		m.Set(tx, "k", fmt.Sprintf("v%d", tag))
		m.Set(tx, fmt.Sprintf("only%d", tag), true)
		arr.Push(tx, []any{tag})
		if e, ok := frag.Children()[0].(*YXmlElement); ok {
			e.SetAttribute(tx, "id", fmt.Sprintf("p%d", tag))
		}
	})
}

// ── Races ────────────────────────────────────────────────────────────────────

// TestClone_ConcurrentReadsWhileCloning clones a document while other
// goroutines read it, which -race checks for unsynchronised access, and checks
// that every clone is the document the readers see.
func TestClone_ConcurrentReadsWhileCloning(t *testing.T) {
	src := newIndependenceDoc(t)
	srcText := src.GetText("t")
	for i := range 20 {
		src.Transact(func(tx *Transaction) {
			srcText.Insert(tx, 0, fmt.Sprintf("line %d\n", i), nil)
		})
	}
	srcEnc := EncodeStateAsUpdateV1(src, nil)
	srcSV := src.StateVector()
	// Every clone must be the encode-and-apply copy of the document the
	// readers see, byte for byte.
	decodeCopy := New(WithClientID(1000))
	require.NoError(t, ApplyUpdateV1(decodeCopy, srcEnc, nil))
	want := EncodeStateAsUpdateV1(decodeCopy, nil)

	const readers, cloners, rounds = 6, 4, 25
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range rounds {
				_ = src.GetText("t").ToString()
				_ = src.GetMap("m").Entries()
				_ = src.GetArray("a").ToSlice()
				_ = src.GetXmlFragment("x").ToXML()
				_ = src.StateVector()
				_ = src.GetSubdocGUIDs()
				_ = EncodeStateAsUpdateV1(src, nil)
			}
		}()
	}
	errs := make(chan error, cloners*rounds)
	for c := range cloners {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for i := range rounds {
				clone := src.Clone(WithClientID(ClientID(1000 + id)))
				if got := EncodeStateAsUpdateV1(clone, nil); !bytes.Equal(want, got) {
					errs <- fmt.Errorf("cloner %d round %d: encoding differs", id, i)
				}
			}
		}(c)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	// The readers and the cloners left the source as it was.
	require.Equal(t, srcEnc, EncodeStateAsUpdateV1(src, nil))
	require.Equal(t, srcSV, src.StateVector())
}

func TestClone_PendingQueueWithEmptiedSlots(t *testing.T) {
	src := New(WithClientID(1))
	src.GetText("t")
	for _, clock := range []uint64{1, 10, 20} {
		require.NoError(t, ApplyUpdateV1(src, itemAfter(99, clock, ID{Client: 5}, "abc"), nil))
	}
	require.NoError(t, ApplyUpdateV1(src, itemAfter(99, 0, ID{Client: 5}, "xyzw"), nil))
	require.Equal(t, 1, src.store.pending.dead)
	clone := src.Clone(WithClientID(2))
	requireCloneIsomorphic(t, src, clone)
	requireCloneDisjoint(t, src, clone)
	require.Equal(t, src.PendingStats(), clone.PendingStats())
	filler := New(WithClientID(5))
	filler.Transact(func(txn *Transaction) { txn.GetText("t").Insert(txn, 0, "a", nil) })
	update := EncodeStateAsUpdateV1(filler, nil)
	for _, doc := range []*Doc{src, clone} {
		require.NoError(t, ApplyUpdateV1(doc, update, nil))
	}
	require.Equal(t, EncodeStateAsUpdateV1(src, nil), EncodeStateAsUpdateV1(clone, nil))
}

func TestClone_DeferredMoveHeapAndCounters(t *testing.T) {
	src := New(WithClientID(1), WithGC(false))
	arr := src.GetArray("a")
	var moves []*Item
	src.Transact(func(txn *Transaction) {
		for i := range 4 {
			target := ID{Client: 8, Clock: uint64(i)}
			move := &Item{ID: ID{Client: 1, Clock: uint64(i)}, Parent: &arr.abstractType, Content: NewContentMove(&target, 1)}
			require.True(t, integrated(tryIntegrate(txn, move)))
			moves = append(moves, move)
		}
	})
	src.Transact(func(txn *Transaction) { moves[0].delete(txn) })
	clone := src.Clone(WithClientID(2), WithGC(false))
	requireCloneIsomorphic(t, src, clone)
	require.Equal(t, src.store.pendingMoveCount, clone.store.pendingMoveCount)
	require.Equal(t, src.store.deletedPendingMoves, clone.store.deletedPendingMoves)
	require.Equal(t, src.store.pendingMoveMapCap, clone.store.pendingMoveMapCap)
	clone.Transact(func(txn *Transaction) { clone.store.Find(moves[1].ID).delete(txn) })
	require.Equal(t, 1, src.store.deletedPendingMoves)
	require.Equal(t, 2, clone.store.deletedPendingMoves)
}

func TestClone_ConcurrentRootCreation(t *testing.T) {
	src := New()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 100 {
			src.GetText(fmt.Sprintf("root-%d", i))
		}
	}()
	for range 100 {
		require.NotNil(t, src.Clone())
	}
	wg.Wait()
}

func TestClone_ReplacesAClientIDAlreadyPresentInTheCopiedState(t *testing.T) {
	src := New(WithClientID(1))
	src.Transact(func(txn *Transaction) { txn.GetText("t").Insert(txn, 0, "hello", nil) })
	clone := src.Clone(WithClientID(1))
	require.NotEqual(t, ClientID(1), clone.ClientID())
	require.Equal(t, ClientID(99), src.Clone(WithClientID(99)).ClientID())
	clone.Transact(func(txn *Transaction) { txn.GetText("t").Insert(txn, 5, " copy", nil) })
	require.NoError(t, ApplyUpdateV1(src, EncodeStateAsUpdateV1(clone, src.StateVector()), nil))
	require.Equal(t, "hello copy", src.GetText("t").ToString())
}
