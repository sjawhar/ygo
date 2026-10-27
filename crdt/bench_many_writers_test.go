package crdt

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
)

// manyWriterDoc builds a document the way a document service writes one: a
// 400-paragraph markdown upload written again 240 times. It is an XML
// fragment of paragraphs, each with a blockId attribute and one text node,
// where every version is written by a writer with a fresh random client id.
// Version v sets 5% of the paragraphs, those with (p+v)%20 == 0, to
// "revision v" and the others to "revision 0" (which restores the 5% version
// v-1 set), each by deleting and inserting the text between the old and new
// text's common prefix and suffix, as such a service's text diff does. As in
// such an upload, every version also writes every paragraph's blockId again.
// Version 0 writes the paragraphs.
//
// ygo encodes client groups in ascending client-id order, so in such a
// document most items' origins lie in a later client group than the item: a
// fresh document decoding the state defers them until that group arrives.
// Each version's blockId writes and text edits sit next to what the previous
// version wrote, so the dependencies chain through every version.
type manyWriterDoc struct {
	doc  *Doc
	frag *YXmlFragment
	rng  *rand.Rand
	// fork makes each version on a fork of the document, as such a service
	// does: the fork decodes the whole state, edits it under its own client id
	// and its update is applied back. Without fork the document edits itself
	// under the version's client id, which yields the same document (see
	// TestManyWriterDoc_ForkEquivalent) without decoding the state per
	// version.
	fork bool
	// versions counts the versions after version 0; updates holds every
	// version's update, version 0's first.
	versions int
	updates  [][]byte
}

func manyWriterText(p, version int) string {
	rev := 0
	if version > 0 && (p+version)%20 == 0 {
		rev = version
	}
	return fmt.Sprintf("Paragraph %d revision %d: %s", p, rev, strings.Repeat("lorem ipsum dolor sit amet ", 9))
}

func newManyWriterDoc(seed uint64, paragraphs int, fork bool) *manyWriterDoc {
	m := &manyWriterDoc{rng: rand.New(rand.NewPCG(seed, 0)), fork: fork}
	m.doc = New(WithClientID(m.clientID()))
	m.frag = m.doc.GetXmlFragment("prosemirror")
	m.version(func(txn *Transaction, frag *YXmlFragment) {
		for p := range paragraphs {
			el := NewYXmlElement("paragraph")
			frag.InsertElement(txn, p, el)
			el.SetAttributeValue(txn, "blockId", manyWriterBlockID(p))
			text := NewYXmlText()
			el.InsertText(txn, 0, text)
			text.Insert(txn, 0, manyWriterText(p, 0), nil)
		}
	})
	return m
}

func manyWriterBlockID(p int) string { return fmt.Sprintf("block-%08d", p) }

func (m *manyWriterDoc) clientID() ClientID { return ClientID(m.rng.Uint32()) }

// version writes one version under a fresh client id.
func (m *manyWriterDoc) version(edit func(*Transaction, *YXmlFragment)) {
	id := m.clientID()
	before := m.doc.StateVector()
	if m.fork {
		fork := New(WithClientID(id))
		if err := ApplyUpdateV1(fork, EncodeStateAsUpdateV1(m.doc, nil), nil); err != nil {
			panic(err)
		}
		frag := fork.GetXmlFragment("prosemirror")
		fork.Transact(func(txn *Transaction) { edit(txn, frag) })
		if err := ApplyUpdateV1(m.doc, EncodeStateAsUpdateV1(fork, before), nil); err != nil {
			panic(err)
		}
	} else {
		m.doc.clientID = id
		m.doc.Transact(func(txn *Transaction) { edit(txn, m.frag) })
	}
	m.updates = append(m.updates, EncodeStateAsUpdateV1(m.doc, before))
}

// rewrite writes the next version.
func (m *manyWriterDoc) rewrite() {
	m.versions++
	m.version(func(txn *Transaction, frag *YXmlFragment) {
		for p, child := range frag.Children() {
			el := child.(*YXmlElement)
			el.SetAttributeValue(txn, "blockId", manyWriterBlockID(p))
			was, now := manyWriterText(p, m.versions-1), manyWriterText(p, m.versions)
			if was == now {
				continue
			}
			prefix := 0
			for was[prefix] == now[prefix] {
				prefix++
			}
			suffix := 0
			for suffix < min(len(was), len(now))-prefix && was[len(was)-1-suffix] == now[len(now)-1-suffix] {
				suffix++
			}
			text := el.Children()[0].(*YXmlText)
			if n := len(was) - suffix - prefix; n > 0 {
				text.Delete(txn, prefix, n)
			}
			if ins := now[prefix : len(now)-suffix]; ins != "" {
				text.Insert(txn, prefix, ins, nil)
			}
		}
	})
}

// manyWriterStates returns the encoded state of a seeded 400-paragraph
// manyWriterDoc after each of the given numbers of versions (ascending).
func manyWriterStates(seed uint64, versions ...int) [][]byte {
	m := newManyWriterDoc(seed, 400, false)
	states := make([][]byte, 0, len(versions))
	for _, n := range versions {
		for m.versions < n {
			m.rewrite()
		}
		states = append(states, EncodeStateAsUpdateV1(m.doc, nil))
	}
	return states
}

// TestManyWriterDoc_ForkEquivalent pins that a manyWriterDoc written on forks,
// as such a document service writes, and one that edits itself under each
// version's client id produce the same updates and state, so the faster build
// stands in for forks.
func TestManyWriterDoc_ForkEquivalent(t *testing.T) {
	forked := newManyWriterDoc(7, 40, true)
	direct := newManyWriterDoc(7, 40, false)
	for range 30 {
		forked.rewrite()
		direct.rewrite()
	}
	for i := range forked.updates {
		if !bytes.Equal(forked.updates[i], direct.updates[i]) {
			t.Fatalf("version %d: forked update differs from the direct one", i)
		}
	}
	if !bytes.Equal(EncodeStateAsUpdateV1(forked.doc, nil), EncodeStateAsUpdateV1(direct.doc, nil)) {
		t.Fatal("forked state differs from the direct one")
	}
}

var manyWriterBench = struct {
	once   sync.Once
	states [][]byte
}{}

var manyWriterBenchVersions = []int{60, 120, 240}

// BenchmarkApplyUpdateV1_ManyWriters decodes the state of a document written
// by 60, 120 and 240 writers (manyWriterDoc) into a fresh document. Most of
// its items wait on an item in a later client group, so the decode's retry of
// deferred items dominates if it is superlinear. The fresh document allows
// 1<<20 pending items, as such a service's documents do: at the default
// 100,000 the 240-writer state's 104,700 deferred items would add the
// pending-budget preflight, a one-off scan of the update, to that size alone.
func BenchmarkApplyUpdateV1_ManyWriters(b *testing.B) {
	manyWriterBench.once.Do(func() {
		manyWriterBench.states = manyWriterStates(1, manyWriterBenchVersions...)
	})
	for i, n := range manyWriterBenchVersions {
		state := manyWriterBench.states[i]
		b.Run(fmt.Sprintf("writers=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(state)))
			for range b.N {
				if err := ApplyUpdateV1(New(WithClientID(1), WithMaxPendingItems(1<<20)), state, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
