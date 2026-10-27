package crdt

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/reearth/ygo/encoding"
)

// resolveWithinUpdatePendingFixedPoint is resolveWithinUpdatePending as it was
// through 1.51.3-sami.5: it retries every deferred item in arrival order, pass
// after pass, until a pass integrates nothing, then parks the rest. It is the
// oracle the dependency-directed retry is checked against.
func resolveWithinUpdatePendingFixedPoint(txn *Transaction, pending []*Item) error {
	for len(pending) > 0 {
		remaining := pending[:0]
		for _, item := range pending {
			if !tryIntegrate(txn, item) {
				remaining = append(remaining, item)
			}
		}
		if len(remaining) == len(pending) {
			for _, item := range remaining {
				if err := parkDeferredItem(txn, item); err != nil {
					return err
				}
			}
			break
		}
		pending = remaining
	}
	return nil
}

// applyStep is one update of a differential case.
type applyStep struct {
	update []byte
	v2     bool
}

// applyCase applies its steps in order to a fresh document built with opts.
type applyCase struct {
	name  string
	opts  []DocOption
	steps []applyStep
}

// applySnapshot is everything about a document the retry of deferred items
// can change: the apply's error, the encoded state, the state vector, the
// pending stats, the delete set, the pending deletes, and the parked items in
// order with the clocks the queue waits on.
type applySnapshot struct {
	err       string
	state     []byte
	sv        StateVector
	stats     PendingStats
	deletes   []byte
	pendingDs []byte
	parked    []string
	missing   StateVector
}

func snapshotDoc(d *Doc, err error) (s applySnapshot) {
	if err != nil {
		s.err = err.Error()
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				s.state = []byte(fmt.Sprintf("encode panicked: %v", r))
			}
		}()
		s.state = EncodeStateAsUpdateV1(d, nil)
	}()
	s.sv = d.StateVector()
	s.stats = d.PendingStats()
	enc := encoding.NewEncoder()
	encodeDeleteSet(enc, buildDeleteSet(d.store))
	s.deletes = append([]byte(nil), enc.Bytes()...)
	enc = encoding.NewEncoder()
	encodeDeleteSet(enc, d.store.pendingDs)
	s.pendingDs = append([]byte(nil), enc.Bytes()...)
	if p := d.store.pending; p != nil {
		for _, item := range p.items {
			s.parked = append(s.parked, fmt.Sprintf("%d:%d+%d parent=%t sub=%v", item.ID.Client, item.ID.Clock, item.Content.Len(), item.Parent != nil, item.ParentSub != nil))
		}
		s.missing = maps2SV(p.missing)
	}
	return s
}

func maps2SV(sv StateVector) StateVector {
	out := make(StateVector, len(sv))
	for c, k := range sv {
		out[c] = k
	}
	return out
}

func (s applySnapshot) diff(o applySnapshot) string {
	var d []string
	if s.err != o.err {
		d = append(d, fmt.Sprintf("error %q, oracle %q", s.err, o.err))
	}
	if !bytes.Equal(s.state, o.state) {
		d = append(d, "EncodeStateAsUpdateV1 bytes differ")
	}
	if !reflect.DeepEqual(s.sv, o.sv) {
		d = append(d, fmt.Sprintf("state vector %v, oracle %v", s.sv, o.sv))
	}
	if !reflect.DeepEqual(s.stats, o.stats) {
		d = append(d, fmt.Sprintf("PendingStats %+v, oracle %+v", s.stats, o.stats))
	}
	if !bytes.Equal(s.deletes, o.deletes) {
		d = append(d, "delete set differs")
	}
	if !bytes.Equal(s.pendingDs, o.pendingDs) {
		d = append(d, "pending deletes differ")
	}
	if !reflect.DeepEqual(s.parked, o.parked) {
		d = append(d, fmt.Sprintf("parked items %v, oracle %v", s.parked, o.parked))
	}
	if !reflect.DeepEqual(s.missing, o.missing) {
		d = append(d, fmt.Sprintf("pending missing %v, oracle %v", s.missing, o.missing))
	}
	return strings.Join(d, "; ")
}

// diffCoverage counts what the differential inputs exercised.
type diffCoverage struct {
	cases, steps    int
	deferred        int // resolveDeferred calls with deferred items
	deferredItems   int
	parkedSteps     int // steps after which items sat parked
	multiParkedStep int // steps after which two or more items sat parked
}

// runDiffCase applies c with resolve swapped in and snapshots the document
// after every step.
func runDiffCase(c applyCase, resolve func(*Transaction, []*Item) error) []applySnapshot {
	saved := resolveDeferred
	resolveDeferred = resolve
	defer func() { resolveDeferred = saved }()
	d := New(append([]DocOption{WithClientID(1 << 40)}, c.opts...)...)
	snaps := make([]applySnapshot, 0, len(c.steps))
	for _, step := range c.steps {
		var err error
		if step.v2 {
			err = ApplyUpdateV2(d, step.update, nil)
		} else {
			err = ApplyUpdateV1(d, step.update, nil)
		}
		snaps = append(snaps, snapshotDoc(d, err))
	}
	return snaps
}

// checkDiffCases runs every case under the dependency-directed retry and the
// fixed-point oracle and fails on any difference after any step.
func checkDiffCases(t *testing.T, cases []applyCase) diffCoverage {
	t.Helper()
	var cov diffCoverage
	counted := func(txn *Transaction, pending []*Item) error {
		if len(pending) > 0 {
			cov.deferred++
			cov.deferredItems += len(pending)
		}
		return resolveWithinUpdatePending(txn, pending)
	}
	for _, c := range cases {
		got := runDiffCase(c, counted)
		want := runDiffCase(c, resolveWithinUpdatePendingFixedPoint)
		cov.cases++
		cov.steps += len(c.steps)
		for i := range got {
			if d := got[i].diff(want[i]); d != "" {
				t.Errorf("%s: step %d/%d: %s", c.name, i+1, len(c.steps), d)
				break
			}
			if n := got[i].stats.Items; n > 0 {
				cov.parkedSteps++
				if n > 1 {
					cov.multiParkedStep++
				}
			}
		}
	}
	return cov
}

func (c diffCoverage) String() string {
	return fmt.Sprintf("%d inputs, %d updates applied, %d with deferred items (%d items), %d steps leaving items parked (%d leaving two or more)",
		c.cases, c.steps, c.deferred, c.deferredItems, c.parkedSteps, c.multiParkedStep)
}

// oneUpdateCases applies each update alone, as V1 and as V2.
func oneUpdateCases(name string, updates [][]byte) []applyCase {
	cases := make([]applyCase, 0, 2*len(updates))
	for i, u := range updates {
		cases = append(cases,
			applyCase{name: fmt.Sprintf("%s#%d/v1", name, i), steps: []applyStep{{update: u}}},
			applyCase{name: fmt.Sprintf("%s#%d/v2", name, i), steps: []applyStep{{update: u, v2: true}}})
	}
	return cases
}

// collectHex returns every hex-decodable string in a JSON value, walking
// object keys in sorted order.
func collectHex(v any, out [][]byte) [][]byte {
	switch x := v.(type) {
	case string:
		if len(x) >= 2 && len(x)%2 == 0 {
			if b, err := hex.DecodeString(x); err == nil {
				out = append(out, b)
			}
		}
	case []any:
		for _, e := range x {
			out = collectHex(e, out)
		}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			out = collectHex(x[k], out)
		}
	}
	return out
}

// fixtureDiffCases builds cases from every Yjs conformance fixture under
// testdata: each hex update alone (V1 and V2), and each fixture row's updates
// applied in sequence forwards and backwards.
func fixtureDiffCases(t *testing.T) []applyCase {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no conformance fixtures: %v", err)
	}
	var cases []applyCase
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		rows, ok := doc.([]any)
		if !ok {
			rows, _ = doc.(map[string]any)["fixtures"].([]any)
		}
		if len(rows) == 0 {
			t.Fatalf("%s: no fixture rows", file)
		}
		for r, row := range rows {
			name := fmt.Sprintf("%s[%d]", filepath.Base(file), r)
			updates := collectHex(row, nil)
			cases = append(cases, oneUpdateCases(name, updates)...)
			if len(updates) > 1 {
				var fwd, back applyCase
				fwd.name, back.name = name+"/forward", name+"/backward"
				for i := range updates {
					fwd.steps = append(fwd.steps, applyStep{update: updates[i]})
					back.steps = append(back.steps, applyStep{update: updates[len(updates)-1-i]})
				}
				cases = append(cases, fwd, back)
			}
		}
	}
	return cases
}

// fuzzDiffCases builds cases from the fuzz seeds FuzzApplyUpdateV1/V2 add and
// every committed corpus file of the repository's fuzz targets.
func fuzzDiffCases(t *testing.T) []applyCase {
	t.Helper()
	seeds := fuzzSeedsV1()
	seeds = append(seeds, contentJSONFuzzSeeds(false)...)
	seeds = append(seeds, contentJSONFuzzSeeds(true)...)
	for _, h := range legacyContentJSONV1Fixtures {
		b, err := hex.DecodeString(h)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, b)
	}
	files, err := filepath.Glob(filepath.Join("..", "*", "testdata", "fuzz", "*", "*"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no fuzz corpus: %v", err)
	}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if len(lines) < 2 || lines[0] != "go test fuzz v1" {
			t.Fatalf("%s: not a fuzz corpus file", file)
		}
		for _, line := range lines[1:] {
			quoted, ok := strings.CutPrefix(line, "[]byte(")
			if !ok {
				continue
			}
			b, err := strconv.Unquote(strings.TrimSuffix(quoted, ")"))
			if err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			seeds = append(seeds, []byte(b))
		}
	}
	return oneUpdateCases("fuzz", seeds)
}

// randomCorpus is a randomized many-client editing session: peers with
// out-of-order client ids insert and delete in a text, an array, a map and an
// XML fragment, and sync with each other at random.
type randomCorpus struct {
	rng     *rand.Rand
	peers   []*Doc
	updates [][]byte // each local transaction's update, in commit order
	svs     []StateVector
	opts    []DocOption
}

var randomRunes = []string{"a", "b", "c", "xy", "é", "日本", "😀", "z\n"}

func newRandomCorpus(seed uint64) *randomCorpus {
	rng := rand.New(rand.NewPCG(seed, 2))
	c := &randomCorpus{rng: rng}
	if rng.IntN(2) == 0 {
		c.opts = append(c.opts, WithGC(false))
	}
	n := 2 + rng.IntN(7)
	base := uint64(rng.Uint32())
	for i := range n {
		var id uint64
		switch seed % 3 {
		case 0: // later writers sort first: origins lie in later groups
			id = base + uint64(n-i)*7
		case 1:
			id = base + uint64(i)*7
		default:
			id = uint64(rng.Uint32())
		}
		c.peers = append(c.peers, New(append([]DocOption{WithClientID(ClientID(id))}, c.opts...)...))
	}
	rounds := 20 + rng.IntN(60)
	for range rounds {
		if rng.IntN(5) == 0 {
			c.sync()
			continue
		}
		c.edit(c.peers[rng.IntN(len(c.peers))])
	}
	return c
}

func (c *randomCorpus) str() string {
	var b strings.Builder
	for range 1 + c.rng.IntN(4) {
		b.WriteString(randomRunes[c.rng.IntN(len(randomRunes))])
	}
	return b.String()
}

func (c *randomCorpus) sync() {
	a, b := c.peers[c.rng.IntN(len(c.peers))], c.peers[c.rng.IntN(len(c.peers))]
	if a == b {
		return
	}
	if err := ApplyUpdateV1(b, EncodeStateAsUpdateV1(a, b.StateVector()), nil); err != nil {
		panic(err)
	}
	if c.rng.IntN(3) == 0 {
		c.svs = append(c.svs, b.StateVector())
	}
}

func (c *randomCorpus) edit(p *Doc) {
	rng := c.rng
	before := p.StateVector()
	text, arr, m, frag := p.GetText("t"), p.GetArray("a"), p.GetMap("m"), p.GetXmlFragment("x")
	p.Transact(func(txn *Transaction) {
		for range 1 + rng.IntN(3) {
			switch rng.IntN(9) {
			case 0:
				text.Insert(txn, rng.IntN(text.Len()+1), c.str(), nil)
			case 1:
				if n := text.Len(); n > 0 {
					at := rng.IntN(n)
					text.Delete(txn, at, 1+rng.IntN(min(4, n-at)))
				}
			case 2:
				arr.Insert(txn, rng.IntN(arr.Len()+1), []any{rng.IntN(100), c.str(), rng.IntN(2) == 0})
			case 3:
				if n := arr.Len(); n > 0 {
					at := rng.IntN(n)
					arr.Delete(txn, at, 1+rng.IntN(min(3, n-at)))
				}
			case 4:
				m.Set(txn, fmt.Sprintf("k%d", rng.IntN(6)), c.str())
			case 5:
				m.Delete(txn, fmt.Sprintf("k%d", rng.IntN(6)))
			case 6:
				el := NewYXmlElement("p")
				frag.InsertElement(txn, rng.IntN(frag.Len()+1), el)
				el.SetAttribute(txn, "id", c.str())
				xt := NewYXmlText()
				el.InsertText(txn, 0, xt)
				xt.Insert(txn, 0, c.str(), nil)
			case 7:
				kids := frag.Children()
				if len(kids) == 0 {
					continue
				}
				el, ok := kids[rng.IntN(len(kids))].(*YXmlElement)
				if !ok {
					continue
				}
				if rng.IntN(4) == 0 {
					el.SetAttribute(txn, "id", c.str())
					continue
				}
				for _, kid := range el.Children() {
					if xt, ok := kid.(*YXmlText); ok {
						if n := xt.Len(); n > 0 && rng.IntN(2) == 0 {
							at := rng.IntN(n)
							xt.Delete(txn, at, 1+rng.IntN(min(3, n-at)))
						} else {
							xt.Insert(txn, rng.IntN(n+1), c.str(), nil)
						}
					}
				}
			case 8:
				if n := frag.Len(); n > 0 {
					frag.Delete(txn, rng.IntN(n), 1)
				}
			}
		}
	})
	if u := EncodeStateAsUpdateV1(p, before); len(u) > 2 {
		c.updates = append(c.updates, u)
	}
	if rng.IntN(4) == 0 {
		c.svs = append(c.svs, p.StateVector())
	}
}

// hub returns a document holding every peer's edits.
func (c *randomCorpus) hub() *Doc {
	hub := New(c.opts...)
	for _, u := range c.updates {
		if err := ApplyUpdateV1(hub, u, nil); err != nil {
			panic(err)
		}
	}
	return hub
}

// craftGroups writes an update of client groups cut at random from hub's
// store: a client can appear in several groups whose clock ranges overlap,
// repeat or leave gaps, in any client order, so one struct can cover another
// that waits on a dependency. Real encoders never write this; a peer can.
func craftGroups(rng *rand.Rand, hub *Doc, v2 bool) []byte {
	clients := make([]ClientID, 0, len(hub.store.clients))
	for client := range hub.store.clients {
		clients = append(clients, client)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i] < clients[j] })
	n := 1 + rng.IntN(8)
	groups := make([][]*Item, 0, n)
	for range n {
		items := hub.store.clients[clients[rng.IntN(len(clients))]]
		at := rng.IntN(len(items))
		groups = append(groups, items[at:at+1+rng.IntN(min(6, len(items)-at))])
	}
	ds := newDeleteSet()
	if rng.IntN(2) == 0 {
		ds = buildDeleteSet(hub.store)
	}
	return encodeGroups(groups, ds, hub.store, v2)
}

// encodeGroups writes an update of the given client groups, each a run of
// clock-contiguous items, as V1 or V2.
func encodeGroups(groups [][]*Item, ds DeleteSet, store *StructStore, v2 bool) []byte {
	if !v2 {
		enc := encoding.NewEncoder()
		enc.WriteVarUint(uint64(len(groups)))
		for _, items := range groups {
			enc.WriteVarUint(uint64(len(items)))
			enc.WriteVarUint(uint64(items[0].ID.Client))
			enc.WriteVarUint(items[0].ID.Clock)
			for _, item := range items {
				encodeItem(enc, item, 0, store)
			}
		}
		encodeDeleteSet(enc, ds)
		return enc.Bytes()
	}
	enc := newV2Encoder()
	enc.restEnc.WriteVarUint(uint64(len(groups)))
	for _, items := range groups {
		enc.restEnc.WriteVarUint(uint64(len(items)))
		enc.writeClient(items[0].ID.Client)
		enc.restEnc.WriteVarUint(items[0].ID.Clock)
		for _, item := range items {
			encodeItemV2(enc, item, 0, store)
		}
	}
	encodeDeleteSetV2(enc, ds)
	return enc.toBytes()
}

// synthUpdate writes an update of random structs over four clients and small
// clocks: strings in a root text, values in a root array or under a root map
// key, values in a container named by id, and GC structs, with random origins
// and right origins and possibly several groups per client. Different structs
// can carry the same clocks with different dependencies, as an equivocating
// peer's would, so one struct covers another that waits on something else.
func synthUpdate(rng *rand.Rand, source *Doc, v2 bool) []byte {
	text, arr, m := source.GetText("t"), source.GetArray("a"), source.GetMap("m")
	randomID := func() *ID {
		return &ID{Client: ClientID(1 + rng.IntN(4)), Clock: uint64(rng.IntN(16))}
	}
	n := 1 + rng.IntN(6)
	groups := make([][]*Item, 0, n)
	for range n {
		client, clock := ClientID(1+rng.IntN(4)), uint64(rng.IntN(12))
		size := 1 + rng.IntN(4)
		items := make([]*Item, 0, size)
		for range size {
			item := &Item{ID: ID{Client: client, Clock: clock}}
			n := 1 + rng.IntN(3)
			switch rng.IntN(5) {
			case 0:
				item.Parent, item.Content = &text.abstractType, NewContentString(strings.Repeat("s", n))
			case 1:
				item.Parent, item.Content = &arr.abstractType, NewContentAny(make([]any, n)...)
			case 2:
				key := fmt.Sprintf("k%d", rng.IntN(3))
				item.Parent, item.ParentSub, item.Content = &m.abstractType, &key, NewContentAny(n)
			case 3:
				item.parentID, item.Content = randomID(), NewContentAny(make([]any, n)...)
			default:
				item.Deleted, item.Content = true, NewContentDeleted(n)
			}
			if !item.Deleted {
				if rng.IntN(3) != 0 {
					item.Origin = randomID()
				}
				if rng.IntN(3) == 0 {
					item.OriginRight = randomID()
				}
			}
			items = append(items, item)
			clock += uint64(item.Content.Len())
		}
		groups = append(groups, items)
	}
	return encodeGroups(groups, newDeleteSet(), source.store, v2)
}

// synthDiffCases applies one to three synthUpdates in turn to a fresh
// document, as V1 and as V2; a third of the documents bound their conflict
// scans tightly and a third cap their pending queue at a few items.
func synthDiffCases(n int) []applyCase {
	rng := rand.New(rand.NewPCG(5, 5))
	source := New()
	cases := make([]applyCase, 0, n)
	for i := range n {
		v2 := i%2 == 1
		c := applyCase{name: fmt.Sprintf("synth=%d", i)}
		switch i % 6 / 2 { // V1 and V2 alike
		case 1:
			c.opts = []DocOption{WithMaxIntegrateScan(1 + rng.IntN(6))}
		case 2:
			c.opts = []DocOption{WithMaxPendingItems(1 + rng.IntN(4))}
		}
		for range 1 + rng.IntN(3) {
			c.steps = append(c.steps, applyStep{update: synthUpdate(rng, source, v2), v2: v2})
		}
		cases = append(cases, c)
	}
	return cases
}

// cases builds the corpus's differential inputs: the whole state; every
// update in a random order (parking items across updates); state diffs from
// random peers against random earlier state vectors (partially overlapping
// updates); random chunks of the updates merged, in random order; and crafted
// updates whose client groups overlap (craftGroups), alone and before the
// state. Each sequence is also applied as V2, and under a small pending cap
// and a small scan bound.
func (c *randomCorpus) cases(name string) []applyCase {
	rng := c.rng
	hub := c.hub()
	state := EncodeStateAsUpdateV1(hub, nil)
	cases := []applyCase{
		{name: name + "/state/v1", opts: c.opts, steps: []applyStep{{update: state}}},
		{name: name + "/state/v2", opts: c.opts, steps: []applyStep{{update: mustV2(state), v2: true}}},
	}
	if len(hub.store.clients) > 0 {
		for i := range 4 {
			v2 := i%2 == 1
			crafted := applyStep{update: craftGroups(rng, hub, v2), v2: v2}
			again := applyStep{update: craftGroups(rng, hub, v2), v2: v2}
			cases = append(cases,
				applyCase{name: fmt.Sprintf("%s/crafted%d", name, i), opts: c.opts, steps: []applyStep{crafted, again}},
				applyCase{name: fmt.Sprintf("%s/crafted%d/state", name, i), opts: c.opts, steps: []applyStep{crafted, {update: state}}})
		}
	}

	shuffled := append([][]byte(nil), c.updates...)
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	n := 2 + rng.IntN(6)
	overlapping := make([][]byte, 0, n)
	for range n {
		p := c.peers[rng.IntN(len(c.peers))]
		var sv StateVector
		if len(c.svs) > 0 && rng.IntN(4) != 0 {
			sv = c.svs[rng.IntN(len(c.svs))]
		}
		overlapping = append(overlapping, EncodeStateAsUpdateV1(p, sv))
	}

	var merged [][]byte
	for rest := shuffled; len(rest) > 0; {
		n := 1 + rng.IntN(min(5, len(rest)))
		u, err := MergeUpdatesV1(rest[:n]...)
		if err != nil {
			panic(err)
		}
		merged = append(merged, u)
		rest = rest[n:]
	}
	rng.Shuffle(len(merged), func(i, j int) { merged[i], merged[j] = merged[j], merged[i] })

	for _, seq := range []struct {
		name    string
		updates [][]byte
	}{{"shuffled", shuffled}, {"overlapping", overlapping}, {"merged", merged}} {
		v1 := applyCase{name: name + "/" + seq.name + "/v1", opts: c.opts}
		mixed := applyCase{name: name + "/" + seq.name + "/mixed", opts: c.opts}
		for _, u := range seq.updates {
			v1.steps = append(v1.steps, applyStep{update: u})
			if rng.IntN(2) == 0 {
				mixed.steps = append(mixed.steps, applyStep{update: mustV2(u), v2: true})
			} else {
				mixed.steps = append(mixed.steps, applyStep{update: u})
			}
		}
		capped, bounded := v1, v1
		capped.name, bounded.name = v1.name+"/cap", v1.name+"/scan"
		capped.opts = append(append([]DocOption(nil), c.opts...), WithMaxPendingItems(1+rng.IntN(8)))
		bounded.opts = append(append([]DocOption(nil), c.opts...), WithMaxIntegrateScan(1+rng.IntN(40)))
		cases = append(cases, v1, mixed, capped, bounded)
	}
	return cases
}

func mustV2(v1 []byte) []byte {
	v2, err := UpdateV1ToV2(v1)
	if err != nil {
		panic(err)
	}
	return v2
}

// manyWriterDiffCases builds document-service-shaped inputs: the 400-paragraph
// manyWriterDoc's state after 60, 120 and 240 versions (V1 and V2); a
// 60-version document's updates applied in a random order; and its state
// without one early version, so that most of the update parks. small builds
// each kind once from a 12-version document.
func manyWriterDiffCases(small bool) []applyCase {
	states := []int{60, 120, 240}
	versions, skips := 60, []int{1, 2, 30}
	if small {
		states, versions, skips = nil, 12, []int{1}
	}
	m := newManyWriterDoc(4, 400, false)
	for m.versions < versions {
		m.rewrite()
	}
	var cases []applyCase
	addState := func(n int, state []byte) {
		cases = append(cases,
			applyCase{name: fmt.Sprintf("writers=%d/v1", n), steps: []applyStep{{update: state}}},
			applyCase{name: fmt.Sprintf("writers=%d/v2", n), steps: []applyStep{{update: mustV2(state), v2: true}}})
	}
	if small {
		addState(versions, EncodeStateAsUpdateV1(m.doc, nil))
	}
	for i, state := range manyWriterStates(3, states...) {
		addState(states[i], state)
	}

	rng := rand.New(rand.NewPCG(4, 4))
	shuffled := applyCase{name: fmt.Sprintf("writers=%d/shuffled", versions)}
	for _, i := range rng.Perm(len(m.updates)) {
		shuffled.steps = append(shuffled.steps, applyStep{update: m.updates[i]})
	}
	cases = append(cases, shuffled)

	for _, skip := range skips {
		rest := make([][]byte, 0, len(m.updates)-1)
		rest = append(rest, m.updates[:skip]...)
		rest = append(rest, m.updates[skip+1:]...)
		partial, err := MergeUpdatesV1(rest...)
		if err != nil {
			panic(err)
		}
		cases = append(cases, applyCase{
			name:  fmt.Sprintf("writers=%d/without-version-%d", versions, skip),
			steps: []applyStep{{update: partial}, {update: m.updates[skip]}},
		}, applyCase{
			name:  fmt.Sprintf("writers=%d/without-version-%d/v2", versions, skip),
			steps: []applyStep{{update: mustV2(partial), v2: true}, {update: mustV2(m.updates[skip]), v2: true}},
		})
	}
	return cases
}

// TestWithinUpdateRetry_MatchesFixedPoint checks the dependency-directed
// retry of deferred items against the fixed-point retry it replaced: for every
// input, applying it to a fresh document leaves the same encoded state, state
// vector, pending stats, delete set, pending deletes, and parked items in the
// same order waiting on the same clocks. Under -short or -race (CI runs the
// package under -race within 120 s) each class of input runs a sample.
func TestWithinUpdateRetry_MatchesFixedPoint(t *testing.T) {
	small := testing.Short() || raceEnabled
	t.Run("conformance fixtures", func(t *testing.T) {
		t.Log(checkDiffCases(t, fixtureDiffCases(t)))
	})
	t.Run("fuzz corpus", func(t *testing.T) {
		cases := fuzzDiffCases(t)
		// TestUnit_PendingBudget_OverlappingGroups's update: a later group
		// covers a struct that waits on a parent no group carries.
		for _, version := range []int{1, 2} {
			step := applyStep{update: pendingOverlappingGroups(version), v2: version == 2}
			cases = append(cases,
				applyCase{name: fmt.Sprintf("overlapping-groups/v%d", version), steps: []applyStep{step}},
				applyCase{name: fmt.Sprintf("overlapping-groups/v%d/cap", version), opts: []DocOption{WithMaxPendingItems(1)}, steps: []applyStep{step}})
		}
		t.Log(checkDiffCases(t, cases))
	})
	t.Run("random many-client", func(t *testing.T) {
		seeds := 300
		if small {
			seeds = 10
		}
		cases := make([]applyCase, 0, seeds)
		for seed := range seeds {
			cases = append(cases, newRandomCorpus(uint64(seed)).cases(fmt.Sprintf("seed=%d", seed))...)
		}
		cov := checkDiffCases(t, cases)
		t.Log(cov)
		if cov.deferred == 0 || cov.multiParkedStep == 0 {
			t.Fatalf("the corpus never deferred or parked items: %v", cov)
		}
	})
	t.Run("synthetic groups", func(t *testing.T) {
		n := 4000
		if small {
			n = 400
		}
		cov := checkDiffCases(t, synthDiffCases(n))
		t.Log(cov)
		if cov.multiParkedStep == 0 {
			t.Fatalf("no synthetic input parked items: %v", cov)
		}
	})
	t.Run("many writers", func(t *testing.T) {
		cov := checkDiffCases(t, manyWriterDiffCases(small))
		t.Log(cov)
		if cov.multiParkedStep == 0 {
			t.Fatalf("no many-writer input parked items: %v", cov)
		}
	})
}

// TestWithinUpdateRetry_ParkedOrder applies one many-client session's updates
// to fresh documents in many orders, so the items an update defers integrate
// in different orders and different survivors park, and checks the parked
// items' order against the oracle's after every update.
func TestWithinUpdateRetry_ParkedOrder(t *testing.T) {
	c := newRandomCorpus(3) // seed%3 == 0: later writers have lower ids
	for len(c.updates) < 40 {
		c.edit(c.peers[c.rng.IntN(len(c.peers))])
	}
	rng := rand.New(rand.NewPCG(9, 9))
	cases := make([]applyCase, 0, 50)
	for p := range 50 {
		cs := applyCase{name: fmt.Sprintf("order=%d", p)}
		chunk := make([][]byte, 0, len(c.updates))
		for _, i := range rng.Perm(len(c.updates)) {
			chunk = append(chunk, c.updates[i])
			if rng.IntN(3) == 0 {
				u, err := MergeUpdatesV1(chunk...)
				if err != nil {
					t.Fatal(err)
				}
				cs.steps = append(cs.steps, applyStep{update: u})
				chunk = chunk[:0]
			}
		}
		for _, u := range chunk {
			cs.steps = append(cs.steps, applyStep{update: u})
		}
		cases = append(cases, cs)
	}
	cov := checkDiffCases(t, cases)
	t.Log(cov)
	if cov.multiParkedStep == 0 {
		t.Fatalf("no order parked two or more items: %v", cov)
	}
}
