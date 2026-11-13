package crdt

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A value that reads back from Any tag 124 is a Go float32 that holds the
// written number exactly. Printed with the shortest digits that identify a
// float32 it reads as another number: float32(1<<40) as 1099511600000. Each
// reader below prints the digits the value holds instead, which is what the
// writer prints and what Yjs's JSON.stringify prints for the same Number.
//
// A float32 holds each value below exactly. A Go integer reaches tag 124 the
// way lib0 writes the same Number, and a Go float64 reaches it because
// WriteAny narrows it; a JavaScript peer writes the same bytes for either.
// json is JSON.stringify's output for the value, from Node.
var float32ExactValues = []struct {
	name string
	v    any
	f    float64
	json string
}{
	{"int64 2^40", int64(1 << 40), 1 << 40, "1099511627776"},
	{"int64 -2^31", int64(math.MinInt32), math.MinInt32, "-2147483648"},
	{"int64 ms timestamp", int64(1718999973888), 1718999973888, "1718999973888"},
	{"float64 2^40", float64(1 << 40), 1 << 40, "1099511627776"},
	{"float64 -2^31", float64(math.MinInt32), math.MinInt32, "-2147483648"},
	{"float64 ms timestamp", float64(1718999973888), 1718999973888, "1718999973888"},
}

var float32PrintUpdateVersions = []struct {
	name   string
	encode func(*Doc, StateVector) []byte
	apply  func(*Doc, []byte, any) error
}{
	{"V1", EncodeStateAsUpdateV1, ApplyUpdateV1},
	{"V2", EncodeStateAsUpdateV2, ApplyUpdateV2},
}

// One document writes the value, a second applies its update, and both print
// the same JSON: top-level and inside plain maps and arrays.
func TestUnit_ToJSON_ReplicaPrintsTheWrittenNumber(t *testing.T) {
	for _, c := range float32ExactValues {
		for _, version := range float32PrintUpdateVersions {
			t.Run(c.name+"/"+version.name, func(t *testing.T) {
				writer := newTestDoc(1)
				m, arr := writer.GetMap("m"), writer.GetArray("a")
				writer.Transact(func(txn *Transaction) {
					m.Set(txn, "v", c.v)
					m.Set(txn, "plain", map[string]any{"v": c.v, "list": []any{c.v}})
					arr.Push(txn, []any{c.v, []any{c.v}})
				})
				replica := newTestDoc(2)
				require.NoError(t, version.apply(replica, version.encode(writer, nil), nil))

				wantMap := `{"plain":{"list":[` + c.json + `],"v":` + c.json + `},"v":` + c.json + `}`
				wantArray := `[` + c.json + `,[` + c.json + `]]`
				for name, doc := range map[string]*Doc{"writer": writer, "replica": replica} {
					got, err := doc.GetMap("m").ToJSON()
					require.NoError(t, err)
					assert.Equal(t, wantMap, string(got), "%s YMap.ToJSON", name)
					got, err = doc.GetArray("a").ToJSON()
					require.NoError(t, err)
					assert.Equal(t, wantArray, string(got), "%s YArray.ToJSON", name)
				}
			})
		}
	}
}

// Exponent form and its cutoffs follow the float64 the float32 equals, as in
// JSON.stringify; json is Node's output for Math.fround of each value.
func TestUnit_ToJSON_Float32PrintsJSONStringifyDigits(t *testing.T) {
	for _, c := range []struct {
		v    float32
		json string
	}{
		{0.1, "0.10000000149011612"},
		{1.5, "1.5"},
		{1 << 53, "9007199254740992"},
		{1e20, "100000002004087730000"},
		{1e21, "1.0000000200408773e+21"},
		{1e-6, "9.999999974752427e-7"},
		{9.99e-7, "9.990000080506434e-7"},
		{math.MaxFloat32, "3.4028234663852886e+38"},
		{math.SmallestNonzeroFloat32, "1.401298464324817e-45"},
	} {
		doc := newTestDoc(1)
		arr := doc.GetArray("a")
		doc.Transact(func(txn *Transaction) { arr.Push(txn, []any{c.v, []any{c.v}}) })
		got, err := arr.ToJSON()
		require.NoError(t, err)
		assert.Equal(t, `[`+c.json+`,[`+c.json+`]]`, string(got), "float32(%v)", c.v)
	}
}

// fmtValToJSON writes a text attribute or embed as V1's JSON text.
func TestUnit_FmtValToJSON_Float32AsJSONStringify(t *testing.T) {
	for _, c := range []struct {
		in   any
		want string
	}{
		{float32(1 << 40), "1099511627776"},
		{float32(math.MinInt32), "-2147483648"},
		{float32(1718999973888), "1718999973888"},
		{float32(0.1), "0.10000000149011612"},
		{[]any{float32(1 << 40), "s"}, `[1099511627776,"s"]`},
		{map[string]any{"ts": float32(1718999973888), "n": []any{float32(0.1)}}, `{"n":[0.10000000149011612],"ts":1718999973888}`},
	} {
		assert.Equal(t, c.want, fmtValToJSON(c.in), "fmtValToJSON(%v)", c.in)
	}
}

// V2 carries a text attribute or embed as lib0 Any, V1 as JSON text. A
// document that receives V2 and re-encodes as V1 passes on the value written.
func TestUnit_TextValues_V2ThenV1KeepTheWrittenNumber(t *testing.T) {
	for _, c := range float32ExactValues {
		t.Run(c.name, func(t *testing.T) {
			writer := newTestDoc(1)
			txt := writer.GetText("t")
			writer.Transact(func(txn *Transaction) {
				txt.Insert(txn, 0, "ab", Attributes{"ts": c.v})
				txt.InsertEmbed(txn, 2, map[string]any{"w": c.v}, nil)
			})
			relay := newTestDoc(2)
			require.NoError(t, ApplyUpdateV2(relay, EncodeStateAsUpdateV2(writer, nil), nil))
			reader := newTestDoc(3)
			require.NoError(t, ApplyUpdateV1(reader, EncodeStateAsUpdateV1(relay, nil), nil))

			assert.Equal(t, []Delta{
				{Op: DeltaOpInsert, Insert: "ab", Attributes: Attributes{"ts": c.f}},
				{Op: DeltaOpInsert, Insert: map[string]any{"w": c.f}},
			}, reader.GetText("t").ToDelta())
		})
	}
}

// An XML attribute's string rendering names the number written, whichever Go
// type the replica holds it as.
func TestUnit_YXmlElement_GetAttribute_ReplicaShowsTheWrittenNumber(t *testing.T) {
	for _, c := range float32ExactValues {
		for _, version := range float32PrintUpdateVersions {
			t.Run(c.name+"/"+version.name, func(t *testing.T) {
				writer := newTestDoc(1)
				frag := writer.GetXmlFragment("x")
				writer.Transact(func(txn *Transaction) {
					el := NewYXmlElement("e")
					el.SetAttributeValue(txn, "v", c.v)
					frag.InsertElement(txn, 0, el)
				})
				replica := newTestDoc(2)
				require.NoError(t, version.apply(replica, version.encode(writer, nil), nil))

				kids := replica.GetXmlFragment("x").Children()
				require.Len(t, kids, 1)
				got, ok := kids[0].(*YXmlElement).GetAttribute("v")
				require.True(t, ok)
				n, err := strconv.ParseFloat(got, 64)
				require.NoError(t, err)
				assert.InDelta(t, c.f, n, 0, "GetAttribute = %s", got)
				assert.Equal(t, `<e v="`+got+`"></e>`, replica.GetXmlFragment("x").ToXML())
			})
		}
	}
}
