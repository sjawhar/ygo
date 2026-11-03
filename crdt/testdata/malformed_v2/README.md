# Malformed V2 updates that now reach the apply at caps 1-30

The 2,760 `s` V2 updates are the ones whose result at a small cap changes when #334's skip uses the V2 rest cursor (`pendingBudget.deleteSetLimit`, #334 commit `09d5fa53`). Before that skip, `checkParkedDeletes` walked to the delete set and refused each of them before any change. Now the walk does not run, and the update fails during the apply. At a cap of 1<<20, where the walk never ran, the result was already the same as now. The 24 `ovf-` updates were added later; see "Running clocks past 2^64".

## Files

- `s<seed>-u<index>-cap<caps>.v2`: one update, as raw V2 bytes. `<seed>` is the generator seed (1, 2 or 3). `<index>` is the 0-based line of the update in that seed's output. `<caps>` lists the caps at which its result changed.
- `ovf-<kind>-<width>-<n>.v2`: one update whose running clock passes 2^64 (see "Running clocks past 2^64" below).
- `cases.tsv`: one row per case, giving the file, the cap (`WithMaxPendingItems`, set with `SetMaxPendingItems` after the preload) and the preload. The preload is the number of delete ranges parked beforehand: 0, or 2 ranges on clients 500 and 501 at clock 0. There are 5,195 cases: 5,147 from the `s` files and 48 from the `ovf-` files.

## How the set was made

1. **Generator.** `TestZZGenReviewCorpus`, source below. It ran in package `crdt` in a copy of #334 at `45e01bf5` (unpublished). The helpers it uses (`walkCorpus`, `walkedUpdate` and `mutate` in `struct_walk_test.go`, `groupsUpdate` in `pending_cursor_test.go`) are identical at the published #334 commit `6144baf7`. The command:

   ```sh
   for seed in 1 2 3; do
     ZZ_SEED=$seed ZZ_CORPUS=corpus$seed.txt go test ./crdt -run TestZZGenReviewCorpus -count=1
   done
   cat corpus1.txt corpus2.txt corpus3.txt > corpusB.txt   # 126,840 lines
   ```

   `walkCorpus` is not fully deterministic: the subdocument content draws a random GUID, and map order varies. So the same seed does not give the same bytes twice. The files here are the record.

2. **Replay.** Each of the 126,840 updates was applied to a new `Doc` at caps 1, 3, 8, 30 and 1<<20, with preload 0 and 2. Each apply recorded: error, `errors.Is(ErrInvalidUpdate)`, panic, parked ranges, pending items, and a SHA-256 of `EncodeStateAsUpdateV1`. The replay ran on two trees:
   - `rc`: the tree of `845ce79c`, which has #340 at `0b047644`.
   - `rcnew`: `rc` with #334's shared walk and rest-cursor skip.

   This directory holds every V2 update with a row that differs between them: 5,147 rows from 2,760 updates, all at caps 1-30. In every one, `rc` refused before any change and `rcnew` failed during the apply.

## The test

`crdt/malformed_v2_relay_test.go` runs each case on two documents:

1. A room applies the update at the case's cap, and relays what `provider/websocket` relays (`peer.applyToRoom`):
   - the update itself, when the apply succeeds;
   - the landed bytes, when the apply fails partway (`ApplyUpdateV1Landed` semantics, here for V2);
   - nothing, when nothing landed.
2. A peer with the same state applies the relay.

The test then requires `EncodeStateAsUpdateV1` to be byte-equal on room and peer, and `PendingStats` to be equal. As a control, it runs every case again at cap 1<<20 and counts the divergences there. It also requires the apply to refuse each `ovf-` case with `ErrInvalidUpdate`.

```sh
go test ./crdt -run TestMalformedV2_RelayedLandedBytesConvergeThePeer -v
```

## Running clocks past 2^64

A V2 delete set gives each range's clock as a diff from the end of the range before, and a group's skips add their lengths to the group's clock. Both are running sums. `ReadVarUint` refuses a value over 56 bits, so one value cannot take a clock past 2^64, but a sum of many values can wrap it onto a low clock. The `s` files never reach this: their largest update is 491 bytes, and a wrap needs at least 2,048 bytes of 8-byte varuints (128 delete ranges at up to 2^57 each, or 256 skips at up to 2^56).

The 24 `ovf-` updates, each with preload 0 and 2, are four kinds. Their cap is 1,048,576, so that the pending cap, which refuses a delete set of thousands of parked ranges at a small cap, does not refuse them before the clock bound can:

- `ds`: a delete set whose running clock reaches 2^64 and then names a clock below 5.
- `dsvalid`: the same after a range at clock 0.
- `structds`: client 30's "q" on root text "t", then such a delete set for client 30.
- `skips`: a group whose skips bring its clock back to where it started, then "Z" on root text "t".

`<width>` is `safe` for steps of at most 2^53-1, which Yjs reads exactly (about 2,700 per update), or `wide` for steps of at most 2^56-1, the most `ReadVarUint` reads (about 340).

The generator is `TestZZGenOverflowCorpus`, its source under "Overflow generator source" below. It needs only `newV2Encoder`, so it runs in package `crdt` on this branch, and the same seed gives the same bytes:

```sh
ZZ_OVF_DIR=<dir> go test ./crdt -run TestZZGenOverflowCorpus -count=1
```

In a probe on upstream main `510c2cf5`, a delete set of 2,049 ranges that wraps back to clock 1 deletes that clock in a document that holds it, and skips that bring a client back to clock 6 make "Z" integrate there. Yjs 13.6.32 refuses the first and parks "Z". A tree with the 2^53-1 clock bound refuses all 24 `ovf-` updates.

## Generator source

```go
func TestZZGenReviewCorpus(t *testing.T) {
	out := os.Getenv("ZZ_CORPUS")
	if out == "" {
		t.Skip()
	}
	seed, _ := strconv.Atoi(os.Getenv("ZZ_SEED"))
	rng := rand.New(rand.NewPCG(uint64(seed), 4242))
	updates, v2 := walkCorpus(t, rng)
	source := New()
	parent := ID{Client: 999}
	// Delete-heavy updates: few structs, many ranges on clients nobody holds.
	for range 60 {
		var groups [][]*Item
		for c := range rng.IntN(3) {
			key := "k"
			groups = append(groups, []*Item{{ID: ID{Client: ClientID(c + 1), Clock: uint64(rng.IntN(3))}, parentID: &parent, ParentSub: &key, Content: NewContentString("ab")}})
		}
		ds := newDeleteSet()
		for range rng.IntN(40) {
			ds.add(ID{Client: ClientID(1 + rng.IntN(8)), Clock: uint64(rng.IntN(400))}, 1+rng.IntN(3))
		}
		for _, version := range []int{1, 2} {
			updates = append(updates, groupsUpdate(source, groups, nil, ds, version))
			v2 = append(v2, version == 2)
		}
	}
	var b strings.Builder
	n := len(updates)
	for i := range n {
		fmt.Fprintf(&b, "%v %s\n", v2[i], hex.EncodeToString(updates[i]))
		for range 150 {
			m := updates[i]
			for range 1 + rng.IntN(3) {
				m = mutate(rng, m)
			}
			fmt.Fprintf(&b, "%v %s\n", v2[i], hex.EncodeToString(m))
		}
	}
	if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
```

## Overflow generator source

```go
func TestZZGenOverflowCorpus(t *testing.T) {
	dir := os.Getenv("ZZ_OVF_DIR")
	if dir == "" {
		t.Skip()
	}
	rng := rand.New(rand.NewPCG(1081, 64))
	// steps is values of at most most that sum to exactly 2^64.
	steps := func(most uint64) []uint64 {
		first := most/2 + rng.Uint64N(most/2+1)
		out, left := []uint64{first}, -first // left is 2^64 minus the sum
		for left > most {
			s := most/2 + rng.Uint64N(most/2+1)
			out, left = append(out, s), left-s
		}
		return append(out, left)
	}
	// ds writes one client's delete set whose running clock reaches 2^64, then
	// a range on a low clock; valid puts a range at clock 0 first.
	ds := func(e *v2Encoder, client, most uint64, valid bool) {
		var words []uint64
		if valid {
			words = append(words, 0, 0)
		}
		for _, s := range steps(most) {
			l := 1 + rng.Uint64N(min(s, 16))
			words = append(words, s-l, l-1)
		}
		words = append(words, rng.Uint64N(4), rng.Uint64N(3))
		e.restEnc.WriteVarUint(1)
		e.restEnc.WriteVarUint(client)
		e.restEnc.WriteVarUint(uint64(len(words) / 2))
		for _, w := range words {
			e.restEnc.WriteVarUint(w)
		}
	}
	str := func(e *v2Encoder, s string) {
		e.writeInfo(4)
		e.writeParentInfo(true)
		e.writeString("t")
		e.writeString(s)
	}
	kinds := []struct {
		name  string
		build func(most uint64) []byte
	}{
		{"ds", func(most uint64) []byte { // a delete set alone
			e := newV2Encoder()
			e.restEnc.WriteVarUint(0)
			ds(e, 1+rng.Uint64N(8), most, false)
			return e.toBytes()
		}},
		{"dsvalid", func(most uint64) []byte { // after a range that applies
			e := newV2Encoder()
			e.restEnc.WriteVarUint(0)
			ds(e, 1+rng.Uint64N(8), most, true)
			return e.toBytes()
		}},
		{"structds", func(most uint64) []byte { // client 30's "q", then its delete set
			e := newV2Encoder()
			e.restEnc.WriteVarUint(1)
			e.restEnc.WriteVarUint(1)
			e.writeClient(30)
			e.restEnc.WriteVarUint(0)
			str(e, "q")
			ds(e, 30, most, false)
			return e.toBytes()
		}},
		{"skips", func(most uint64) []byte { // skips back to the group's clock, then "Z"
			sk := steps(most)
			e := newV2Encoder()
			e.restEnc.WriteVarUint(1)
			e.restEnc.WriteVarUint(uint64(len(sk) + 1))
			e.writeClient(ClientID(1 + rng.Uint64N(8)))
			e.restEnc.WriteVarUint(rng.Uint64N(4))
			for _, s := range sk {
				e.writeInfo(10)
				e.restEnc.WriteVarUint(s)
			}
			str(e, "Z")
			e.restEnc.WriteVarUint(0)
			return e.toBytes()
		}},
	}
	var rows []string
	for _, most := range []struct {
		name  string
		value uint64
	}{{"safe", 1<<53 - 1}, {"wide", 1<<56 - 1}} { // the most Yjs reads exactly; the most ygo reads
		for i := range 3 {
			for _, k := range kinds {
				name := fmt.Sprintf("ovf-%s-%s-%d.v2", k.name, most.name, i)
				if err := os.WriteFile(filepath.Join(dir, name), k.build(most.value), 0o644); err != nil {
					t.Fatal(err)
				}
				for _, preload := range []int{0, 2} {
					rows = append(rows, fmt.Sprintf("%s\t1048576\t%d", name, preload))
				}
			}
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "ovf-cases.tsv"), []byte(strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}
```

The generator also writes `ovf-cases.tsv`, the 48 rows appended to `cases.tsv`.
