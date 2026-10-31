# Malformed V2 updates that now reach the apply at caps 1-30

These 2,760 V2 updates are the ones whose result at a small cap changes when #334's skip uses the V2 rest cursor (`pendingBudget.deleteSetLimit`, #334 commit `09d5fa53`). Before that skip, `checkParkedDeletes` walked to the delete set and refused each of them before any change. Now the walk does not run, and the update fails during the apply. At a cap of 1<<20, where the walk never ran, the result was already the same as now.

## Files

- `s<seed>-u<index>-cap<caps>.v2`: one update, as raw V2 bytes. `<seed>` is the generator seed (1, 2 or 3). `<index>` is the 0-based line of the update in that seed's output. `<caps>` lists the caps at which its result changed.
- `cases.tsv`: one row per case, giving the file, the cap (`WithMaxPendingItems`, set with `SetMaxPendingItems` after the preload) and the preload. The preload is the number of delete ranges parked beforehand: 0, or 2 ranges on clients 500 and 501 at clock 0. There are 5,147 cases.

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

The test then requires `EncodeStateAsUpdateV1` to be byte-equal on room and peer, and `PendingStats` to be equal. As a control, it runs every case again at cap 1<<20 and counts the divergences there.

```sh
go test ./crdt -run TestMalformedV2_RelayedLandedBytesConvergeThePeer -v
```

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
