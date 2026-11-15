# Preserved test-suite replay

This harness runs the preserved test sources against the current runtime in a
new detached worktree. It keeps the release's normal suite separate so that
replaying the preserved suite never removes release tests from the working
branch or substitutes runtime implementations.

The frozen test source is commit `aa31c1532f604f67c065b0de0544e69e5eb8a2d0`.
`adaptation.patch` contains the complete, reviewable test changes. The runner
accounts for every original test/example/fuzz function, including explicit
name mappings for semantic replacements, and verifies that every runtime Go
source file remains byte-identical to the selected runtime commit. No legacy
`OnPersistUpdate` API shim is installed.

## Run

The test-source commit must exist in the clone or in `--legacy-repo`. If needed:

```sh
git fetch https://github.com/RichieB2B/ygo.git aa31c1532f604f67c065b0de0544e69e5eb8a2d0
```

Choose a work directory that does not exist. Both root and mobile suites run
with race detection, uncached results and mandatory Node/Yjs checks:

```sh
GOTOOLCHAIN=go1.26.11+auto python3 testutil/compat_suite/run.py \
  --work-dir /tmp/ygo-compat-replay
```

`npm ci` installs the frozen suite's JavaScript dependencies. To use an already
installed matching directory, pass `--node-modules /path/to/testutil/node_modules`.
`--prepare-only` validates and prepares the harness without installing packages
or running tests. The worktree contains `compat-inventory.json`, per-module JSON
test logs and `compat-results.json` for inspection. Remove it when finished with
`git worktree remove --force /tmp/ygo-compat-replay`; the runner never removes it.

## Contract adaptations

- Private encoders no longer accept a struct store, and delete-set decoders now
  also return range counts. Only test call sites change.
- Pending-cap errors are checked with both `ErrPendingCap` and its inherited
  `ErrInvalidUpdate` sentinel. Truncation/decoder reason checks remain intact.
- Subdocument wire options retain GUID and loading semantics. Collection IDs
  stay local and are inherited on attach; the added inheritance case checks
  both local attachment and remote decoding.
- Allocation guards use the release's exact 31-word transaction and 13-word
  store layout. Lazy parking-state checks remain in place.
- WebSocket helpers send SyncStep1 before waiting for SyncStep2. The explicit
  handshake test checks the response and rejects unsolicited state or empty
  awareness frames. Slow-peer disconnect/resync checks retain their assertions.
- Tests that directly acquire an internal room release its in-flight lease
  before requesting eviction. The eviction/version assertions remain intact.
- The persistence lifecycle scenario uses `OpenUpdateLog` directly, retaining
  parked-only, origin, duplicate, replay, dependency-arrival and close checks.
- Tests of the removed parent resolver become V1/V2 merge graph tests. They
  retain bounded-work assertions for 2,000 interleaved structs and 9,001 cyclic
  structs, check lossless wire output, and check integration/parent resolution
  for the rooted chain. None is dropped or skipped.

Existing platform-dependent and opt-in skips are retained; the harness does
not add skip conditions or remove assertions to force a passing result.
