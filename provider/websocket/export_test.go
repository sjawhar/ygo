package websocket

import (
	"context"

	"github.com/reearth/ygo/persistence"
)

// MemoryPersistenceRecordCount reports how many stored records a room holds.
// Test-only: the record count is the only way to tell a real compaction from
// a KeepVersions=0 no-op, which preserves content either way (#186).
func MemoryPersistenceRecordCount(m *MemoryPersistence, room string) int {
	vs, err := m.adapter.Store().ListVersions(context.Background(), room)
	if err != nil {
		return -1
	}
	return len(vs)
}

// StrandedWritesInFlight reports how many goroutines are registered in the
// stranded-write path — the #229 counter Shutdown joins on: committing
// goroutines inside the persistence observer, and every room's running feeder.
// Test-only, and the only way to SEQUENCE a shutdown-join test
// deterministically: a test must know the committer has registered before it
// lets the worker exit, otherwise it is asserting against the acknowledged
// residual (a commit that starts after Shutdown has read the counter) rather
// than against the join. It tells a committer apart only while no feeder runs,
// that is in a test that has not committed past the room's buffer.
func StrandedWritesInFlight(s *Server) int64 { return s.stranded.n.Load() }

// PersistenceQueueLen reports how many committed updates room holds queued
// beyond its persistence buffer (persistOverflow), or 0 for a room the server
// does not hold.
func PersistenceQueueLen(s *Server, room string) int {
	rm := residentRoom(s, room)
	if rm == nil {
		return 0
	}
	rm.persistMu.Lock()
	defer rm.persistMu.Unlock()
	return len(rm.persistOverflow)
}

// PersistenceWait begins the wait a peer's read loop makes before it applies
// its next update (awaitPersistence) and returns the function that waits.
// Test-only: the split lets a test commit between the two, which is how it
// tells a wait that covers what was queued when it began from one that also
// covers what is queued while it waits.
func PersistenceWait(s *Server, room string) (wait func()) {
	rm := residentRoom(s, room)
	if rm == nil {
		return func() {}
	}
	released := rm.handedOnWait(rm.persistRetire)
	return func() {
		if released == nil {
			return
		}
		select {
		case <-released:
		case <-rm.persistRetire:
		}
	}
}

// residentRoom returns the room s holds under name, or nil.
func residentRoom(s *Server, name string) *room {
	s.rmu.RLock()
	defer s.rmu.RUnlock()
	return s.rooms[name]
}

// MemoryPersistencePendingRooms reports how many rooms hold un-compacted
// bookkeeping. Test-only: proves the pending map is deleted, not zeroed.
func MemoryPersistencePendingRooms(m *MemoryPersistence) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}

// MemoryPersistencePendingCount reports room's outstanding (un-folded) write
// count, 0 if the room has no entry. Test-only: distinguishes "no un-folded
// writes" from "the bookkeeping entry was erased while a write it did not
// fold was in flight" (PR #230 review, server.go:435) — MemoryPersistencePendingRooms
// only reports how many rooms have an entry, not what each one holds.
func MemoryPersistencePendingCount(m *MemoryPersistence, room string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.rooms[room]
	if l == nil {
		return 0
	}
	return int(l.outstanding())
}

// NewMemoryPersistenceForTest builds a MemoryPersistence around an arbitrary
// *persistence.LegacyAdapter, bypassing NewMemoryPersistence's fixed
// persistence.NewMemoryPersistence() store. Test-only: lets a test park
// inside the wrapped store's Compact via a custom persistence.VersionedPersistence,
// making the window between MemoryPersistence.Compact's pending snapshot and
// the fold completing deterministic to hit, without sleeps or timing luck.
func NewMemoryPersistenceForTest(adapter *persistence.LegacyAdapter) *MemoryPersistence {
	return &MemoryPersistence{adapter: adapter, rooms: make(map[string]*compactLedger)}
}

// MemoryPersistenceBackoffState reports room's consecutive-failure count and
// its next-attempt mark, or (0, 0) when the room has no ledger entry.
//
// Test-only, and the ONLY way to observe the reset-on-success: when a fold
// succeeds and leaves nothing outstanding, dropIfIdleLocked deletes the entry
// outright, which clears the same state incidentally. A test that asserts on
// cadence alone therefore passes with the reset deleted (verified by
// mutation) — the reset only does work when the ledger SURVIVES a successful
// fold, which needs a write landing inside the fold window.
func MemoryPersistenceBackoffState(m *MemoryPersistence, room string) (failures int, retryAt int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.rooms[room]
	if l == nil {
		return 0, 0
	}
	return l.failures, l.retryAt
}

// DefaultPeerWriteTimeout is the deadline writeToConn sets on each write
// when Server.PeerWriteTimeout is unset. Test-only: a test that bounds a
// teardown bounds it against the number the server uses, not a copy of it.
const DefaultPeerWriteTimeout = defaultPeerWriteTimeout

// DefaultCloseFrameTimeout is the deadline on the close frame sent for a
// cancel cause when Server.CloseFrameTimeout is unset. Test-only, for the
// same reason as DefaultPeerWriteTimeout.
const DefaultCloseFrameTimeout = defaultCloseFrameTimeout

// StateFrameBytes is the cap on each frame a peer's SyncStep1 is answered
// with. Test-only: a test asserting the cap must assert against the one the
// server uses, not a copy of the number.
const StateFrameBytes = stateFrameBytes

// DefaultPeerWriteQueueSize is the per-peer write queue's capacity when
// PeerWriteQueueSize is unset. Test-only: the frames of one state answer have
// to fit in it, and that is arithmetic on this number.
const DefaultPeerWriteQueueSize = defaultPeerWriteQueueSize

// PeerQueuedBytes reports the bytes charged to room's peers and its peer
// count, or (0, 0) for a room the server does not hold.
func PeerQueuedBytes(s *Server, room string) (bytes int64, peers int) {
	s.rmu.RLock()
	rm, ok := s.rooms[room]
	s.rmu.RUnlock()
	if !ok {
		return 0, 0
	}
	rm.mu.Lock()
	targets := make([]*peer, 0, len(rm.peers))
	for p := range rm.peers {
		targets = append(targets, p)
	}
	rm.mu.Unlock()
	for _, p := range targets {
		p.wmu.Lock()
		bytes += p.queuedBytes
		p.wmu.Unlock()
	}
	return bytes, len(targets)
}
