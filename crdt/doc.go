// Package crdt implements the Yjs CRDT algorithm in pure Go.
//
// The central concept is the Item: a node in a per-type doubly-linked list
// that carries content and origin pointers enabling conflict-free merging (YATA).
//
// Start with Doc, which is the root of a collaborative document. Inside a
// transaction, resolve root types through the Transaction (the Doc-level
// accessors take the document lock Transact already holds and would
// deadlock; see issue #138):
//
//	doc := crdt.New()
//	doc.Transact(func(txn *crdt.Transaction) {
//	    txn.GetText("content").Insert(txn, 0, "Hello", nil)
//	})
//	update := doc.EncodeStateAsUpdate()
//
// Reference algorithm: https://github.com/yjs/yjs/blob/main/INTERNALS.md
//
// # Quick start
//
//	doc := crdt.New()
//	text := doc.GetText("body")
//	doc.Transact(func(txn *crdt.Transaction) {
//	    text.Insert(txn, 0, "hello", nil)
//	})
//	update := crdt.EncodeStateAsUpdateV1(doc, nil)
//
// See the Example* functions for canonical usage patterns.
//
// # Stability
//
// ygo follows semantic versioning. The v1.x public API is considered
// stable: new functionality lands as minor releases; bug fixes as patch
// releases; breaking changes are deferred to v2.
//
// # Origin Tags
//
// The Origin field on Transaction, ChangeEvent (awareness), and
// UndoManager.trackedOrigins uses type any for user-defined tags.
// Callers must type-assert on read. Origin is used for filtering
// in observers and undo-manager scope.
package crdt

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
)

// ErrSubdocAlreadyIntegrated is raised when a Doc already embedded as a
// subdocument is embedded again. Create a second Doc with the same guid instead.
var ErrSubdocAlreadyIntegrated = errors.New("ygo/crdt: subdocument already integrated; create a second instance with the same guid")

// ErrRootTypeMismatch is raised when a named root is accessed as a different
// shared type than the one already stored in the document.
var ErrRootTypeMismatch = errors.New("crdt: root type mismatch")

// DocOption configures a Doc at creation time.
type DocOption func(*Doc)

// WithClientID sets a fixed ClientID instead of generating a random one.
// Useful in tests and server-side scenarios where the ID must be deterministic.
// The document still takes a new ID when a remote update moves this client's
// clock (see ClientID).
// id must be at most 2^55-1, the largest client a V2 update can carry; a larger
// one panics here, since the document could not encode its own writes in V2
// and every decoder refuses them. Yjs client IDs fit in 53 bits.
func WithClientID(id ClientID) DocOption {
	if id > maxClient {
		// A programmer error: random and remote client IDs are always within it.
		panic(fmt.Sprintf("crdt: WithClientID(%d): a client ID must be at most 2^55-1", id))
	}
	return func(d *Doc) { d.setClientID(id) }
}

// WithGC controls whether deleted item content is freed at the end of each
// transaction. Default is true. Set to false to preserve history for snapshots.
func WithGC(gc bool) DocOption {
	return func(d *Doc) { d.gc = gc }
}

// WithGUID sets the document's subdocument identifier. When a Doc is embedded
// inside another Doc via ContentDoc, the GUID identifies it across peers.
func WithGUID(guid string) DocOption {
	return func(d *Doc) {
		checkUTF8("WithGUID", "guid", guid)
		d.guid = guid
	}
}

// WithAutoLoad marks a subdocument to be auto-loaded by remote peers. Default false.
func WithAutoLoad(v bool) DocOption { return func(d *Doc) { d.autoLoad = v } }

// WithShouldLoad controls whether a provider should sync this doc now. Default
// true; a decoded subdocument starts with its autoLoad value, as in Yjs.
func WithShouldLoad(v bool) DocOption { return func(d *Doc) { d.shouldLoad = v } }

// WithCollectionID sets an optional subdocument collection identifier.
func WithCollectionID(id string) DocOption {
	return func(d *Doc) {
		checkUTF8("WithCollectionID", "id", id)
		d.collectionID = id
	}
}

// defaultMaxPendingItems is the default cap on parked items and, separately,
// parked delete ranges (see WithMaxPendingItems).
const defaultMaxPendingItems = 100_000

// WithMaxPendingItems caps parked items and, separately, parked delete ranges
// (default 100,000 when n <= 0), refusing with ErrInvalidUpdate, before any
// change, an update that would park past either, so a peer cannot grow them
// without bound. A struct sent again merges into the queue as Yjs merges its
// pending update, and the cap counts what the park adds after the merge.
func WithMaxPendingItems(n int) DocOption {
	return func(d *Doc) { d.maxPendingItems = n }
}

// SetMaxPendingItems sets the WithMaxPendingItems cap after construction, so
// stored state that exceeds it can load uncapped first.
func (d *Doc) SetMaxPendingItems(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.maxPendingItems = n
}

// WithMaxIntegrateScan bounds the items the conflict scans of one remote
// update (ApplyUpdate and its V1 and V2 forms) may pass, all of its items
// together, and makes the document apply a remote update whole or not at all
// (apply_rollback.go). Integrating an item scans the items between its left
// origin and its right origin (Item.integrate). An item an editor sends scans
// only what other peers inserted concurrently at the same place, so a remote
// update's scans pass a handful of items; an item that names a left origin
// early in a list and no right origin scans to the list's end instead, and an
// update of n such items scans n times the list - 2,000 of them, about 12 KB,
// took 20.8 s against a list of 20,000 unmerged items. An update whose scans
// would pass the bound is refused with ErrIntegrateScanBudget, and like every
// remote update the bounded document refuses, it leaves the document as it was,
// fires no observer, and is returned as a RefusedUpdateError reporting what its
// scans passed. The drain of the pending queue an update's apply ends with has
// an allowance of its own, the bound again; a drain that would pass it is taken
// back and the queue parked again as it was, while the update stands, and the
// queue is drained again once an apply brings a clock it waits on further
// (apply_rollback.go). Such a document refuses a remote update carrying a
// subdocument or a move. Zero or negative, the default, sets no bound. Local
// transactions are never bounded: their inserts name both neighbours. A budget
// (WithIntegrateScanBudget) takes the bound's place while it is set.
func WithMaxIntegrateScan(n int) DocOption {
	return func(d *Doc) { d.maxIntegrateScan = n }
}

// SetMaxIntegrateScan sets the bound of WithMaxIntegrateScan on a document
// already made, for an owner that loads trusted state first and bounds what
// arrives after it. It takes the document lock.
func (d *Doc) SetMaxIntegrateScan(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.maxIntegrateScan = n
}

// WithIntegrateScanBudget bounds the conflict scans of each remote update by an
// allowance of its own: as each remote apply (ApplyUpdate and its V1 and V2
// forms) begins, the document calls budget with the apply's origin, and the
// items that apply's scans pass, the drain of the pending queue it ends with
// included, may number at most what budget returns; zero or negative leaves
// that one apply's scans unbounded. One fixed bound has to hold the largest
// honest update - a peer handing over a whole state edited offline scans what
// others wrote meanwhile at every place it wrote too - so it lets a small
// update scan as much; a budget lets the owner size each update's allowance by
// what it can see of the update before applying it, such as the structs it
// carries, keyed by the origin it applies it with. An update whose own scans
// would pass its allowance is refused, with ErrIntegrateScanBudget, as under
// WithMaxIntegrateScan; a drain whose scans would pass what the update left of
// it is taken back and the queue parked again as it was, while the update
// stands, and the queue is drained again once an apply brings a clock it waits
// on further (apply_rollback.go).
//
// An owner keeping an account of scans learns what each apply cost from
// Transaction.IntegrateScanned in an OnAfterTransaction observer when the
// apply commits, and from RefusedUpdateError.Scanned when the document
// refused it; either is at most what budget returned for it, when positive.
//
// A document with a budget is a bounded document whatever budget returns: it
// applies every remote update whole or not at all, and refuses a remote update
// carrying a subdocument or a move. While a budget is set, the bound of
// WithMaxIntegrateScan is not used. budget runs under the document's lock, so
// it must not call the document. Local transactions are never bounded.
func WithIntegrateScanBudget(budget func(origin any) int) DocOption {
	return func(d *Doc) { d.integrateScanBudget = budget }
}

// SetIntegrateScanBudget sets the budget of WithIntegrateScanBudget on a
// document already made, or with nil removes it. It takes the document lock.
func (d *Doc) SetIntegrateScanBudget(budget func(origin any) int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.integrateScanBudget = budget
}

// ApplyBudget bounds one remote apply. A field below zero is no limit; zero
// allows nothing in that measure, so the zero value of the struct admits an
// apply that decodes nothing, integrates no struct of its own, takes no byte
// of content and splits nothing. A caller that wants a measure unbounded
// sets it to -1.
//
// Zero is "nothing" rather than "no limit" because the two strictest rules
// are the useful ones: Structs: 0 is "this update may integrate no item of
// its own", which still admits an update that only deletes text the document
// holds (its splits add structs that Structs does not count, see Split), and
// Bytes: 0 is "it may integrate no content byte". Neither can be said if zero
// means no limit. A document with no budget callback at all is unbounded in
// every measure, which is what a document without the option does
// (WithApplyBudget).
//
// The three bound different costs, and a document that bounds what it takes
// from a peer needs all three.
//
// Decoded bounds the apply's memory, which the two counts below cannot: a
// frame of lib0 Any values integrates nothing when it names clocks the
// document already holds, and still allocates about 32 bytes for each byte of
// the frame while it decodes (a null is one byte on the wire and an interface
// word and a slot in a []any once decoded), so a megabyte of frame reaches
// tens of megabytes of heap before a count of what it integrated could refuse
// it. It also bounds what an update can make the document do per byte: each
// delete range it carries can split a struct the document holds, and a split
// adds a struct no Structs count bounds (see Split).
//
// Structs keeps the document decodable. Its whole state is one update to its
// next load, and every struct of it decodes and integrates then, so an owner
// that lets a peer add a million one-character items has a document whose load
// no longer fits the time or the memory it has for one.
//
// Bytes is the creep rule: a 64 KiB paste is one struct of one byte of wire
// overhead per character, which no count of structs bounds.
//
// Split is what bounds the structs Structs excludes.
type ApplyBudget struct {
	// Decoded is the values the apply may decode: each struct, each lib0 Any
	// value at any depth, each Any map entry, each JSON member.
	Decoded int
	// Structs is the items the apply may integrate from the update, splits
	// of items the document already held excluded. A struct of the update
	// that parks behind a clock the document does not hold counts here too,
	// where it parks (WithApplyBudget).
	Structs int
	// Bytes is the bytes of content the apply may integrate, counted for a
	// struct that parks as for one that integrates. Besides the content
	// itself (contentBytes) it counts the map key an item carries, which the
	// document keeps for as long as the entry lives. A deleted item is not
	// counted: it carries no content, and what the document keeps for it is a
	// range record of two integers whatever span it names, so its span is no
	// byte cost. The number of delete ranges an update carries is bounded by
	// Decoded instead.
	Bytes int
	// Split is the structs the apply may make by splitting items the
	// document already held, which Structs excludes and which an update
	// carrying no struct of its own can still make: this is what bounds
	// them. A document whose structs are near the count its next load can
	// decode needs it, because a single delete grows that count without
	// integrating anything. Measured on the one-run text of
	// TestUnit_ApplyBudget_RefusesADeleteWhoseSplitsPassWhatItAllows, a
	// delete of a range inside the run cuts that one item into three: two
	// structs the document keeps until the items between the halves are
	// gone, from an update whose Structs count is 0. A room already at the
	// count its load can decode takes one such delete and can no longer be
	// loaded, which reads as "crdt: invalid update" on the next load and
	// not as a refusal of the update that caused it.
	//
	// It is read where the split is made and not where Structs and Bytes
	// are read (countSplit), because most splits are made after that point:
	// the delete set runs after it, and the splits of a delete-only update
	// are all its own.
	//
	// Split and Structs count the drain of the pending queue differently,
	// and a caller sizing a budget needs the difference. A struct the drain
	// integrates is outside Structs: it is one an earlier update parked,
	// charged in full - struct and bytes - to the apply that brought it, at
	// the moment it parked, so charging it again to whichever update later
	// fills the gap would charge one edit twice and let one sender have
	// every update that fills its gap refused. A split the drain makes is
	// inside Split: it belongs to no earlier update, since it is a half of
	// an item the document already held, cut at the moment the parked
	// struct lands, and nothing charged it at park. So Split is what an
	// apply may add by cutting, its drain included, and Structs is what it
	// may add by carrying. Together they bound what one apply adds to the
	// document; without the drain's splits in Split, neither does.
	//
	// Passing Split in the drain does not refuse the update. The drain is
	// taken back, the parked structs stay parked whole, and the apply that
	// filled the gap commits with what it carried itself: the content whose
	// splits passed the budget is another sender's, already in the room's
	// queue, and the sender that filled the gap can neither see it nor
	// send anything smaller. The budget still holds - the document gains
	// nothing from that drain - and what an owner watches for is a pending
	// queue that stops falling (Doc.PendingStats), which it can answer in
	// its own terms. Passing Split in the update's own structs or delete
	// set refuses the apply, as the other three measures do.
	Split int
}

// WithApplyBudget bounds what each remote update may decode and add by an
// allowance of its own: as each remote apply (ApplyUpdate and its V1 and V2
// forms) begins, the document calls budget with the apply's origin, and the
// apply may decode at most ApplyBudget.Decoded values, integrate at most
// ApplyBudget.Structs structs of its own, take at most ApplyBudget.Bytes
// bytes of content and make at most ApplyBudget.Split structs by splitting
// items the document already held; a field below zero leaves that one apply
// unbounded in that measure. The owner sizes each update's allowance by what
// it can see of the update before applying it - its length, the peer it came
// from, what the document holds already (StructCount) - keyed by the origin
// it applies it with, as the scan budget is (WithIntegrateScanBudget).
//
// An update that passes its allowance is refused with ErrApplyBudget, and like
// every remote update a bounded document refuses it leaves the document as it
// was, fires no observer, and is returned as a RefusedUpdateError reporting
// what the refused apply reached: Decoded, Structs, Bytes and Split. The
// decoded values are counted as the apply decodes them, so a frame of them is
// refused before it is all in memory; the structs and the bytes are counted as
// they integrate and checked once the apply has integrated, which the decode
// count has already bounded the heap for; a split is counted and checked where
// the apply makes it, since the delete set makes its splits after the structs
// and the bytes have been read (ApplyBudget.Split).
//
// A struct of the update that parks - one waiting on a clock the document
// does not hold yet - counts against Structs, and its content against Bytes,
// at the moment it parks: the update that brings content is charged for it
// whether that content integrates now or waits for a gap its sender will
// fill, and a parked struct costs the document the same memory either way.
// The apply that later fills the gap is charged nothing for what an earlier
// update parked, so no sender can have every update that fills its gap
// refused (noteIntegrated).
//
// Splits are excluded from Structs because a split costs the document nothing
// lasting - it leaves the same content, and the commit merges the halves back
// once the items between them are gone (tryMergeWithLefts) - and because the
// caller needs "did this update integrate anything of its own" to be
// answerable exactly: a budget of Structs: 1 must refuse an update that
// integrates two characters whatever the deletes beside them split, and a
// budget of Structs: 0 must take an update that only deletes. What the
// splits of such an update cost is Split's to bound.
//
// A document with an apply budget is a bounded document whatever budget
// returns: it applies every remote update whole or not at all, and refuses a
// remote update carrying a subdocument or a move. budget runs under the
// document's lock, so it must not call the document. Local transactions are
// never bounded.
func WithApplyBudget(budget func(origin any) ApplyBudget) DocOption {
	return func(d *Doc) { d.applyBudget = budget }
}

// SetApplyBudget sets the budget of WithApplyBudget on a document already
// made, or with nil removes it, for an owner that loads trusted state first
// and bounds what arrives after it. It takes the document lock.
func (d *Doc) SetApplyBudget(budget func(origin any) ApplyBudget) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applyBudget = budget
}

// ApplyReport is what one remote apply that committed left the document at:
// the totals the document stands at once the apply has committed, and the
// apply's own deltas: the four counts ApplyBudget bounds, and the bytes the
// apply added to the document's encoded state (WithApplyReport).
type ApplyReport struct {
	// Structs is the structs the document holds now, those parked in the
	// pending queue included: a total of what is there, not a sum of what
	// the applies before this one added. Doc.StructCount counts the store
	// alone, and a parked struct is held in memory, returned in the
	// document's encoded state and integrated as soon as its gap is
	// filled, so an owner bounding what a room costs needs it counted
	// (Doc.PendingStats).
	Structs int
	// Integrated is the structs this apply added of its own, the count
	// ApplyBudget.Structs bounds: a struct of the update that parked is
	// counted here, where it parked, and the drain that later lands it
	// adds nothing (noteIntegrated).
	Integrated int
	// Bytes is the content bytes this apply added, the count
	// ApplyBudget.Bytes bounds: the content itself and the map key an item
	// carries, for a struct that parked as for one that integrated, and
	// nothing for a deleted span (applyTally.integrated).
	Bytes int
	// Split is the structs this apply made by splitting items the document
	// already held, the count ApplyBudget.Split bounds, the splits of its
	// drain of the pending queue included (countSplit).
	//
	// A drain whose splits passed ApplyBudget.Split is taken back while
	// the apply stands, and its splits stay counted here though the
	// document kept none of them: the take-back gives back the cuts, not
	// the count, which is what the budget reads to refuse the next one
	// (drainParked). Structs is of the document either way, so an owner
	// watching what the room holds reads that and a queue that stops
	// falling (Doc.PendingStats).
	Split int
	// Decoded is the values this apply decoded, the count
	// ApplyBudget.Decoded bounds.
	Decoded int
	// StateBytes is the bytes this apply's transaction added to the
	// document's encoded state, as EncodeStateAsUpdateV1(doc, nil) measures
	// it, so an owner that encodes a room once and from then on adds each
	// apply's StateBytes holds the state's length, or more, without encoding
	// again (the contract is below).
	// Bytes is not that number: it is the content the budget charges, and a
	// GC struct or a split half adds state with no content. Nor are the bytes
	// an update logs, which fell short of the state where an owner summed
	// them: 24 frames of 900 KiB, each parked behind a gap of its own client,
	// fired no update and logged nothing while the encoded state reached
	// 22,119,318 bytes, and a 100,000-character run with every other
	// character deleted logged 291,366 bytes against an encoded state of
	// 1,019,797, each delete's splits adding item headers no update carries.
	// Measured on rooms of those shapes, the 24 frames report 22,118,664 and
	// the deletes 725,230, each the encode's growth to the byte.
	//
	// It is a signed delta, below zero for an apply that leaves the state
	// smaller: a delete's content is written as its length once the commit
	// collects it, the commit merges split halves back, and a struct the
	// drain drops leaves the queue. A total that could only grow would hold
	// every deleted byte against the room for good, and refuse one that
	// deletes as much as it writes long before its state reached the cap.
	//
	// It is counted where the apply makes each change, not by encoding the
	// document, which costs 53.0 ms on a 494,309-struct room: each struct
	// at its encoded length where it integrates or parks, with the skip and
	// section header it adds; each split at the header and length prefix the
	// new half adds; the delete set at the ranges it gains; the commit's
	// merges and collection at what they take out; and a drain that lands
	// parked structs at what landing changes, what they held parked having
	// been counted where they parked (state_bytes.go). Measured on one
	// machine, a keystroke on that room applies in 4.3 µs with a report against
	// 2.2 µs without, and in 4.0 µs with 100,000 structs parked: the first
	// apply to find a queue reads what it holds once, 2.6 ms for those
	// 100,000, and keeps that reading up as parks change the queue.
	//
	// The contract is cumulative, not per apply. EncodeStateAsUpdateV1
	// writes the state in one of two forms: the direct encode while nothing
	// is pending - no struct parked and no delete parked (Doc.PendingStats)
	// - and otherwise a merge of it with the parked parts (withParked), which
	// writes two classes of value at other lengths. An integer outside int32
	// that a float32 holds exactly, such as 2^40, in a ContentAny value or a
	// subdocument's meta, is a float64 of nine bytes in the direct form and
	// a float32 of five merged. An integer in an embed or format value that
	// a float64 rounds up to the next power of ten, such as
	// 9999999999999999, is its digits in the direct form and a digit longer
	// merged, where V1's JSON text is read back as a float64, as every V1
	// decode and Yjs's JSON.parse read it; so only a document fed V2, which
	// carries the integer itself, or written locally holds one, and none can
	// whose writers send no embed and no format value other than a string, a
	// boolean or null. The report counts each value at its longer form from
	// the apply that brings it to the one that removes it. So a total that
	// starts from an encode of a room holding neither class and adds each
	// report's StateBytes is never below the state in either form: it runs
	// four bytes above a merged state for each wide integer held, and a byte
	// above a direct one for each rounding integer, at most 588 bytes above
	// across 2,400 applies of parks, fills, overwrites, embeds and deletes
	// (TestUnit_ApplyReport_StateBytesTotalStaysAtOrAboveTheStateInBothForms).
	// No one apply's report equals its own growth in both forms, and it need
	// not: a park counts the values it parks at nine bytes, which the state
	// writes in five, and the fill that empties the queue counts nothing for
	// the four a value the state then regains. A base encode of a room that
	// already holds such values is short of the total's form by four bytes a
	// wide integer if taken while anything is pending, and a byte a rounding
	// integer if taken while nothing is; a room loaded through V1 holds no
	// rounding integer, so its base taken while nothing is pending is exact.
	//
	// Where the report cannot know a part of the state it takes the most the
	// part can add, never less, which puts the total above the state by a
	// bounded figure in three shapes:
	//   - a delete range added to a client holding more than 254 structs, whose
	//     ranges the document does not count: a byte over where the varuint
	//     counting them does not grow, as at the 1,001st range, which reports
	//     14 against 13 (two where more than 127 ranges join a client of more
	//     than 32,766 structs), and a byte where a client's first range joins
	//     a delete set that could hold more than 127 clients;
	//   - a struct appended, split or merged at clocks a parked struct also
	//     holds, as where an update re-sends what the room holds parked:
	//     2 x (the varuint of its client + the varuint of the clock it ends
	//     at) a struct, 16 bytes for client 2^40 below clock 16,384, and the
	//     parked copy's length until it leaves the queue. Across 12,000
	//     applies of the re-sent states of
	//     TestUnit_ApplyReport_StateBytesNeverFallsShortOfRedeliveredState,
	//     run at 300 seeds, the most was one apply 730 bytes over its growth;
	//   - a section of a client the queue holds structs of where what it
	//     holds is not known exactly (after a refused apply took parks back
	//     out, or in a drain that dropped a struct of the client): the skip,
	//     start clock and section header a park can add, and a byte where a
	//     varuint boundary falls within what the section's count can be.
	// state_bytes.go names each where it is counted.
	//
	// It covers its own apply's transaction alone. On a document whose text
	// root has been read with GetText - a raw root never runs it - a remote
	// apply that leaves format markers redundant is followed by the format
	// cleanup (WithFormatCleanup), a local transaction that commits after
	// the report and emits its own update, and no report covers it. Measured
	// with GC off: A deletes "abc" while B bolds its last character, and the
	// apply that brings A's delete to a room holding B's format reports 9
	// while the state grows by 18, the cleanup's delete of the redundant
	// marker adding a delete-set entry of 9 bytes for B's client of
	// 2^40 + 1. With GC on the same case reports 6 and the state grows by 6.
	// An owner bounding state counts those commits too.
	StateBytes int
}

// WithApplyReport reports what each remote update that commits left the
// document at: as the apply commits, under the lock and inside the
// transaction that applied it, the document calls report with the apply's
// origin and an ApplyReport of what the document now holds and what that
// apply added.
//
// It is what makes the library, and not its embedder, the authority on what
// a document holds. An owner that sizes each update's allowance
// (WithApplyBudget) from what it can read before the apply reads a number
// that is both old and short: Doc.StructCount leaves out the structs parked
// in the pending queue, and an owner keeping running totals of its own
// instead has to re-derive, from outside, every quantity an apply moves -
// what parked, what a drain landed, what a delete split, what the commit
// merged back. Each of those is known here, where the apply ends.
//
// Structs is a total and Bytes and StateBytes deltas, and what separates
// them is what each costs to produce. The struct total is two counters, the
// store's structs and the pending queue's, which cost 13.4 µs to read
// together on the rooms measured below: payable on every apply. There is no
// counter of the bytes a document holds, and the only answer is a
// whole-state encode, measured on one machine at 2.96 ms for a mebibyte of
// prose held in 10 structs, 53.0 ms for a 7,792,586-byte state of 494,309
// structs and 52.8 ms at 600,000 structs, against the 128 ms a browser's
// keystroke is answered in at the median. An encode per apply would spend
// two fifths of that answer on the measurement alone, so the byte counts are
// this apply's own: Bytes taken off the count the budget already keeps as
// the apply integrates, and StateBytes counted as the apply changes each
// part of the encoded state, which an owner that wants the state's length
// adds to one encode of its own and need not encode again.
//
// The four other deltas are exactly the counts of ApplyBudget, so this
// report and RefusedUpdateError (endRemoteApply) carry the same four numbers
// by two routes - for a refusal that reached the same point in the apply. A
// refusal earlier than that stopped counting earlier and carries what it
// had counted by then, not what the whole update would have come to:
// measured, an apply reporting Decoded 3 against the same update refused at
// Decoded 1 by an allowance of one. An apply the document rolled back or
// refused reports nothing at all - the document is as it was, its encoded
// state included, and its tally is in the error.
//
// The totals are read once the commit's merges have run (squashRuns,
// tryMergeWithLefts), so Structs is the count the next reader of the
// document finds and not the structs the apply linked: an update that
// integrates one character beside the same client's last one leaves
// Integrated at 1 and the total where it was.
//
// A document that reports is a bounded document, as one with an apply
// budget is: it applies every remote update whole or not at all, and
// refuses a remote update carrying a subdocument or a move. The counts are
// the journal's, and the journal is what an apply can be taken back to, so
// a document that only watches is bounded like one that refuses. report
// runs under the document's lock, so it must not call the document; a panic
// it raises refuses nothing, the apply having committed, and reaches the
// caller of the apply once the transaction has committed, as a panic from
// anything else under Transact does.
func WithApplyReport(report func(origin any, report ApplyReport)) DocOption {
	return func(d *Doc) { d.applyReport = report }
}

// SetApplyReport sets the callback of WithApplyReport on a document already
// made, or with nil removes it. It takes the document lock.
func (d *Doc) SetApplyReport(report func(origin any, report ApplyReport)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applyReport = report
}

// WithAllowedRoots names the root types a remote update may write under on
// this document: as each remote apply begins, the document calls roots with
// the apply's origin, and an update that names, integrates under or deletes
// under any root outside the answer is refused with ErrRootNotAllowed, rolled
// back whole as every refusal of a bounded document is, and returned as a
// RefusedUpdateError whose Root is the root's name. A nil callback, or one
// answering a nil or empty slice, allows every root, which is what a document
// without the option does.
//
// The list is a containment rule over the whole update, not a rule about the
// names it writes on the wire. An update names a root only where its decode
// resolves one by name - the first item of a root, or a map key whose slot is
// empty - and an item written beside items the document already holds names
// no root at all, since it inherits its parent from the item its origin
// names, nor does a delete range. So the apply also walks up the parents of
// every item it integrates or deletes, and refuses the update when that walk
// reaches a root the list does not name: without it a sender could overwrite
// and delete inside a root it may not touch, and only a root with nothing in
// it would be guarded. An item under a nested type is contained by the root
// above that type, so an update writing deep inside an allowed root is
// allowed; an item whose parents reach no root of the document is refused,
// since the owner cannot have allowed what is under no root of theirs.
//
// The list is what the owner allows the update to write under, not what the
// document holds: a root the document holds already is refused too when the
// list does not name it, since an owner that keeps a root of its own beside
// the ones a peer may write needs the peer's updates refused when they reach
// into it. A root the document does not hold yet is allowed by its name, so
// an update the list allows may create it.
//
// A document with an allowed-roots list is a bounded document, as one with an
// apply budget is. roots runs under the document's lock, so it must not call
// the document, and the answer is read once per apply and not retained.
func WithAllowedRoots(roots func(origin any) []string) DocOption {
	return func(d *Doc) { d.allowedRoots = roots }
}

// SetAllowedRoots sets the list of WithAllowedRoots on a document already
// made, or with nil removes it. It takes the document lock.
func (d *Doc) SetAllowedRoots(roots func(origin any) []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.allowedRoots = roots
}

// WithRefusedContent names the kinds of content a remote update may not
// carry on this document: as each remote apply begins, the document calls
// kinds with the apply's origin, and an update carrying a struct of any kind
// in the answer is refused with ErrContentNotAllowed, rolled back whole as
// every refusal of a bounded document is, and returned as a
// RefusedUpdateError. A nil callback, or one answering a nil or empty slice,
// takes every kind of content, which is what a document without the option
// does.
//
// It is for an owner whose editor writes a known subset of the format and
// whose readers cannot show the rest. A browser that sends a kind the
// owner's schema has no node for is not refused by any count: one such
// struct is one struct of a few bytes, within any budget an owner can set,
// and every reader of the document then sees content outside the schema -
// one crafted edit takes a document out of editing for everyone until a
// person repairs it. The owner names the kinds its schema cannot hold and
// the apply refuses them where it reads them, before one is integrated.
//
// The check is on the content a struct carries and not on what it says it
// is: the apply reads each struct of the update, and a struct of a refused
// kind refuses the whole update wherever in the update it sits. An update
// that carries none of the named kinds is applied exactly as it would be
// with no option set.
//
// A document with this option is a bounded document, as one with an apply
// budget is. kinds runs under the document's lock, so it must not call the
// document, and the answer is read once per apply and not retained.
func WithRefusedContent(kinds func(origin any) []ContentKind) DocOption {
	return func(d *Doc) { d.refusedContent = kinds }
}

// SetRefusedContent sets the kinds of WithRefusedContent on a document
// already made, or with nil removes it. It takes the document lock.
func (d *Doc) SetRefusedContent(kinds func(origin any) []ContentKind) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refusedContent = kinds
}

// updateSub pairs a unique subscription ID with its callback so that
// unsubscribe closures can find and remove the right entry even when
// callbacks are removed out-of-order.
type updateSub struct {
	id uint64
	fn func([]byte, any)
}

// transactionSub pairs a unique subscription ID with a post-transaction callback.
type transactionSub struct {
	id uint64
	fn func(*Transaction)
}

// SubdocsEvent reports subdocument lifecycle changes for one transaction. The
// slices are freshly allocated per fire; do not retain or mutate them.
type SubdocsEvent struct {
	Added   []*Doc
	Removed []*Doc
	Loaded  []*Doc
}

// subdocsSub pairs a unique subscription ID with an OnSubdocs callback.
type subdocsSub struct {
	id uint64
	fn func(SubdocsEvent)
}

// Doc is the root of a Yjs collaborative document.
// All shared types (YArray, YMap, YText, …) live inside a Doc.
type Doc struct {
	clientID       ClientID      // read and written under mu, or before the doc is shared
	sharedClientID atomic.Uint64 // clientID's copy for ClientID, read without mu
	gc             bool
	guid           string // guid identifies this document (for subdocument embedding). Defaults to a random uuidv4 (see New) unless set with WithGUID.
	// linkMu guards shouldLoad, collectionID, item, parent and detached. It is a leaf lock:
	// no other lock is taken while it is held, so a parent's transaction can
	// take it, also for a document embedded in itself.
	linkMu          sync.Mutex
	shouldLoad      bool
	autoLoad        bool
	collectionID    string
	meta            any   // subdocument meta from Yjs opts, written back unchanged
	item            *Item // set on integrate when this Doc is embedded as a subdocument (#63)
	parent          *Doc  // document that owns item
	detached        bool  // true once an embedded Doc is removed or destroyed
	subdocs         map[string]*Doc
	onSubdocs       []subdocsSub
	maxPendingItems int // 0 = use defaultMaxPendingItems; see WithMaxPendingItems and #46
	// maxIntegrateScan is the scan bound of WithMaxIntegrateScan; zero or
	// negative for none. integrateScanBudget is the budget of
	// WithIntegrateScanBudget, which takes the bound's place while set.
	maxIntegrateScan    int
	integrateScanBudget func(origin any) int
	// applyBudget is the budget of WithApplyBudget, allowedRoots the list of
	// WithAllowedRoots, refusedContent the kinds of WithRefusedContent and
	// applyReport the callback of WithApplyReport, each nil for none.
	// applying is the running tally
	// of the remote apply on this document, between beginRemoteApply and
	// endRemoteApply and nil at every other time: the decode counts against
	// the budget through the document, since the decoders it counts in are
	// shared with the merge and snapshot decoders, which have no apply and
	// pass a document that holds no tally (applyTally).
	applyBudget    func(origin any) ApplyBudget
	allowedRoots   func(origin any) []string
	refusedContent func(origin any) []ContentKind
	applyReport    func(origin any, report ApplyReport)
	applying       *applyTally

	store *StructStore
	share map[string]sharedType // named root types

	// mu guards all document state. Transact and observer registration hold the
	// write lock; read-only methods (Get, ToSlice, Keys, etc.) hold the read
	// lock. Read methods must NOT be called from inside a Transact callback —
	// Transact holds the write lock and a nested RLock would deadlock.
	mu sync.RWMutex

	// subIDGen is a monotonically increasing counter used to issue unique IDs
	// to each observer subscription, enabling correct out-of-order unsubscribe.
	subIDGen uint64

	// onUpdate observers fire after each committed transaction with the encoded
	// incremental V1 update bytes and the transaction origin.
	onUpdate []updateSub

	// onAfterTxn observers fire after each committed transaction with the full
	// Transaction, which carries beforeState, afterState, deleteSet and Local.
	// Used by UndoManager; also available to application code that needs
	// richer change metadata than the binary update alone provides.
	onAfterTxn []transactionSub

	// undoManagerCount tracks how many UndoManagers are currently attached to
	// this doc. While count > 0, transaction-commit auto-GC (#78 H1) is
	// suppressed because UndoManager.applyStackItem must be able to flip
	// Deleted=false on items it captured — which requires their original
	// Content to still be present. Mutated under d.mu.
	undoManagerCount int
}

// ClientID returns the document's client identifier. A remote transaction
// that changes this document's own client's clock gives it a new random one
// when it commits, as Yjs does.
func (d *Doc) ClientID() ClientID {
	return ClientID(d.sharedClientID.Load())
}

// setClientID sets the document's client identifier; called under mu once the
// doc is shared.
func (d *Doc) setClientID(id ClientID) {
	d.clientID = id
	d.sharedClientID.Store(uint64(id))
}

// GUID returns the document's identifier. Every Doc has one — a random uuidv4 by default (Yjs parity), or the value passed to WithGUID.
func (d *Doc) GUID() string {
	return d.guid
}

// AutoLoad returns whether this subdocument should be auto-loaded by remote peers.
func (d *Doc) AutoLoad() bool {
	return d.autoLoad
}

// ShouldLoad returns whether a provider should sync this doc now.
func (d *Doc) ShouldLoad() bool {
	d.linkMu.Lock()
	defer d.linkMu.Unlock()
	return d.shouldLoad
}

// CollectionID returns the optional subdocument collection identifier. A
// subdocument without one takes its parent's when it is added, as in Yjs.
func (d *Doc) CollectionID() string {
	d.linkMu.Lock()
	defer d.linkMu.Unlock()
	return d.collectionID
}

// inheritCollectionID gives d the collection ID id when d has none, as Yjs
// cleanupTransactions does for each added subdocument.
func (d *Doc) inheritCollectionID(id string) {
	d.linkMu.Lock()
	defer d.linkMu.Unlock()
	if d.collectionID == "" {
		d.collectionID = id
	}
}

// Load signals that this subdocument's data should be synced. On a subdocument
// it sets shouldLoad=true and emits a subdocs "loaded" event on the parent; on a
// root doc it just sets shouldLoad=true. Safe to call multiple times.
//
// On an embedded subdocument, Load opens a transaction on the PARENT document,
// so it MUST NOT be called from inside the parent's Transact closure
// (re-entrant lock — same footgun as GetText-in-Transact).
func (d *Doc) Load() {
	d.linkMu.Lock()
	if d.shouldLoad {
		d.linkMu.Unlock()
		return
	}
	d.shouldLoad = true
	item, parent := d.item, d.parent
	d.linkMu.Unlock()
	if item == nil {
		return
	}
	parent.Transact(func(txn *Transaction) {
		// item can be deleted after Load copied it under linkMu and before this
		// parent transaction starts. Do not report a loaded event for that
		// removed item. A detached subdocument clears d.item and returns above.
		if item.Deleted {
			return
		}
		txn.addSubdocLoaded(d)
	})
}

// reattachSubdoc puts back the links detachSubdoc dropped, where the apply
// that deleted item is rolled back whole and item is alive again
// (applyJournal.rollBackTo). Without it a refused update leaves the
// subdocument detached though the document still holds the item that carries
// it: Load would set shouldLoad and tell no one, because a detached
// subdocument has no parent to open a transaction on.
func (d *Doc) reattachSubdoc(item *Item, parent *Doc) {
	d.linkMu.Lock()
	defer d.linkMu.Unlock()
	if d.item == nil && d.detached {
		d.item, d.parent, d.detached = item, parent, false
	}
}

// scanBounded is whether d bounds what its remote updates may do, by a scan
// bound (WithMaxIntegrateScan), a scan budget (WithIntegrateScanBudget), an
// apply budget (WithApplyBudget), an allowed-roots list (WithAllowedRoots), a
// set of refused content kinds (WithRefusedContent) or a report of what each
// apply left it at (WithApplyReport), and so applies each whole or not at
// all. Its caller holds d.mu.
//
// The budget, the list and the set are in it because their refusals are
// rollbacks like the scan bound's: a document that refuses an apply keeps the
// journal beginRemoteApply takes, and without it there would be nothing to
// roll an apply back to and the refused update would stand, half integrated.
// The report is in it because the journal is where the counts it carries are
// kept (applyTally): a document that only watches is bounded like one that
// refuses, which is what WithApplyReport says it is.
func (d *Doc) scanBounded() bool {
	return d.integrateScanBudget != nil || d.maxIntegrateScan > 0 ||
		d.applyBudget != nil || d.allowedRoots != nil || d.refusedContent != nil ||
		d.applyReport != nil
}

// applyScanLimit is the most items the conflict scans of a remote apply with
// origin may pass on d, or 0 for no limit: what d's budget returns for origin
// when it has one, otherwise its bound. Its caller holds d.mu.
func (d *Doc) applyScanLimit(origin any) int {
	if d.integrateScanBudget != nil {
		return max(d.callIntegrateScanBudget(origin), 0)
	}
	return max(d.maxIntegrateScan, 0)
}

// callIntegrateScanBudget calls the WithIntegrateScanBudget callback and tags a
// panic it raises as budgetCallbackPanic, so the apply's recovery names the
// callback (recoveredApplyErr) rather than item integration. The caller holds
// d.mu, as the callback contract requires.
func (d *Doc) callIntegrateScanBudget(origin any) (n int) {
	defer func() {
		if r := recover(); r != nil {
			panic(budgetCallbackPanic{"WithIntegrateScanBudget", r})
		}
	}()
	return d.integrateScanBudget(origin)
}

// attachSubdoc records the item and parent together under linkMu and returns
// the load request that integration must report.
func (d *Doc) attachSubdoc(item *Item, parent *Doc) bool {
	d.linkMu.Lock()
	defer d.linkMu.Unlock()
	if d.item != nil || d.detached {
		panic(ErrSubdocAlreadyIntegrated)
	}
	d.item, d.parent = item, parent
	return d.shouldLoad
}

// detachSubdoc drops links that would otherwise retain the former parent.
func (d *Doc) detachSubdoc(item *Item) {
	d.linkMu.Lock()
	defer d.linkMu.Unlock()
	if d.item == item {
		d.item, d.parent, d.detached = nil, nil, true
	}
}

// callApplyBudget calls the WithApplyBudget callback, tagging a panic it
// raises as callIntegrateScanBudget does; the caller holds d.mu.
func (d *Doc) callApplyBudget(origin any) (budget ApplyBudget) {
	defer func() {
		if r := recover(); r != nil {
			panic(budgetCallbackPanic{"WithApplyBudget", r})
		}
	}()
	return d.applyBudget(origin)
}

// callAllowedRoots calls the WithAllowedRoots callback, tagging a panic it
// raises as callIntegrateScanBudget does; the caller holds d.mu.
func (d *Doc) callAllowedRoots(origin any) (roots []string) {
	defer func() {
		if r := recover(); r != nil {
			panic(budgetCallbackPanic{"WithAllowedRoots", r})
		}
	}()
	return d.allowedRoots(origin)
}

// callRefusedContent calls the WithRefusedContent callback, tagging a panic
// it raises as callIntegrateScanBudget does; the caller holds d.mu.
func (d *Doc) callRefusedContent(origin any) (kinds []ContentKind) {
	defer func() {
		if r := recover(); r != nil {
			panic(budgetCallbackPanic{"WithRefusedContent", r})
		}
	}()
	return d.refusedContent(origin)
}

// maxPendingItemsLimit returns the effective cap on the pending queue depth.
func (d *Doc) maxPendingItemsLimit() int {
	if d.maxPendingItems <= 0 {
		return defaultMaxPendingItems
	}
	return d.maxPendingItems
}

// uuidV4 returns a random RFC-4122 v4 UUID string. Used as the default document
// guid so every Doc has a stable unique id for subdocument embedding (Yjs
// parity: Yjs defaults guid to uuidv4()). No external dependency.
func uuidV4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("crypto/rand failed: %w", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// NewClientID generates a fresh ClientID via crypto/rand.
//
// The 32-bit space matches Yjs JS upstream (which uses uint32 to stay within
// JavaScript's Number.MAX_SAFE_INTEGER). With uniform random distribution, the
// birthday-bound for collisions is around 65k peers — beyond that, collisions
// become probable. For multi-tenant deployments, callers may want to coordinate
// IDs externally; the random generation here is a collision-avoidance
// heuristic, not an authentication primitive. See SECURITY.md.
func NewClientID() ClientID {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand.Read returning error is unrecoverable — process is broken.
		panic(fmt.Errorf("crypto/rand failed: %w", err))
	}
	return ClientID(binary.BigEndian.Uint32(b[:]))
}

// New creates a new Doc with a randomly generated ClientID.
func New(opts ...DocOption) *Doc {
	d := &Doc{
		gc:         true,
		guid:       uuidV4(),
		shouldLoad: true,
		store:      newStructStore(),
		share:      make(map[string]sharedType),
		subdocs:    make(map[string]*Doc),
	}
	d.setClientID(NewClientID())
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// rawType is a placeholder sharedType created during update decoding when the
// concrete type (YArray, YMap, YText, …) is not yet known. It is upgraded
// transparently the first time the user calls GetArray/GetMap/GetText/etc.
type rawType struct {
	abstractType
}

func (r *rawType) baseType() *abstractType { return &r.abstractType }
func (r *rawType) prepareFire(_ *Transaction, _ map[string]struct{}) func() {
	return nil // rawType has no observers
}

// getOrCreateType returns the abstractType for a named root type, creating a
// rawType placeholder if none exists yet. Must be called with d.mu already held
// (i.e., from within a Transact callback or another locked helper).
func (d *Doc) getOrCreateType(name string) *abstractType {
	if t, ok := d.share[name]; ok {
		return t.baseType()
	}
	r := &rawType{}
	r.doc = d
	r.itemMap = make(map[string]*Item)
	r.owner = r
	r.name = name
	d.share[name] = r
	return &r.abstractType
}

// upgradeRawType moves raw's state into dst, stores dst in d.share[name], and
// repoints every struct parented to raw: integrated ones all sit on its list,
// and parked ones would otherwise integrate into the discarded placeholder.
// Must be called with d.mu held.
func (d *Doc) upgradeRawType(raw *rawType, dst sharedType, name string) {
	at := dst.baseType()
	*at = raw.abstractType // copy all fields (doc, start, itemMap, length, item, name)
	at.owner = dst
	at.cleanFormatting = false
	for item := at.start; item != nil; item = item.Right {
		item.Parent = at
	}
	if d.store.pending != nil {
		for _, item := range d.store.pending.items {
			if item != nil && item.Parent == &raw.abstractType {
				item.Parent = at
			}
		}
	}
	d.share[name] = dst
}

// panicRootTypeMismatch keeps a named root's concrete shared type stable. A
// root accessor with a different type must not replace the root and discard
// its contents.
func panicRootTypeMismatch(name string, existing sharedType, requested string) {
	panic(fmt.Errorf("%w: root %q has type %T; cannot access it as %s", ErrRootTypeMismatch, name, existing, requested))
}

// getArrayLocked is the lock-free body of GetArray. Callers must hold d.mu —
// either Doc.GetArray (takes it) or Transaction.GetArray (Transact holds it).
func (d *Doc) getArrayLocked(name string) *YArray {
	if t, ok := d.share[name]; ok {
		if arr, ok := t.(*YArray); ok {
			return arr
		}
		if raw, ok := t.(*rawType); ok {
			arr := &YArray{}
			d.upgradeRawType(raw, arr, name)
			return arr
		}
		panicRootTypeMismatch(name, t, "*crdt.YArray")
	}
	arr := &YArray{}
	arr.doc = d
	arr.itemMap = make(map[string]*Item)
	arr.owner = arr
	arr.name = name
	d.share[name] = arr
	return arr
}

// GetArray returns the named root YArray, creating it if it does not exist.
// It panics if that name already belongs to a different root type.
// It takes the document lock — inside a Transact callback use txn.GetArray
// instead (see Transact and GetText for the locking contract; issue #138).
func (d *Doc) GetArray(name string) *YArray {
	checkUTF8("Doc.GetArray", "name", name)
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getArrayLocked(name)
}

// getMapLocked is the lock-free body of GetMap. Callers must hold d.mu —
// either Doc.GetMap (takes it) or Transaction.GetMap (Transact holds it).
func (d *Doc) getMapLocked(name string) *YMap {
	if t, ok := d.share[name]; ok {
		if m, ok := t.(*YMap); ok {
			return m
		}
		if raw, ok := t.(*rawType); ok {
			m := &YMap{}
			d.upgradeRawType(raw, m, name)
			return m
		}
		panicRootTypeMismatch(name, t, "*crdt.YMap")
	}
	m := &YMap{}
	m.doc = d
	m.itemMap = make(map[string]*Item)
	m.owner = m
	m.name = name
	d.share[name] = m
	return m
}

// GetMap returns the named root YMap, creating it if it does not exist.
// It panics if that name already belongs to a different root type.
// It takes the document lock — inside a Transact callback use txn.GetMap
// instead (see Transact and GetText for the locking contract; issue #138).
func (d *Doc) GetMap(name string) *YMap {
	checkUTF8("Doc.GetMap", "name", name)
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getMapLocked(name)
}

// getTextLocked is the lock-free body of GetText. Callers must hold d.mu —
// either Doc.GetText (takes it) or Transaction.GetText (Transact holds it).
func (d *Doc) getTextLocked(name string) *YText {
	if t, ok := d.share[name]; ok {
		if txt, ok := t.(*YText); ok {
			return txt
		}
		if raw, ok := t.(*rawType); ok {
			txt := &YText{}
			d.upgradeRawType(raw, txt, name)
			return txt
		}
		panicRootTypeMismatch(name, t, "*crdt.YText")
	}
	txt := &YText{}
	txt.doc = d
	txt.itemMap = make(map[string]*Item)
	txt.owner = txt
	txt.name = name
	d.share[name] = txt
	return txt
}

// GetText returns the named root YText, creating it if it does not exist.
// It panics if that name already belongs to a different root type.
//
// GetText takes the document lock and therefore MUST NOT be called from
// inside a Transact callback — that self-deadlocks a non-reentrant lock,
// silently (issue #138). Inside a transaction use txn.GetText, or resolve
// the handle first:
//
//	txt := doc.GetText("t")
//	doc.Transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, "hi", nil) })
func (d *Doc) GetText(name string) *YText {
	checkUTF8("Doc.GetText", "name", name)
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getTextLocked(name)
}

// buildPhase2 runs under d.mu (called from Transact while the lock is held)
// and returns a closure that fires all observers in the correct order.
// The closure must be invoked OUTSIDE d.mu — observers may re-enter Doc
// methods that acquire d.mu, which would deadlock under the lock.
//
// Returns nil when there is nothing to fire: no observers, or a remote apply
// rolled back (boundedApplyState.rolledBack) or refused (Transaction.refused).
func buildPhase2(d *Doc, txn *Transaction) func() {
	// A remote apply rolled back (apply_rollback.go), or refused before it
	// began to integrate, left the document as it was: there is no update to
	// encode and nothing for any observer.
	if txn.boundedApply != nil && txn.boundedApply.rolledBack || txn.refused {
		return nil
	}
	// Encode the incremental update and snapshot observer slices while still
	// holding the lock so we get a consistent view.
	var updateBytes []byte
	if len(d.onUpdate) > 0 {
		updateBytes = encodeV1Locked(d, txn.beforeState)
	}

	// Snapshot per-type observer closures while the write lock is held.
	// prepareFire copies each type's observer slice and builds the event struct,
	// so concurrent Observe/Unobserve calls (which also hold the write lock)
	// cannot race with the fire loop below (N-C1).
	fireFns := make([]func(), 0, len(txn.changed))
	for t, keys := range txn.changed {
		if t.owner != nil {
			if fn := t.owner.prepareFire(txn, keys); fn != nil {
				fireFns = append(fireFns, fn)
			}
		}
	}
	txn.typeObserved = len(fireFns) > 0

	// Snapshot deep-observer chains.
	type deepEntry struct {
		fns []func(*Transaction)
	}
	firedDeep := make(map[*abstractType]struct{})
	var deepSnap []deepEntry
	for t := range txn.changed {
		current := t
		for current != nil {
			if _, already := firedDeep[current]; already {
				break
			}
			firedDeep[current] = struct{}{}
			if len(current.deepObservers) > 0 {
				fns := make([]func(*Transaction), len(current.deepObservers))
				for i, s := range current.deepObservers {
					fns[i] = s.fn
				}
				deepSnap = append(deepSnap, deepEntry{fns})
			}
			if current.item != nil {
				current = current.item.Parent
			} else {
				break
			}
		}
	}

	// Snapshot OnUpdate callbacks.
	onUpdateSnap := make([]func([]byte, any), len(d.onUpdate))
	for i, s := range d.onUpdate {
		onUpdateSnap[i] = s.fn
	}
	onAfterTxnSnap := make([]func(*Transaction), len(d.onAfterTxn))
	for i, s := range d.onAfterTxn {
		onAfterTxnSnap[i] = s.fn
	}

	// #63 — reconcile the subdoc registry and build the event under the lock.
	// This runs ABOVE the early-return below so GetSubdocs()/GetSubdocGUIDs()
	// reflect adds/removes even when there are zero OnSubdocs observers.
	hasSubdocs := len(txn.subdocsAdded)+len(txn.subdocsRemoved)+len(txn.subdocsLoaded) > 0
	var subdocsEv SubdocsEvent
	if hasSubdocs {
		parentCollectionID := d.CollectionID()
		for sd := range txn.subdocsAdded {
			sd.inheritCollectionID(parentCollectionID)
			d.subdocs[sd.guid] = sd
			subdocsEv.Added = append(subdocsEv.Added, sd)
		}
		for sd := range txn.subdocsRemoved {
			delete(d.subdocs, sd.guid)
			subdocsEv.Removed = append(subdocsEv.Removed, sd)
		}
		for sd := range txn.subdocsLoaded {
			subdocsEv.Loaded = append(subdocsEv.Loaded, sd)
		}
	}
	onSubdocsSnap := make([]func(SubdocsEvent), len(d.onSubdocs))
	for i, s := range d.onSubdocs {
		onSubdocsSnap[i] = s.fn
	}

	if len(fireFns) == 0 && len(deepSnap) == 0 && len(onUpdateSnap) == 0 &&
		len(onAfterTxnSnap) == 0 && (!hasSubdocs || len(onSubdocsSnap) == 0) {
		return nil
	}

	return func() {
		for _, fn := range fireFns {
			fn()
		}
		if txn.afterTypeObservers != nil {
			txn.afterTypeObservers()
		}
		for _, de := range deepSnap {
			for _, fn := range de.fns {
				fn(txn)
			}
		}
		for _, fn := range onUpdateSnap {
			fn(updateBytes, txn.Origin)
		}
		for _, fn := range onAfterTxnSnap {
			fn(txn)
		}
		if hasSubdocs {
			for _, fn := range onSubdocsSnap {
				fn(subdocsEv)
			}
		}
	}
}

// transactInternal is the shared transaction entry point that Transact,
// TransactContext, TransactE, and TransactContextE all delegate to. It
// acquires d.mu, runs fn under the lock with panic-safe cleanup, fires
// Phase 2 observers outside the lock, and re-raises any panic to the
// caller. The ctx parameter is stored on the Transaction struct and
// exposed to fn via Transaction.Ctx().
//
// fn's returned error is captured as the named return value retErr.
// Observers fire, unless a remote apply was refused (Transaction.refused),
// before fn's error is returned.
//
// See docs/superpowers/specs/2026-04-21-transact-panic-safety-design.md
// for the full rationale on the defer/recover structure.
func (d *Doc) transactInternal(ctx context.Context, fn func(*Transaction) error, origin ...any) (retErr error) {
	var orig any
	if len(origin) > 0 {
		orig = origin[0]
	}

	d.mu.Lock()
	// A parking apply records only what this transaction parks.
	if d.store.parked != nil {
		*d.store.parked = transactionParked{}
	}

	txn := &Transaction{
		doc:         d,
		Origin:      orig,
		Local:       true,
		deleteSet:   newOrderedDeleteSet(),
		beforeState: d.store.StateVector(),
		// Pre-size changed to common-case capacity (#54 A): most transactions
		// touch 1-3 types, and the zero-hint alloc forces immediate rehashing
		// on the very first append. newItems is left nil — pre-sizing it here
		// added 1 alloc per txn even when no ContentString was inserted, which
		// hurt array/map-only workloads more than it helped text workloads
		// (the squashRuns target).
		changed: make(map[*abstractType]map[string]struct{}, 4),
		ctx:     ctx,
	}

	defer func() {
		r := recover()

		if r != nil {
			func() {
				defer func() { _ = recover() }()
				if txn.afterState == nil {
					txn.afterState = d.store.StateVector()
				}
			}()
		}

		var phase2 func()
		if r != nil {
			func() {
				defer func() { _ = recover() }()
				phase2 = buildPhase2(d, txn)
			}()
		} else {
			phase2 = buildPhase2(d, txn)
		}

		// Yjs cleans up the format markers a remote change left redundant
		// after the change's observers, in a follow-up local transaction,
		// and GCs the change only then. A type observer may give a text its
		// first marker, so with type observers to fire the gate waits.
		deferGC := false
		if r == nil && !txn.Local {
			if needsFormattingCleanup(txn) {
				txn.formatCleanup, deferGC = true, true
			} else if txn.typeObserved && changedText(txn) {
				txn.afterTypeObservers = func() {
					d.mu.Lock()
					txn.formatCleanup = needsFormattingCleanup(txn)
					d.mu.Unlock()
				}
				deferGC = true
			}
		}

		// #78 H1 — Auto-GC at transaction commit. Runs AFTER buildPhase2 so
		// the observer Deltas have already been computed against the original
		// content; runs BEFORE Unlock so other goroutines never see partially-
		// GC'd state. No-op when doc.gc is false.
		//
		// When an UndoManager is attached we skip auto-GC: undoing a deletion
		// re-inserts a COPY of the deleted item's content (applyStackItem →
		// redoItem), which only works if that original content is still present.
		// Yjs handles this with a per-item keep flag; we take the conservative
		// position of disabling auto-GC entirely while any UndoManager is
		// registered. RunGC remains available as the explicit manual entry point.
		if d.gc && d.undoManagerCount == 0 && !deferGC {
			gcTxnDeleteSet(d, txn)
		}

		// The report of a remote apply that committed, after the commit's own
		// work and under the lock the apply held, so the structs it reports are
		// the ones the document is left holding and a panicking callback cannot
		// skip what the commit still owes the document (WithApplyReport).
		//
		// A callback run before the commit that panicked would make this a
		// panicking transaction, which skips the remote formatting cleanup:
		// after a remote deletion under such a callback, a later plain insert
		// came out bold. A panic here is carried like a panic in fn - kept and
		// re-raised after the unlock - so the consumer still hears it and the
		// document is left whole either way.
		if reported := reportApplyRecovered(txn); reported != nil && r == nil {
			r = reported
		}

		// The document's park record drops this transaction's views of the queue.
		if d.store.parked != nil {
			*d.store.parked = transactionParked{}
		}

		// A remote transaction that moved this document's own clock gives it a
		// new client ID, as Yjs cleanupTransactions does, so a local write never
		// continues a clock that another peer set.
		if !txn.Local && txn.afterState != nil && txn.afterState.Clock(d.clientID) != txn.beforeState.Clock(d.clientID) {
			d.setClientID(NewClientID())
		}

		// Mark the transaction committed BEFORE releasing the lock so the
		// txn-scoped accessors fail loudly from observers (which receive the
		// *Transaction after unlock) or from a retained txn, instead of
		// mutating d.share unsynchronized. See Transaction.done.
		txn.done = true
		d.mu.Unlock()

		if deferGC {
			// Runs even if an observer panics, as Yjs runs the cleanup.
			defer d.afterRemoteObservers(txn)
		}
		if phase2 != nil {
			phase2()
		}

		if r != nil {
			panic(r)
		}
	}()

	retErr = fn(txn)
	// The scans' marks are the transaction's alone; an observer that keeps the
	// *Transaction must not keep them.
	if txn.boundedApply != nil {
		txn.boundedApply.scanMarks = nil
	}

	rearbitrateMoves(txn)
	txn.afterState = d.store.StateVector()

	squashRuns(txn)
	// #78 H2 — Re-merge transient splits. Must run after squashRuns so any
	// new-item run that squash-collapsed disappears from tryMergeWithLefts's
	// view (its left.Right == item check filters out absorbed items).
	tryMergeWithLefts(txn)
	return retErr
}

// Transact executes fn inside a transaction. All insertions and deletions made
// during fn are batched; observers fire once after fn returns.
//
// The optional origin value tags the transaction: observers receive it
// (OnUpdate's origin parameter, Transaction.Origin) and UndoManager's
// WithTrackedOrigins filters on it — always by Go interface equality (==).
// If you mint per-caller origin tokens as pointers, use a pointer to a
// NON-zero-size type (e.g. type token struct{ _ byte }): pointers to
// zero-size types may all share one address (runtime.zerobase) and compare
// equal, silently merging origins that were meant to be distinct — see
// WithTrackedOrigins' doc for the full trap (#203).
//
// Transact holds the document write lock for the duration of fn. Anything
// that takes that same non-reentrant lock MUST NOT be called from inside fn
// — it deadlocks silently, and Go exposes no goroutine identity with which
// to detect it (issue #138). That includes the Doc-level root accessors
// (GetMap, GetText, GetArray, GetXmlFragment), Doc.Load, observer
// registration (Observe, ObserveDeep, OnUpdate, OnAfterTransaction,
// OnSubdocs), and — notably — nested Transact on the same Doc, which
// remains an undetected deadlock.
//
// Inside fn, resolve root types through the Transaction instead — its
// accessors reuse the lock the transaction already holds — or resolve
// handles before the transaction:
//
//	doc.Transact(func(txn *crdt.Transaction) {
//	    txn.GetMap("m").Set(txn, "k", 1) // in-transaction accessor: works
//	})
//
//	m := doc.GetMap("m") // or: resolve outside...
//	doc.Transact(func(txn *crdt.Transaction) {
//	    m.Set(txn, "k", 1) // ...and use the handle inside
//	})
//
// (Observer callbacks are NOT affected — they fire after the lock is
// released and may call any Doc method; see below.)
//
// Observers are intentionally fired OUTSIDE the document lock. This means:
//   - Observer callbacks may safely call back into any Doc method (Transact,
//     GetArray, ApplyUpdate, etc.) without deadlocking.
//   - The document may be modified by another goroutine between the time fn
//     returns and the time observers fire; observers should treat txn as a
//     snapshot of what changed, not a live view of the current state.
//
// Panic semantics:
//   - If fn panics (or any Phase 1 work panics), d.mu is released via defer.
//   - Observers fire with whatever partial state was committed before the
//     panic: OnUpdate receives a V1 update describing the mutations that
//     completed (non-empty if fn mutated; minimal but well-formed if fn
//     panicked before mutating); per-type, deep, and OnAfterTransaction
//     observers fire for what was recorded in txn.changed.
//   - The original panic is re-raised to the caller after observers fire.
//   - Rollback is NOT supported. The in-memory doc reflects fn's partial
//     work. Callers who need atomicity must implement it above Transact.
//     This matches the behavior of Yjs JS and the Rust yrs implementation;
//     yrs explicitly directs users to UndoManager for transactional undo.
//   - If fn panics and an observer callback also panics during the partial
//     firing, the observer's panic reaches the caller and the original
//     fn panic value is lost.
func (d *Doc) Transact(fn func(*Transaction), origin ...any) {
	_ = d.transactInternal(context.Background(), func(t *Transaction) error {
		fn(t)
		return nil
	}, origin...)
}

// TransactE is like Transact but allows fn to return an error. The error is
// returned to the caller after all observers have fired. Mutations commit
// regardless of the error (no rollback — matches Yjs JS doc.transact(f) and
// the Rust yrs implementation).
//
//   - If fn returns nil, TransactE returns nil.
//   - If fn returns a non-nil error, that error becomes the return value of
//     TransactE. Mutations that fn completed before returning are committed.
//   - Observers (OnUpdate, OnAfterTransaction, per-type) fire BEFORE TransactE
//     returns the error — matching the Yjs JS "finally-block observer" pattern.
//   - If fn panics, the existing v1.1.1 panic-safety contract applies: partial
//     state commits, observers fire with partial changes, and the panic is
//     re-raised to the caller. TransactE does not swallow panics.
func (d *Doc) TransactE(fn func(*Transaction) error, origin ...any) error {
	return d.transactInternal(context.Background(), fn, origin...)
}

// OnUpdate registers a callback that fires after every committed transaction.
// The callback receives the incremental V1 update bytes for that transaction
// and the origin value passed to Transact. Returns an unsubscribe function.
//
// Callbacks that compare the origin (an echo guard, a per-user filter) do so
// with Go ==, so the caution on Transact's doc applies: pointer origin
// tokens must point at non-zero-size types, or distinct tokens may alias
// (runtime.zerobase, #203).
//
// The unsubscribe function is safe to call concurrently and handles
// out-of-order unsubscription correctly (no index-capture bug).
func (d *Doc) OnUpdate(fn func(update []byte, origin any)) func() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.subIDGen++
	id := d.subIDGen
	d.onUpdate = append(d.onUpdate, updateSub{id: id, fn: fn})
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		for i, s := range d.onUpdate {
			if s.id == id {
				d.onUpdate = append(d.onUpdate[:i], d.onUpdate[i+1:]...)
				return
			}
		}
	}
}

// OnAfterTransaction registers a callback that fires after every committed
// transaction, receiving the full Transaction object. This provides richer
// change metadata than OnUpdate (beforeState, afterState, deleteSet, Local
// flag) and is the hook used by UndoManager. Returns an unsubscribe function.
func (d *Doc) OnAfterTransaction(fn func(*Transaction)) func() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.subIDGen++
	id := d.subIDGen
	d.onAfterTxn = append(d.onAfterTxn, transactionSub{id: id, fn: fn})
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		for i, s := range d.onAfterTxn {
			if s.id == id {
				d.onAfterTxn = append(d.onAfterTxn[:i], d.onAfterTxn[i+1:]...)
				return
			}
		}
	}
}

// OnSubdocs registers a callback fired once after any transaction that adds,
// removes, or loads subdocuments. Returns an unsubscribe function. Mirrors
// OnUpdate (safe concurrent, out-of-order unsubscribe).
func (d *Doc) OnSubdocs(fn func(SubdocsEvent)) func() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.subIDGen++
	id := d.subIDGen
	d.onSubdocs = append(d.onSubdocs, subdocsSub{id: id, fn: fn})
	return func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		for i, s := range d.onSubdocs {
			if s.id == id {
				d.onSubdocs = append(d.onSubdocs[:i], d.onSubdocs[i+1:]...)
				return
			}
		}
	}
}

// GetSubdocs returns the resident subdocuments, sorted by guid.
func (d *Doc) GetSubdocs() []*Doc {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*Doc, 0, len(d.subdocs))
	for _, sd := range d.subdocs {
		out = append(out, sd)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].guid < out[j].guid })
	return out
}

// GetSubdocGUIDs returns the guids of the resident subdocuments, sorted.
func (d *Doc) GetSubdocGUIDs() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.subdocs))
	for g := range d.subdocs {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// getXmlFragmentLocked is the lock-free body of GetXmlFragment. Callers must
// hold d.mu — either Doc.GetXmlFragment (takes it) or
// Transaction.GetXmlFragment (Transact holds it).
func (d *Doc) getXmlFragmentLocked(name string) *YXmlFragment {
	if t, ok := d.share[name]; ok {
		if f, ok := t.(*YXmlFragment); ok {
			return f
		}
		if raw, ok := t.(*rawType); ok {
			f := &YXmlFragment{}
			d.upgradeRawType(raw, f, name)
			return f
		}
		panicRootTypeMismatch(name, t, "*crdt.YXmlFragment")
	}
	f := &YXmlFragment{}
	f.doc = d
	f.itemMap = make(map[string]*Item)
	f.owner = f
	f.name = name
	d.share[name] = f
	return f
}

// GetXmlFragment returns the named root YXmlFragment, creating it if it does
// not exist. It panics if that name already belongs to a different root type.
// It takes the document lock — inside a Transact callback use
// txn.GetXmlFragment instead (see Transact and GetText for the locking
// contract; issue #138).
func (d *Doc) GetXmlFragment(name string) *YXmlFragment {
	checkUTF8("Doc.GetXmlFragment", "name", name)
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getXmlFragmentLocked(name)
}

// StateVector returns the current state vector of the document.
func (d *Doc) StateVector() StateVector {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.store.StateVector()
}

// StructCount is how many structs the document holds: the items of every
// client, deleted ones and the placeholders of collected ones included, which
// is the count an owner sizes an apply budget against (WithApplyBudget) and
// the count that decides what a load of the document's whole state costs. It
// takes the document's read lock, and so must not be called inside a Transact
// callback (see Transact and GetText; issue #138).
func (d *Doc) StructCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.store.Count()
}

// EncodeStateAsUpdate encodes the full document state as a V1 binary update.
func (d *Doc) EncodeStateAsUpdate() []byte {
	return EncodeStateAsUpdateV1(d, nil)
}

// ApplyUpdate decodes and integrates a V1 binary update into the document.
func (d *Doc) ApplyUpdate(update []byte) error {
	return ApplyUpdateV1(d, update, nil)
}

// TransactContext is like Transact but associates a context with the
// transaction so fn can cooperatively observe cancellation.
//
// If ctx is already cancelled when TransactContext is called, fn is not
// invoked and ctx.Err() is returned immediately.
//
// Inside fn, callers can poll txn.Ctx().Err() or <-txn.Ctx().Done() to
// detect cancellation and return early. Any mutations fn completed
// before returning are committed (no rollback, consistent with the
// Transact panic-safety contract — see Transact's godoc).
//
// If ctx cancels during fn and fn does not poll, fn runs to completion —
// Go has no safe mechanism for interrupting arbitrary fn code. ctx.Err()
// is returned after the transaction commits as a "missed cancellation"
// signal to the caller. It is not an error flag for the mutations; those
// are committed either way.
//
// Neither Yjs JS nor the Rust yrs implementation offers mid-fn
// interruption either; cooperative polling is the ecosystem norm.
func (d *Doc) TransactContext(ctx context.Context, fn func(*Transaction), origin ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_ = d.transactInternal(ctx, func(t *Transaction) error {
		fn(t)
		return nil
	}, origin...)
	return ctx.Err()
}

// TransactContextE is like TransactContext but allows fn to return an error.
// It combines context-cancellation awareness with fn-error propagation.
//
//   - If ctx is already cancelled when TransactContextE is called, fn is not
//     invoked and ctx.Err() is returned immediately.
//   - If fn returns nil and ctx is not cancelled after the transaction commits,
//     TransactContextE returns nil.
//   - If fn returns a non-nil error but ctx is still fine, that error is
//     returned.
//   - If ctx cancels during fn (cooperative only — Go cannot safely interrupt
//     arbitrary fn code), ctx.Err() wins over fn's error. This matches the
//     precedent set by TransactContext and Yjs JS's ecosystem norm of
//     cooperative cancellation polling via txn.Ctx().
//   - Mutations commit regardless of fn's error or ctx cancellation (no
//     rollback — matches Yjs JS doc.transact(f) and yrs semantics).
//   - Observers fire BEFORE TransactContextE returns, even when fn errors.
//   - Panics in fn follow the v1.1.1 panic-safety contract (partial commit,
//     observer fire, re-raise).
func (d *Doc) TransactContextE(ctx context.Context, fn func(*Transaction) error, origin ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fnErr := d.transactInternal(ctx, fn, origin...)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fnErr
}

// PendingStats is a snapshot of the doc's pending-queue depth. Returned by
// Doc.PendingStats. Used for observability — operators can monitor queue
// growth to detect adversarial peers or persistent convergence gaps. The
// snapshot is immediately stale; do not use as a synchronization primitive.
type PendingStats struct {
	// Items is the number of decoded items parked in the pending queue
	// (waiting for cross-update Origin/OriginRight dependencies or
	// same-client clock-gap dependencies to arrive).
	Items int

	// DeleteRanges is the total number of delete-set ranges in pendingDs
	// across all clients. Each range is one [clock, clock+len) span.
	DeleteRanges int

	// MissingFor lists the ClientIDs that pending items reference but
	// haven't yet received. Sorted ascending. Read-only.
	MissingFor []ClientID
}

// PendingStats returns a snapshot of the pending-queue state. Cheap; takes
// a read lock and copies a few values. Intended for operational monitoring
// of out-of-order delta convergence; see v1.2.0 release notes for the
// pending-structs machinery this exposes.
func (d *Doc) PendingStats() PendingStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats := PendingStats{}
	if d.store.pending != nil {
		stats.Items = d.store.pending.heldStructs()
		// missing is a StateVector (map[ClientID]uint64) — extract sorted keys
		missing := make([]ClientID, 0, len(d.store.pending.missing))
		for c := range d.store.pending.missing {
			missing = append(missing, c)
		}
		sort.Slice(missing, func(i, j int) bool { return missing[i] < missing[j] })
		stats.MissingFor = missing
	}
	stats.DeleteRanges = d.store.pendingDsRanges
	return stats
}

// Destroy detaches all observers and clears internal state, releasing
// references held by the document. After Destroy the document must not be used.
func (d *Doc) Destroy() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onUpdate = nil
	d.onAfterTxn = nil
	d.share = make(map[string]sharedType)
	d.store = newStructStore()
	d.linkMu.Lock()
	d.item, d.parent, d.detached = nil, nil, true
	d.linkMu.Unlock()
}
