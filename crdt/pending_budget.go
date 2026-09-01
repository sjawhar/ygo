package crdt

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"

	"github.com/reearth/ygo/encoding"
	"github.com/reearth/ygo/internal/anycodec"
)

// pendingBudget checks whether dependencies can resolve within this update
// before charging its unresolved structs to the persistent pending limit.
// Preflight retains one blocked head and a cursor per wire group, never one
// dependency tuple per struct in a blocked tail.
type pendingBudget struct {
	initial   StateVector
	update    []byte
	remaining int
	v2        bool
	checked   bool
	v2Start   *v2Decoder
	v2Rest    encoding.Decoder
}

func newPendingBudget(doc *Doc, initial StateVector, update []byte, v2 bool) pendingBudget {
	remaining := doc.maxPendingItemsLimit()
	if doc.store.pending != nil {
		remaining -= len(doc.store.pending.items)
	}
	return pendingBudget{initial: initial, update: update, remaining: remaining, v2: v2}
}

func (b *pendingBudget) scanner() *pendingScanner {
	if b.v2Start == nil {
		return newPendingScanner(b.update, b.v2)
	}
	// Column cursors copy by value; the immutable columns and string pool are
	// shared with the real decoder. Only the raw rest cursor needs rebinding.
	v2 := *b.v2Start
	rest := b.v2Rest
	v2.restDec = &rest
	return &pendingScanner{v2: &v2, rest: &rest}
}

func (b *pendingBudget) check(count int) error {
	if b.checked || count < b.remaining {
		return nil
	}
	if b.remaining < 0 {
		return ErrInvalidUpdate
	}
	known := make(StateVector, len(b.initial))
	for client, clock := range b.initial {
		known[client] = clock
	}
	heads := 0
	// Two allocation-light passes handle permanently missing heads, cycles,
	// and unrelated progress without retaining a head for every wire group.
	// This bound is independent of chain length: remaining dependencies use
	// the worklist, never an unbounded sequence of full-message scans.
	for pass := 0; pass < 2; pass++ {
		result, err := scanPendingGroups(b.scanner(), known, b.remaining, nil, false)
		if err != nil {
			return wrapUpdateErr(err)
		}
		if result.pending <= b.remaining {
			b.checked = true
			return nil
		}
		if !result.progressed {
			return ErrInvalidUpdate
		}
		heads = result.heads
	}
	// The validated head count bounds this allocation. Exact capacity avoids
	// keeping successive large backing arrays alive on fragmented updates.
	groups := make([]pendingGroup, 0, heads)
	result, err := scanPendingGroups(b.scanner(), known, b.remaining, groups, true)
	if err != nil {
		return wrapUpdateErr(err)
	}
	groups = result.groups
	if len(groups) > 0 {
		if err := resolvePendingGroups(groups, known); err != nil {
			return wrapUpdateErr(err)
		}
		pending := 0
		for i := range groups {
			g := &groups[i]
			for !g.done {
				if g.end > known.Clock(g.client) {
					pending++
					if pending > b.remaining {
						return ErrInvalidUpdate
					}
				}
				if err := g.next(); err != nil {
					return wrapUpdateErr(err)
				}
			}
		}
	}
	b.checked = true
	return nil
}

type pendingPass struct {
	groups         []pendingGroup
	heads, pending int
	progressed     bool
}

func scanPendingGroups(s *pendingScanner, known StateVector, remaining int, groups []pendingGroup, collect bool) (pendingPass, error) {
	result := pendingPass{groups: groups}
	clients := s.uint()
	if clients > maxV2Items {
		return result, ErrInvalidUpdate
	}
	var total uint64
	for i := uint64(0); i < clients && s.err == nil; i++ {
		n := s.uint()
		total += n
		if total > maxV2Items {
			return result, ErrInvalidUpdate
		}
		client, clock := s.client(), s.uint()
		blocked := false
		for j := uint64(0); j < n && s.err == nil; j++ {
			length, skip, deps, numDeps := s.item()
			end := clock + length
			if end < clock {
				return result, ErrInvalidUpdate
			}
			if !skip && end > known.Clock(client) {
				ready := clock <= known.Clock(client)
				for _, dep := range deps[:numDeps] {
					if dep.Clock >= known.Clock(dep.Client) {
						ready = false
					}
				}
				if ready {
					known[client] = end
					result.progressed = true
				} else {
					result.pending++
					if clients == 1 && result.pending > remaining {
						// No other group can fill this group's first blocked head. All
						// following uncovered non-Skip structs remain permanently pending.
						return result, ErrInvalidUpdate
					}
					if !blocked {
						result.heads++
						if collect {
							group := pendingGroup{client: client, clock: clock, end: end, deps: deps, numDeps: numDeps, remaining: n - j - 1}
							if group.remaining > 0 {
								group.cursor = s.checkpoint()
							}
							result.groups = append(result.groups, group)
						}
						blocked = true
					}
				}
			}
			clock = end
		}
	}
	return result, s.err
}

// A group retains its current blocked struct and the cursor just after it.
// Multiple/overlapping groups for one client are independent: another group
// can cover this head even while its explicit dependencies remain missing.
type pendingGroup struct {
	client       ClientID
	clock, end   uint64
	deps         [3]ID
	numDeps      int
	remaining    uint64
	cursor       *pendingCursor
	queued, done bool
}

type pendingCursor struct {
	scanner pendingScanner
	rest    encoding.Decoder
}

func (s *pendingScanner) checkpoint() *pendingCursor {
	c := &pendingCursor{scanner: *s, rest: *s.rest}
	c.scanner.rest = &c.rest
	if s.v2 != nil {
		v2 := *s.v2
		v2.restDec = &c.rest
		c.scanner.v2 = &v2
	}
	return c
}

func (g *pendingGroup) next() error {
	for g.remaining > 0 {
		s := &g.cursor.scanner
		length, skip, deps, numDeps := s.item()
		if s.err != nil {
			return s.err
		}
		clock := g.end
		end := clock + length
		if end < clock {
			return ErrInvalidUpdate
		}
		g.clock, g.end = clock, end
		g.remaining--
		if !skip {
			g.deps, g.numDeps = deps, numDeps
			return nil
		}
	}
	g.done = true
	return nil
}

func (g *pendingGroup) missing(known StateVector) (ClientID, uint64, bool) {
	if g.clock > known.Clock(g.client) {
		return g.client, g.clock, true
	}
	for _, dep := range g.deps[:g.numDeps] {
		if dep.Clock >= known.Clock(dep.Client) {
			return dep.Client, dep.Clock + 1, true
		}
	}
	return 0, 0, false
}

// Only decoder cursors and scalar clocks survive a scan. V1 reads the caller's
// buffer; V2 uses its normal column decoder without building a key dictionary
// or decoding Any/JSON values into object trees.
type pendingScanner struct {
	rest *encoding.Decoder
	v2   *v2Decoder
	keys int
	err  error
}

func newPendingScanner(update []byte, v2 bool) *pendingScanner {
	s := &pendingScanner{}
	if v2 {
		s.v2, s.err = newV2Decoder(update)
		if s.err == nil {
			s.rest = s.v2.restDec
		}
	} else {
		s.rest = encoding.NewDecoder(update)
	}
	return s
}

func (s *pendingScanner) uint() uint64 {
	if s.err != nil {
		return 0
	}
	v, err := s.rest.ReadVarUint()
	s.err = err
	return v
}
func (s *pendingScanner) byte() byte {
	if s.err != nil {
		return 0
	}
	v, err := s.rest.ReadUint8()
	s.err = err
	return v
}
func (s *pendingScanner) client() ClientID {
	if s.v2 == nil {
		return ClientID(s.uint())
	}
	if s.err != nil {
		return 0
	}
	v, err := s.v2.readClient()
	s.err = err
	return v
}
func (s *pendingScanner) id(right bool) ID {
	if s.v2 == nil {
		return ID{Client: s.client(), Clock: s.uint()}
	}
	if s.err != nil {
		return ID{}
	}
	var id ID
	if right {
		id, s.err = s.v2.readRightID()
	} else {
		id, s.err = s.v2.readLeftID()
	}
	return id
}
func (s *pendingScanner) length() uint64 {
	if s.v2 == nil {
		return s.uint()
	}
	if s.err != nil {
		return 0
	}
	v, err := s.v2.readLen()
	s.err = err
	return uint64(v)
}
func (s *pendingScanner) bytes() {
	if s.err == nil {
		_, s.err = s.rest.ReadVarBytes()
	}
}
func (s *pendingScanner) any() {
	if s.err == nil {
		rest := s.rest.RemainingBytes()
		consumed, err := anycodec.Skip(rest)
		// Keep the existing pointer (also v2.restDec), resetting its buffer to
		// the unread suffix. This advances without re-reading every skipped byte.
		*s.rest = *encoding.NewDecoder(rest[consumed:])
		s.err = err
	}
}

func (s *pendingScanner) text() uint64 {
	if s.err != nil {
		return 0
	}
	var n uint64
	if s.v2 != nil {
		var str string
		str, s.err = s.v2.readString()
		for _, r := range str {
			n++
			if r > 0xffff {
				n++
			}
		}
	} else {
		var raw []byte
		raw, s.err = s.rest.ReadVarBytes()
		if s.err == nil && !utf8.Valid(raw) {
			s.err = encoding.ErrInvalidUTF8
		}
		for len(raw) > 0 {
			r, size := utf8.DecodeRune(raw)
			raw = raw[size:]
			n++
			if r > 0xffff {
				n++
			}
		}
	}
	return n
}
func (s *pendingScanner) key() {
	if s.v2 == nil {
		s.text()
		return
	}
	if s.err != nil {
		return
	}
	var index int64
	index, s.err = s.v2.keyClockDec.Read()
	if index < 0 {
		s.err = ErrInvalidUpdate
	}
	if s.err == nil && index >= int64(s.keys) {
		s.text()
		s.keys++
	}
}
func (s *pendingScanner) item() (length uint64, skip bool, deps [3]ID, numDeps int) {
	var info byte
	if s.err != nil {
		return
	}
	if s.v2 == nil {
		info = s.byte()
	} else {
		info, s.err = s.v2.readInfo()
	}
	tag := info & 0x1f
	if tag == 0 {
		return s.length(), false, deps, 0
	}
	if tag == 10 {
		return s.uint(), true, deps, 0
	}
	if info&flagHasOrigin != 0 {
		deps[numDeps] = s.id(false)
		numDeps++
	}
	if info&flagHasRightOrigin != 0 {
		deps[numDeps] = s.id(true)
		numDeps++
	}
	if numDeps == 0 {
		var named bool
		if s.v2 == nil {
			named = s.byte() == 1
		} else if s.err == nil {
			named, s.err = s.v2.readParentInfo()
		}
		if named {
			s.text()
		} else {
			deps[0] = s.id(false)
			numDeps++
		}
		if info&flagHasParentSub != 0 {
			s.text()
		}
	}
	return s.content(tag), false, deps, numDeps
}
func (s *pendingScanner) content(tag byte) uint64 {
	if s.err != nil {
		return 0
	}
	switch tag {
	case wireDeleted:
		return s.length()
	case wireJSON, wireAny:
		n := s.length()
		if (s.v2 != nil && n > maxV2Items) || (s.v2 == nil && n > uint64(s.rest.Remaining())) {
			s.err = ErrInvalidUpdate
			return 0
		}
		if tag == wireJSON && s.v2 == nil {
			// Match decodeContent's whole-item JSON-first legacy fallback,
			// without constructing the values or their nested object trees.
			probe := *s.rest
			jsonErr := skipJSONVals(&probe, n)
			if jsonErr == nil {
				*s.rest = probe
			} else if n > 0 && isAnyTag(s.rest.RemainingBytes()[0]) {
				for i := uint64(0); i < n && s.err == nil; i++ {
					s.any()
				}
				if s.err != nil {
					s.err = jsonErr
				}
			} else {
				s.err = jsonErr
			}
		} else {
			for i := uint64(0); i < n && s.err == nil; i++ {
				if tag == wireJSON {
					s.json()
				} else {
					s.any()
				}
			}
		}
		return n
	case wireBinary:
		s.bytes()
	case wireString:
		return s.text()
	case wireEmbed:
		if s.v2 == nil {
			s.json()
		} else {
			s.any()
		}
	case wireFormat:
		s.key()
		if s.v2 == nil {
			s.json()
		} else {
			s.any()
		}
	case wireType:
		var ref byte
		if s.v2 == nil {
			ref = s.byte()
		} else {
			ref, s.err = s.v2.readTypeRef()
		}
		if ref == 3 || ref == 5 {
			s.key()
		}
	case wireDoc:
		if s.v2 == nil {
			s.bytes()
		} else {
			s.text()
		}
		s.any()
	case wireMove:
		s.uint()
		s.uint()
		s.uint()
	default:
		s.err = ErrInvalidUpdate
	}
	return 1
}

// json validates JSON-bearing content without materializing its values.
func (s *pendingScanner) json() {
	if s.err != nil {
		return
	}
	if s.v2 == nil {
		s.err = skipJSONVals(s.rest, 1)
		return
	}
	var value string
	value, s.err = s.v2.readString()
	if s.err == nil {
		s.err = validatePendingJSON([]byte(value))
	}
}

// skipJSONVals validates V1 JSON text without allocating decoded values.
func skipJSONVals(dec *encoding.Decoder, n uint64) error {
	for i := uint64(0); i < n; i++ {
		raw, err := dec.ReadVarBytes()
		if err != nil {
			return err
		}
		if err := validatePendingJSON(raw); err != nil {
			return err
		}
	}
	return nil
}

// Match json.Unmarshal's syntax and float-range validation. This also keeps
// the V1 legacy Any fallback's choice consistent for ambiguous 116–127 bytes.
func validatePendingJSON(raw []byte) error {
	if !utf8.Valid(raw) {
		return encoding.ErrInvalidUTF8
	}
	if bytes.Equal(raw, []byte("undefined")) {
		return nil
	}
	if !json.Valid(raw) {
		return ErrInvalidUpdate
	}
	for j := 0; j < len(raw); j++ {
		if raw[j] == '"' {
			for j++; raw[j] != '"'; j++ {
				if raw[j] == '\\' {
					j++
				}
			}
		} else if raw[j] == '-' || raw[j] >= '0' && raw[j] <= '9' {
			start := j
			for j < len(raw) && (raw[j] >= '0' && raw[j] <= '9' || raw[j] == '-' || raw[j] == '+' || raw[j] == '.' || raw[j] == 'e' || raw[j] == 'E') {
				j++
			}
			if _, err := strconv.ParseFloat(string(raw[start:j]), 64); err != nil {
				return ErrInvalidUpdate
			}
			j--
		}
	}
	return nil
}
