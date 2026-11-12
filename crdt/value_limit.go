package crdt

import "fmt"

// ErrValueLimit is why DocContainsUpdateV1Limit, DiffUpdateV1Limit and
// HeldStructsDifferV1Limit stop decoding an update: it carries more values
// than the limit their caller passed. It wraps ErrInvalidUpdate.
var ErrValueLimit = fmt.Errorf("%w: it carries more values than the decode allows", ErrInvalidUpdate)

// limitedScratch is the document a decode outside an apply decodes an update
// into, counting what it decodes against values as an apply's tally counts
// against ApplyBudget.Decoded: each struct, each delete range, each lib0 Any
// value at any depth and each JSON value. A limit below zero counts nothing.
func limitedScratch(values int) *Doc {
	doc := New()
	if values >= 0 {
		doc.applying = &applyTally{budget: ApplyBudget{Decoded: values, Structs: -1, Bytes: -1, Split: -1}}
	}
	return doc
}

// decodeLimitedV1 is decodeStructsV1 on a scratch document whose decode stops
// with ErrValueLimit past values (limitedScratch). The count is checked where
// each value is read, before what it would allocate is taken, by the panic
// the apply's tally raises (applyTally.decode), recovered here.
func decodeLimitedV1(update []byte, values int) (structs map[ClientID][]*Item, ds DeleteSet, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, over := r.(applyBudgetExceeded); !over {
				panic(r)
			}
			structs, ds, err = nil, DeleteSet{}, ErrValueLimit
		}
	}()
	return decodeStructsV1(limitedScratch(values), update)
}
