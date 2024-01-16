// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package expr

import (
	"fmt"
	"reflect"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// PrimedQuery contains all concrete values needed to run a SQLair query on a
// database.
type PrimedQuery struct {
	sql string
	// params are the query parameters to pass to the database.
	params []any
	// outputs specifies where to scan the query results.
	outputs []typeinfo.Output
}

// Params returns the query parameters to pass with the SQL to a database.
func (pq *PrimedQuery) Params() []any {
	return pq.params
}

// HasOutputs returns true if the SQLair query contains at least one output
// expression.
func (pq *PrimedQuery) HasOutputs() bool {
	return len(pq.outputs) > 0
}

// SQL returns the SQL string to send to the database.
func (pq *PrimedQuery) SQL() string {
	return pq.sql
}

// ScanArgs produces a list of pointers to be passed to rows.Scan. After a
// successful call, the onSuccess function must be invoked. The outputArgs will
// be populated with the query results. All the structs/maps/slices mentioned in
// the query must be in outputArgs.
func (pq *PrimedQuery) ScanArgs(columnNames []string, outputArgs []any) (scanArgs []any, onSuccess func(), err error) {

	typeToValue, err := typeinfo.ValidateOutputs(outputArgs)
	if err != nil {
		return nil, nil, err
	}

	// Generate the pointers.
	var ptrs []any
	var scanProxies []typeinfo.ScanProxy
	var columnInResult = make([]bool, len(columnNames))
	argTypeUsed := map[reflect.Type]bool{}
	for _, column := range columnNames {
		idx, ok := markerIndex(column)
		if !ok {
			// Columns not mentioned in output expressions are scanned into x.
			var x any
			ptrs = append(ptrs, &x)
			continue
		}
		if idx >= len(pq.outputs) {
			return nil, nil, fmt.Errorf("internal error: sqlair column not in outputs (%d>=%d)", idx, len(pq.outputs))
		}
		columnInResult[idx] = true
		output := pq.outputs[idx]
		ptr, scanProxy, err := output.LocateScanTarget(typeToValue)
		if err != nil {
			return nil, nil, err
		}
		argTypeUsed[output.ArgType()] = true

		ptrs = append(ptrs, ptr)
		if scanProxy != nil {
			scanProxies = append(scanProxies, *scanProxy)
		}
	}

	for i := 0; i < len(pq.outputs); i++ {
		if !columnInResult[i] {
			return nil, nil, fmt.Errorf(`query uses "&%s" outside of result context`, pq.outputs[i].ArgType().Name())
		}
	}

	for argType := range typeToValue {
		if !argTypeUsed[argType] {
			return nil, nil, fmt.Errorf("%q not referenced in query", argType.Name())
		}
	}

	onSuccess = func() {
		for _, sp := range scanProxies {
			sp.OnSuccess()
		}
	}

	return ptrs, onSuccess, nil
}
