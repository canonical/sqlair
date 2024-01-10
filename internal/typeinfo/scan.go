// Copyright 2023 Canonical Ltd.
// Licensed under Apache 2.0, see LICENCE file for details.

package typeinfo

import "reflect"

// ScanProxy is a shim for scanning query results
// into struct fields or map keys.
type ScanProxy struct {
	// original is the reflected value of a map or struct field
	// into which we want to scan SQL query results.
	original reflect.Value

	// scan is the reflected value that we populated by rows.Scan
	scan reflect.Value

	// key when valid indicates that this proxy is
	// for a key in the map indicated by original.
	key reflect.Value
}

// OnSuccess is run after using rows.Scan to read a single query column
// return into the variable referenced by the scan member.
// When the ScanProxy is for a map key, we set the map's value for the key.
// When the proxy is for a struct field, we set that field.
func (sp ScanProxy) OnSuccess() {
	if sp.key.IsValid() {
		sp.original.SetMapIndex(sp.key, sp.scan)
	} else {
		var val reflect.Value
		if !sp.scan.IsNil() {
			val = sp.scan.Elem()
		} else {
			val = reflect.Zero(sp.original.Type())
		}
		sp.original.Set(val)
	}
}
