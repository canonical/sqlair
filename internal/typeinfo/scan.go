package typeinfo

import "reflect"

// ScanProxy is a shim for scanning query results
// into types for which we have information.
type ScanProxy struct {
	original reflect.Value
	scan     reflect.Value
	key      reflect.Value
}

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
