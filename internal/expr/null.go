package expr

import (
	"reflect"

	"github.com/canonical/sqlair/internal/convert"
)

type NullT struct {
	Value reflect.Value
	Valid bool
}

func (nt *NullT) Scan(src any) error {
	if src == nil {
		nt.Valid = false
		return nil
	}
	nt.Valid = true
	return convert.ConvertAssign(nt.Value.Addr().Interface(), src)
}
