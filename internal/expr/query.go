package expr

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func (qe *QueryExpr) QueryArgs() []any {
	return qe.args
}

func (qe *QueryExpr) SQL() string {
	inCount := 0
	outCount := 0
	sql := bytes.Buffer{}
	for _, pp := range *qe.pe {
		switch pp := pp.(type) {
		case *preparedInput:
			sql.WriteString("@sqlair_" + strconv.Itoa(inCount))
			inCount++
		case *preparedOutput:
			for i, oc := range pp.outputColumns {
				sql.WriteString(oc.sql)
				sql.WriteString(" AS ")
				sql.WriteString(markerName(outCount))
				if i != len(pp.outputColumns)-1 {
					sql.WriteString(", ")
				}
				outCount++
			}
		case *preparedBypass:
			sql.WriteString(pp.chunk)
		}
	}
	return sql.String()
}

func (qe *QueryExpr) HasOutputs() bool {
	return len(qe.outputs) > 0
}

// QueryExpr represents a complete SQLair query, ready for execution on a
// database.
type QueryExpr struct {
	pe      *PreparedExpr
	args    []any
	outputs []typeMember
}

const markerPrefix = "_sqlair_"

func markerName(n int) string {
	return markerPrefix + strconv.Itoa(n)
}

// markerIndex returns the int X from the string "_sqlair_X".
func markerIndex(s string) (int, bool) {
	if strings.HasPrefix(s, markerPrefix) {
		n, err := strconv.Atoi(s[len(markerPrefix):])
		if err == nil {
			return n, true
		}
	}
	return 0, false
}

// Query returns a query expression ready for execution on a database. Query
// generates the SQL and extracts the query parameters from the provided
// arguments.
func (pe *PreparedExpr) Query(args ...any) (qe *QueryExpr, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("invalid input parameter: %s", err)
		}
	}()

	var typeValue = make(map[reflect.Type]reflect.Value)
	var typeNames []string
	for _, arg := range args {
		v := reflect.ValueOf(arg)
		if v.Kind() == reflect.Invalid || (v.Kind() == reflect.Pointer && v.IsNil()) {
			return nil, fmt.Errorf("need struct or map, got nil")
		}
		v = reflect.Indirect(v)
		t := v.Type()
		if v.Kind() != reflect.Struct && v.Kind() != reflect.Map {
			return nil, fmt.Errorf("need struct or map, got %s", t.Kind())
		}
		if _, ok := typeValue[t]; ok {
			return nil, fmt.Errorf("type %q provided more than once", t.Name())
		}
		typeValue[t] = v
		typeNames = append(typeNames, t.Name())
	}

	// Generate query parameters.
	qargs := make([]any, 0)
	outputs := make([]typeMember, 0)
	typeUsed := make(map[reflect.Type]bool)
	inCount := 0
	for _, pp := range *pe {
		switch pp := pp.(type) {
		case *preparedInput:
			// Find arg associated with input.
			typeMember := pp.input
			outerType := typeMember.outerType()
			v, ok := typeValue[outerType]
			if !ok {
				// Get the types of all args for checkShadowType.
				argTypes := make([]reflect.Type, 0, len(typeValue))
				for argType := range typeValue {
					argTypes = append(argTypes, argType)
				}
				if err := checkShadowedType(outerType, argTypes); err != nil {
					return nil, err
				}
				return nil, typeMissingError(outerType.Name(), typeNames)
			}
			typeUsed[outerType] = true

			// Retrieve query parameter.
			var val reflect.Value
			switch tm := typeMember.(type) {
			case *structField:
				val = v.Field(tm.index)
			case *mapKey:
				val = v.MapIndex(reflect.ValueOf(tm.name))
				if val.Kind() == reflect.Invalid {
					return nil, fmt.Errorf(`map %q does not contain key %q`, outerType.Name(), tm.name)
				}
			}
			qargs = append(qargs, sql.Named("sqlair_"+strconv.Itoa(inCount), val.Interface()))
			inCount++
		case *preparedOutput:
			for _, oc := range pp.outputColumns {
				outputs = append(outputs, oc.tm)
			}
		case *preparedBypass:
		default:
			return nil, fmt.Errorf("internal error: unknown part type %T", pp)
		}
	}

	for argType := range typeValue {
		if !typeUsed[argType] {
			return nil, fmt.Errorf("%s not referenced in query", argType.Name())
		}
	}

	return &QueryExpr{pe: pe, outputs: outputs, args: qargs}, nil
}

// checkShadowedType returns an error if a query type and some argument type
// have the same name but are from a different package.
func checkShadowedType(queryType reflect.Type, argTypes []reflect.Type) error {
	for _, argType := range argTypes {
		if argType.Name() == queryType.Name() {
			return fmt.Errorf("parameter with type %q missing, have type with same name: %q", queryType.String(), argType.String())
		}
	}
	return nil
}

var scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// ScanArgs returns list of pointers to the struct fields that are listed in qe.outputs.
// All the structs and maps mentioned in the query must be in outputArgs.
func (qe *QueryExpr) ScanArgs(columns []string, outputArgs []any) (scanArgs []any, onSuccess func(), err error) {
	var typesInQuery = []string{}
	var inQuery = make(map[reflect.Type]bool)
	for _, typeMember := range qe.outputs {
		outerType := typeMember.outerType()
		if ok := inQuery[outerType]; !ok {
			inQuery[outerType] = true
			typesInQuery = append(typesInQuery, outerType.Name())
		}
	}

	type scanProxy struct {
		original reflect.Value
		scan     reflect.Value
		key      reflect.Value
	}
	var scanProxies []scanProxy

	var typeDest = make(map[reflect.Type]reflect.Value)
	outputVals := []reflect.Value{}
	for _, outputArg := range outputArgs {
		if outputArg == nil {
			return nil, nil, fmt.Errorf("need map or pointer to struct, got nil")
		}
		outputVal := reflect.ValueOf(outputArg)
		k := outputVal.Kind()
		if k != reflect.Map {
			if k != reflect.Pointer {
				return nil, nil, fmt.Errorf("need map or pointer to struct, got %s", k)
			}
			if outputVal.IsNil() {
				return nil, nil, fmt.Errorf("got nil pointer")
			}
			outputVal = outputVal.Elem()
			k = outputVal.Kind()
			if k != reflect.Struct && k != reflect.Map {
				return nil, nil, fmt.Errorf("need map or pointer to struct, got pointer to %s", k)
			}
		}
		if !inQuery[outputVal.Type()] {
			return nil, nil, fmt.Errorf("type %q does not appear in query, have: %s", outputVal.Type().Name(), strings.Join(typesInQuery, ", "))
		}
		if _, ok := typeDest[outputVal.Type()]; ok {
			return nil, nil, fmt.Errorf("type %q provided more than once, rename one of them", outputVal.Type().Name())
		}
		typeDest[outputVal.Type()] = outputVal
		outputVals = append(outputVals, outputVal)
	}

	// Generate the pointers.
	var ptrs = []any{}
	for _, column := range columns {
		idx, ok := markerIndex(column)
		if !ok {
			// Columns not mentioned in output expressions are scanned into x.
			var x any
			ptrs = append(ptrs, &x)
			continue
		}
		if idx >= len(qe.outputs) {
			return nil, nil, fmt.Errorf("internal error: sqlair column not in outputs (%d>=%d)", idx, len(qe.outputs))
		}
		typeMember := qe.outputs[idx]
		outputVal, ok := typeDest[typeMember.outerType()]
		if !ok {
			return nil, nil, fmt.Errorf("type %q found in query but not passed to get", typeMember.outerType().Name())
		}
		switch tm := typeMember.(type) {
		case *structField:
			val := outputVal.Field(tm.index)
			if !val.CanSet() {
				return nil, nil, fmt.Errorf("internal error: cannot set field %s of struct %s", tm.name, tm.structType.Name())
			}
			pt := reflect.PointerTo(val.Type())
			if val.Type().Kind() != reflect.Pointer && !pt.Implements(scannerInterface) {
				// Rows.Scan will return an error if it tries to scan NULL into a type that cannot be set to nil.
				// For types that are not a pointer and do not implement sql.Scanner a pointer to them is generated
				// and passed to Rows.Scan. If Scan has set this pointer to nil the value is zeroed.
				scanVal := reflect.New(pt).Elem()
				ptrs = append(ptrs, scanVal.Addr().Interface())
				scanProxies = append(scanProxies, scanProxy{original: val, scan: scanVal})
			} else {
				ptrs = append(ptrs, val.Addr().Interface())
			}
		case *mapKey:
			scanVal := reflect.New(tm.mapType.Elem()).Elem()
			ptrs = append(ptrs, scanVal.Addr().Interface())
			scanProxies = append(scanProxies, scanProxy{original: outputVal, scan: scanVal, key: reflect.ValueOf(tm.name)})
		}
	}

	onSuccess = func() {
		for _, sp := range scanProxies {
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
	}

	return ptrs, onSuccess, nil
}
