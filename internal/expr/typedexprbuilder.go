// Copyright 2024 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package expr

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/canonical/sqlair/internal/typeinfo"
)

// typedExprBuild is used to build up typed expressions.
type typedExprBuilder struct {
	argInfo    map[string]typeinfo.ArgInfo
	argUsed    map[typeinfo.ArgInfo]bool
	outputUsed map[string]bool
	typedExprs []typedExpr
}

func newTypedExprBuilder(argInfo map[string]typeinfo.ArgInfo) *typedExprBuilder {
	return &typedExprBuilder{
		argInfo:    argInfo,
		argUsed:    map[typeinfo.ArgInfo]bool{},
		outputUsed: map[string]bool{},
	}
}

// InputMember returns an input locator for a member of a struct or map.
func (teb *typedExprBuilder) InputMember(typeName string, memberName string) (typeinfo.Input, error) {
	arg, err := teb.getArg(typeName)
	if err != nil {
		return nil, err
	}
	vl, err := arg.GetMember(memberName)
	if err != nil {
		return nil, err
	}
	input, ok := vl.(typeinfo.Input)
	if !ok {
		return nil, fmt.Errorf("%s cannot be used as input", vl.ArgType().Kind())
	}
	return input, nil
}

// OutputMember returns an output locator for a member of a struct or map.
func (teb *typedExprBuilder) OutputMember(typeName string, memberName string) (typeinfo.Output, error) {
	arg, err := teb.getArg(typeName)
	if err != nil {
		return nil, err
	}
	vl, err := arg.GetMember(memberName)
	if err != nil {
		return nil, err
	}
	output, ok := vl.(typeinfo.Output)
	if !ok {
		return nil, fmt.Errorf("%s cannot be used as output", vl.ArgType().Kind())
	}
	if _, ok := teb.outputUsed[output.Identifier()]; ok {
		return nil, usedInMultipleOutputsError(output.Desc())
	}
	teb.outputUsed[output.Identifier()] = true
	return output, nil
}

// AllStructInputs returns a list of inputs locators that locate every member
// of the named type along with the names of the members. If the type is not a
// struct an error is returned.
func (teb *typedExprBuilder) AllStructInputs(typeName string) ([]typeinfo.Input, []string, error) {
	arg, err := teb.getArg(typeName)
	if err != nil {
		return nil, nil, err
	}
	members, names, err := arg.GetAllStructMembers()
	if err != nil {
		return nil, nil, err
	}

	var inputs []typeinfo.Input
	for _, member := range members {
		input, ok := member.(typeinfo.Input)
		if !ok {
			return nil, nil, fmt.Errorf("%s cannot be used as input", member.ArgType().Kind())
		}
		inputs = append(inputs, input)
	}
	return inputs, names, nil
}

// AllStructOutputs returns a list of output locators that locate every member
// of the named type along with the names of the members. If the type is not a
// struct an error is returned.
func (teb *typedExprBuilder) AllStructOutputs(typeName string) ([]typeinfo.Output, []string, error) {
	arg, err := teb.getArg(typeName)
	if err != nil {
		return nil, nil, err
	}
	members, names, err := arg.GetAllStructMembers()
	if err != nil {
		return nil, nil, err
	}

	var outputs []typeinfo.Output
	for _, member := range members {
		output, ok := member.(typeinfo.Output)
		if !ok {
			return nil, nil, fmt.Errorf("%s cannot be used as output", member.ArgType().Kind())
		}
		if _, ok := teb.outputUsed[output.Identifier()]; ok {
			return nil, nil, usedInMultipleOutputsError(output.Desc())
		}
		teb.outputUsed[output.Identifier()] = true
		outputs = append(outputs, output)
	}

	return outputs, names, nil
}

// InputSlice returns an input locator for a slice.
func (teb *typedExprBuilder) InputSlice(typeName string) (typeinfo.Input, error) {
	arg, err := teb.getArg(typeName)
	if err != nil {
		return nil, err
	}
	sliceLocator, err := arg.GetSlice()
	if err != nil {
		return nil, err
	}
	input, ok := sliceLocator.(typeinfo.Input)
	if !ok {
		return nil, fmt.Errorf("%s cannot be used as input", sliceLocator.ArgType().Kind())
	}
	return input, nil
}

func (teb *typedExprBuilder) getArg(typeName string) (typeinfo.ArgInfo, error) {
	arg, ok := teb.argInfo[typeName]
	if !ok {
		return nil, nameNotFoundError(teb.argInfo, typeName)
	}
	teb.argUsed[arg] = true
	return arg, nil
}

// Kind looks up the type name and returns its kind.
func (teb *typedExprBuilder) Kind(typeName string) (reflect.Kind, error) {
	arg, ok := teb.argInfo[typeName]
	if !ok {
		return 0, nameNotFoundError(teb.argInfo, typeName)
	}
	return arg.Typ().Kind(), nil
}

// checkAllArgsUsed goes through all the arguments contained in typeToValue and
// checks that they were used when building the typed expression.
func (teb *typedExprBuilder) checkAllArgsUsed(typeToValue map[string]typeinfo.ArgInfo) error {
	for _, argInfo := range typeToValue {
		if !teb.argUsed[argInfo] {
			return fmt.Errorf("type %q not found in statement", argInfo.Typ().Name())
		}
	}
	return nil
}

// AddTypedInsertExpr wraps and adds the columns of an insert expression to the
// typed expressions.
func (teb *typedExprBuilder) AddTypedInsertExpr(insertColumns []typedColumn) {
	teb.typedExprs = append(teb.typedExprs, &typedInsertExpr{insertColumns: insertColumns})
}

// AddTypedInputExpr wrap and adds an input to the typed expressions.
func (teb *typedExprBuilder) AddTypedInputExpr(input typeinfo.Input) {
	teb.typedExprs = append(teb.typedExprs, &typedInputExpr{input})
}

// AddTypedOutputExpr wraps and adds output columns to the typed expressions.
func (teb *typedExprBuilder) AddTypedOutputExpr(outputColumns []outputColumn) {
	teb.typedExprs = append(teb.typedExprs, &typedOutputExpr{outputColumns: outputColumns})
}

// AddBypass adds a bypass part to the typed expressions
func (teb *typedExprBuilder) AddBypass(b *bypass) {
	teb.typedExprs = append(teb.typedExprs, b)
}

// nameNotFoundError generates the arguments present and returns a missing type
// error.
func nameNotFoundError(argInfo map[string]typeinfo.ArgInfo, missingTypeName string) error {
	// Get names of the arguments we have from the ArgInfo keys.
	var argNames []string
	for argName := range argInfo {
		argNames = append(argNames, argName)
	}
	// Sort for consistent error messages.
	sort.Strings(argNames)
	return typeinfo.TypeMissingError(missingTypeName, argNames)
}

func usedInMultipleOutputsError(desc string) error {
	// This error message is appended the expression the second usage was
	// found in.
	return fmt.Errorf("%s is used in multiple output expressions including", desc)
}
