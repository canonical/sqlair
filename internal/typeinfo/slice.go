package typeinfo

import "reflect"

type slice struct {
	sliceType reflect.Type
}

func (s *slice) String() string {
	return s.sliceType.Name() + "[:]"
}

func (s *slice) ArgType() reflect.Type {
	return s.sliceType
}

func (s *slice) LocateParams(typeToValue map[reflect.Type]reflect.Value) ([]reflect.Value, error) {
	sv, ok := typeToValue[s.sliceType]
	if !ok {
		return nil, valueNotFoundError(typeToValue, s.sliceType)
	}

	params := []reflect.Value{}
	for i := 0; i < sv.Len(); i++ {
		params = append(params, sv.Index(i))
	}
	return params, nil
}
