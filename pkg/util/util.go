package util

import (
	"encoding/json"
	"testing"
)

func Every[T any](s []T, comp func(T) (bool, error)) (bool, error) {
	exists := true
	for _, data := range s {
		if valid, err := comp(data); err == nil {
			exists = valid && exists
		} else {
			return false, err
		}
	}

	return exists, nil
}

func Some[T any](s []T, comp func(T) (bool, error)) (bool, error) {
	exists := false
	for _, data := range s {
		if valid, err := comp(data); err == nil {
			exists = valid || exists
		} else {
			return false, err
		}
	}

	return exists, nil
}

type expect struct {
	t *testing.T
}

func (e *expect) JsonEquals(data any) string {
	indent, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		e.t.Fatalf(`unable to json serialize data %s`, data)
	}

	return string(indent)
}

func Expect(t *testing.T) *expect {
	return &expect{t: t}
}
