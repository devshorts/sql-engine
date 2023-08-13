package input

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

func TestReadsPipe(t *testing.T) {
	data := []byte(`{ "foo": 1 }
{"foo": 2 }`)

	reader := bufio.NewReader(bytes.NewReader(data))

	result, err := NewStdinReader().Parse(reader)

	if err != nil {
		t.Error(err)
	}

	expect := []DataRow{
		{"foo": 1},
		{"foo": 2},
	}

	if !reflect.DeepEqual(result, expect) {
		t.Failed()
	}
}
