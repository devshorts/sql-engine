package sql

import (
	"reflect"
	"testing"
)

func TestParses(t *testing.T) {
	result, err := Parse("select foo, bar, biz where x = 2")
	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result.fields, []string{"foo", "bar", "biz"}) {
		t.Fail()
	}

	if result.group.operator != And {
		t.Fail()
	}

	if result.group.predicate[0].leaf.compare != Eq {
		t.Fail()
	}

	if result.group.predicate[0].leaf.value != "2" {
		t.Fail()
	}

	if result.group.predicate[0].leaf.field != "x" {
		t.Fail()
	}
}
