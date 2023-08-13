package sql

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestParses(t *testing.T) {
	result, err := Parse("select foo, bar, biz where (x = 2)")
	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result.Fields, []string{"foo", "bar", "biz"}) {
		t.Fail()
	}

	if result.Group.Operator != And {
		t.Fail()
	}

	if result.Group.Predicate[0].Leaf.Compare != Eq {
		t.Fail()
	}

	if result.Group.Predicate[0].Leaf.Value != "2" {
		t.Fail()
	}

	if result.Group.Predicate[0].Leaf.Field != "x" {
		t.Fail()
	}
}

func TestParsesWithOperator(t *testing.T) {
	result, err := Parse("select foo, bar, biz where x = 2 and y = 3")
	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result.Fields, []string{"foo", "bar", "biz"}) {
		t.Fail()
	}

	if result.Group.Operator != And {
		t.Fail()
	}

	if result.Group.Predicate[0].Leaf.Field != "x" {
		t.Fail()
	}

	if result.Group.Predicate[0].Leaf.Compare != Eq {
		t.Fail()
	}

	if result.Group.Predicate[0].Leaf.Value != "2" {
		t.Fail()
	}

	if result.Group.Predicate[1].Leaf.Field != "y" {
		t.Fail()
	}

	if result.Group.Predicate[1].Leaf.Compare != Eq {
		t.Fail()
	}

	if result.Group.Predicate[1].Leaf.Value != "3" {
		t.Fail()
	}
}

func TestParsesWithOperatorGrouping(t *testing.T) {
	result, err := Parse("select foo, bar, biz where (x = 2 and y = 3) or foo = 1")
	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result.Fields, []string{"foo", "bar", "biz"}) {
		t.Fail()
	}

	query := toJson(Query{
		Fields: []string{"foo", "bar", "biz"},
		Group: &PredicateGroup{
			Operator: Or,
			Predicate: []Tree{
				NewGroup(&PredicateGroup{
					Operator: And,
					Predicate: []Tree{
						NewLeaf(Leaf{Field: "x", Compare: Eq, Value: "2"}),
						NewLeaf(Leaf{Field: "y", Compare: Eq, Value: "3"}),
					},
				}),
				NewLeaf(Leaf{Field: "foo", Compare: Eq, Value: "1"}),
			}},
	}, t)

	rJson := toJson(result, t)

	if query != rJson || rJson == "{}" {
		t.Fail()
	}
}

func toJson[T any](data T, t *testing.T) string {
	rJson, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Errorf(`%s`, err)
		t.Fail()
	}
	return string(rJson)
}