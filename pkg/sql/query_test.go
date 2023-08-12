package sql

import (
	"example/pkg/input"
	"reflect"
	"testing"
)

func TestQueries(t *testing.T) {
	var sample = Query{
		fields: []string{"foo"},
		group: &PredicateGroup{predicate: []Tree{
			NewLeaf(Leaf{value: "1", compare: Eq, field: "foo"}),
		}},
	}

	var data = []input.DataRow{
		{"foo": "1", "bar": "2"},
		{"foo": "1", "bar": "3"},
		{"foo": "2"},
	}

	result, err := QueryData(data, sample)

	if err != nil {
		t.Fail()
	}

	if reflect.DeepEqual(result, []input.DataRow{
		{"foo": "1"},
		{"foo": "2"},
	}) {
		t.Fail()
	}
}

func TestCompoundQueries(t *testing.T) {
	var sample = Query{
		fields: []string{"foo"},
		group: &PredicateGroup{
			operator: Or,
			predicate: []Tree{
				NewLeaf(Leaf{value: "1", compare: Eq, field: "foo"}),
				NewLeaf(Leaf{value: "3", compare: Eq, field: "bar"}),
			}},
	}

	var data = []input.DataRow{
		{"foo": "1", "bar": "2"},
		{"foo": "2", "bar": "3"},
		{"foo": "3"},
	}

	result, err := QueryData(data, sample)

	if err != nil {
		t.Logf("%s", err)
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"foo": "1"},
		{"foo": "2"},
	}) {
		t.Logf("%s", result)
		t.Fail()
	}
}

func TestCompoundTreeQueries(t *testing.T) {
	// foo = 1 or bar = 3 or (baz = 5 and foo=5
	var sample = Query{
		fields: []string{"foo", "id"},
		group: &PredicateGroup{
			operator: Or,
			predicate: []Tree{
				NewLeaf(Leaf{value: "1", compare: Eq, field: "foo"}),
				NewLeaf(Leaf{value: "3", compare: Eq, field: "bar"}),
				NewGroup(PredicateGroup{
					operator: And,
					predicate: []Tree{
						NewLeaf(Leaf{value: 5, compare: Eq, field: "baz"}),
						NewLeaf(Leaf{value: "5", compare: Eq, field: "foo"}),
					},
				}),
			}},
	}

	var data = []input.DataRow{
		{"foo": "1", "bar": "2"},
		{"foo": "2", "bar": "3"},
		{"foo": "3"},
		{"foo": "4", "baz": 5},
		{"foo": "5", "baz": 5, "id": 1},
		{"foo": "5", "baz": 5, "id": 2},
	}

	result, err := QueryData(data, sample)

	if err != nil {
		t.Logf("%s", err)
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"foo": "1"},
		{"foo": "2"},
		{"foo": "5", "id": 1},
		{"foo": "5", "id": 2},
	}) {
		t.Logf("%s", result)
		t.Fail()
	}
}

func TestInClause(t *testing.T) {
	var sample = Query{
		fields: []string{"foo"},
		group: &PredicateGroup{predicate: []Tree{
			NewLeaf(Leaf{value: []string{"1"}, compare: In, field: "foo"}),
		}},
	}

	var data = []input.DataRow{
		{"foo": "1", "bar": "2"},
		{"foo": "1", "bar": "3"},
		{"foo": "2"},
	}

	result, err := QueryData(data, sample)

	if err != nil {
		t.Fail()
	}

	if reflect.DeepEqual(result, []input.DataRow{
		{"foo": "1"},
		{"foo": "2"},
	}) {
		t.Fail()
	}
}
