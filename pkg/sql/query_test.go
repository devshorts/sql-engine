package sql

import (
	"example/pkg/input"
	"reflect"
	"testing"
)

func TestQueries(t *testing.T) {
	var sql = Query{
		Fields: []Field{{Name: "foo"}},
		Group: &PredicateGroup{Predicate: []Tree{
			NewLeaf(Leaf{Value: float64(1), Compare: Eq, Field: "foo"}),
		}},
	}

	var data = []input.DataRow{
		{"foo": 1, "bar": "2"},
		{"foo": 1, "bar": "3"},
		{"foo": 2},
	}

	result, err := NewExecutor(sql).QueryData(data)

	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"foo": 1},
		{"foo": 1},
	}) {
		t.Fail()
	}
}

func TestQueriesAverage(t *testing.T) {
	var sql = Query{
		Fields: []Field{{Name: "foo", Alias: "avg", Function: Average}, {Name: "bar", Alias: "bar"}},
		Group: &PredicateGroup{Predicate: []Tree{
			NewLeaf(Leaf{Value: float64(0), Compare: Gt, Field: "foo"}),
		}},
	}

	var data = []input.DataRow{
		{"foo": 2, "bar": "2"},
		{"foo": 2, "bar": "3"},
		{"foo": 2},
		{"foo": -2099},
		{"foo": 2},
	}

	result, err := NewExecutor(sql).QueryData(data)

	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"avg": float64(2), "bar": "2"},
		{"avg": float64(2), "bar": "3"},
		{"avg": float64(2)},
		{"avg": float64(2)},
	}) {
		t.Fail()
	}
}

func TestQueriesWithAlias(t *testing.T) {
	var sql = Query{
		Fields: []Field{{Name: "foo", Alias: "newfoo"}},
		Group: &PredicateGroup{Predicate: []Tree{
			NewLeaf(Leaf{Value: float64(1), Compare: Eq, Field: "newfoo"}),
		}},
	}

	var data = []input.DataRow{
		{"foo": float64(1), "bar": "2"},
		{"foo": float64(1), "bar": "3"},
		{"foo": float64(2)},
	}

	result, err := NewExecutor(sql).QueryData(data)

	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"newfoo": float64(1)},
		{"newfoo": float64(1)},
	}) {
		t.Fail()
	}
}

func TestQueriesStar(t *testing.T) {
	var sql = Query{
		Fields: []Field{{Name: "*"}},
		Group: &PredicateGroup{Predicate: []Tree{
			NewLeaf(Leaf{Value: float64(1), Compare: Eq, Field: "foo"}),
		}},
	}

	var data = []input.DataRow{
		{"foo": 1, "bar": "2"},
		{"foo": 1, "bar": "3"},
		{"foo": 2},
	}

	result, err := NewExecutor(sql).QueryData(data)

	if err != nil {
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"foo": 1, "bar": "2"},
		{"foo": 1, "bar": "3"},
	}) {
		t.Fail()
	}
}

func TestCompoundQueries(t *testing.T) {
	var sql = Query{
		Fields: []Field{{Name: "foo"}},
		Group: &PredicateGroup{
			Operator: Or,
			Predicate: []Tree{
				NewLeaf(Leaf{Value: float64(1), Compare: Eq, Field: "foo"}),
				NewLeaf(Leaf{Value: float64(3), Compare: Eq, Field: "bar"}),
			}},
	}

	var data = []input.DataRow{
		{"foo": 1, "bar": "2"},
		{"foo": 2, "bar": "3"},
		{"foo": 3},
	}

	result, err := NewExecutor(sql).QueryData(data)

	if err != nil {
		t.Logf("%s", err)
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"foo": 1},
		{"foo": 2},
	}) {
		t.Logf("%s", result)
		t.Fail()
	}
}

func TestCompoundTreeQueries(t *testing.T) {
	// foo = 1 or bar = 3 or (baz = 5 and foo=5
	var sql = Query{
		Fields: []Field{{Name: "foo"}, {Name: "id"}},
		Group: &PredicateGroup{
			Operator: Or,
			Predicate: []Tree{
				NewLeaf(Leaf{Value: float64(1), Compare: Eq, Field: "foo"}),
				NewLeaf(Leaf{Value: float64(3), Compare: Eq, Field: "bar"}),
				NewGroup(&PredicateGroup{
					Operator: And,
					Predicate: []Tree{
						NewLeaf(Leaf{Value: float64(5), Compare: Eq, Field: "baz"}),
						NewLeaf(Leaf{Value: float64(5), Compare: Eq, Field: "foo"}),
					},
				}),
			}},
	}

	var data = []input.DataRow{
		{"foo": float64(1), "bar": "2"},
		{"foo": float64(2), "bar": "3"},
		{"foo": float64(3)},
		{"foo": float64(4), "baz": 5},
		{"foo": float64(5), "baz": 5, "id": 1},
		{"foo": float64(5), "baz": 5, "id": 2},
	}

	result, err := NewExecutor(sql).QueryData(data)

	if err != nil {
		t.Logf("%s", err)
		t.Fail()
	}

	if !reflect.DeepEqual(result, []input.DataRow{
		{"foo": float64(1)},
		{"foo": float64(2)},
		{"foo": float64(5), "id": 1},
		{"foo": float64(5), "id": 2},
	}) {
		t.Logf("%s", result)
		t.Fail()
	}
}

func TestInClause(t *testing.T) {
	var sql = Query{
		Fields: []Field{{Name: "foo"}},
		Group: &PredicateGroup{Predicate: []Tree{
			NewLeaf(Leaf{Value: []float64{1}, Compare: In, Field: "foo"}),
		}},
	}

	var data = []input.DataRow{
		{"foo": "1", "bar": "2"},
		{"foo": "1", "bar": "3"},
		{"foo": "2"},
	}

	result, err := NewExecutor(sql).QueryData(data)

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
