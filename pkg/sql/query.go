package sql

import (
	"cmp"
	"errors"
	"example/pkg/input"
	"example/pkg/util"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
)

type GroupingOperator string

const (
	And GroupingOperator = "and"
	Or                   = "or"
)

type ComparisonOperator string

const (
	Eq  ComparisonOperator = "="
	Neq                    = "!="
	Gt                     = ">"
	Lt                     = "<"
	Gte                    = ">="
	Lte                    = "<="
	In                     = "in"
)

type Leaf struct {
	Field   string
	Compare ComparisonOperator
	Value   interface{}
}

type Tree struct {
	Leaf  *Leaf           `json:",omitempty"`
	Group *PredicateGroup `json:",omitempty"`
}

func NewLeaf(data Leaf) Tree {
	return Tree{Leaf: &data}
}

func NewGroup(data *PredicateGroup) Tree {
	// special case a Group of 1 to be a Leaf
	if len(data.Predicate) == 1 {
		return data.Predicate[0]
	}

	return Tree{Group: data}
}

// (x = 1 OR y = 2) and (y = 3)
type PredicateGroup struct {
	Operator  GroupingOperator
	Predicate []Tree
}

type Function = string

const (
	Average Function = "average"
	Max              = "max"
	Min              = "min"
)

type KeyAlias string

type Field struct {
	name     string
	alias    KeyAlias
	function *Function
}

// select foo where ...
type Query struct {
	Fields []Field
	Group  *PredicateGroup `json:",omitempty"`
}

type Executor struct {
	sql Query
}

func sliceCompare[T comparable](source []T, value T, op ComparisonOperator) (bool, error) {
	switch op {
	case In:
		return slices.Contains(source, value), nil
	default:
		return false, errors.New("Cannot process arrays without in clause")
	}
}

// A Leaf comparison of the data row to know if it should be included in the final result or not
func (s *Executor) compare(predicate *Leaf, value interface{}) (bool, error) {
	if value == nil && predicate.Value != nil {
		return false, nil
	}

	slog.Debug("Processing Predicate",
		"Predicate-Value", fmt.Sprintf("%s", reflect.TypeOf(predicate.Value)),
		"Value", fmt.Sprintf("%s", reflect.TypeOf(value)),
	)

	if reflect.TypeOf(predicate.Value).Kind() == reflect.Slice {
		// in clause if the Predicate is an array
		switch in := predicate.Value.(type) {
		case []string:
			return sliceCompare(in, value.(string), predicate.Compare)
		case []int:
			return sliceCompare(in, value.(int), predicate.Compare)
		case []float64:
			return sliceCompare(in, value.(float64), predicate.Compare)
		case []interface{}:
			return sliceCompare(in, value, predicate.Compare)
		}

		return false, errors.New("unsupported array type")
	}

	var result int
	switch casted := value.(type) {
	case string:
		result = cmp.Compare(casted, predicate.Value.(string))
	case int:
		result = cmp.Compare(casted, predicate.Value.(int))
	case float64:
		result = cmp.Compare(casted, predicate.Value.(float64))
	default:
		return false, errors.New(fmt.Sprintf("unsupported type %s", reflect.TypeOf(value)))
	}

	switch predicate.Compare {
	case Neq:
		return result != 0, nil
	case Eq:
		return result == 0, nil
	case Gt:
		return result > 1, nil
	case Gte:
		return result > 1 || result == 0, nil
	case Lte:
		return result < 1 || result == 0, nil
	case Lt:
		return result < 1, nil
	}

	return false, errors.New("invalid Predicate")
}

func (s *Executor) inPredicateGroup(row input.DataRow, group *PredicateGroup) (bool, error) {
	// no Predicate, just select everything
	if group == nil {
		return true, nil
	}

	exists := func(predicate Tree) (bool, error) {
		if predicate.Leaf != nil {
			value := row[keyNameFromAlias(predicate.Leaf.Field, s.sql)]

			return s.compare(predicate.Leaf, value)
		}

		if predicate.Group != nil {
			return s.inPredicateGroup(row, predicate.Group)
		}

		return false, nil
	}

	switch group.Operator {
	case "":
		fallthrough
	case And:
		return util.Every(group.Predicate, exists)
	case Or:
		return util.Some(group.Predicate, exists)
	}

	return false, errors.New("invalid Predicate Operator")
}

// extracts selected Fields
func selectFields(row input.DataRow, sql Query) input.DataRow {
	selected := make(input.DataRow)

	allFieldNames := FieldNames(sql)

	for key := range row {
		if slices.Contains(allFieldNames, key) || slices.Contains(allFieldNames, "*") {
			selected[string(keyAliasFromName(key, sql))] = row[key]
		}
	}

	return selected
}

func keyAliasFromName(key string, sql Query) KeyAlias {
	for _, field := range sql.Fields {
		if field.name == key {
			if field.alias != "" {
				return field.alias
			}

			return KeyAlias(field.name)
		}
	}

	return KeyAlias(key)
}

func keyNameFromAlias(alias string, sql Query) string {
	for _, field := range sql.Fields {
		if field.alias == KeyAlias(alias) {
			return field.name
		}
	}

	return ""
}

func FieldNames(sql Query) []string {
	var names []string

	for _, field := range sql.Fields {
		names = append(names, field.name)
	}

	return names
}

func (s *Executor) QueryData(data []input.DataRow) ([]input.DataRow, error) {
	var results []input.DataRow

	for _, row := range data {
		if exists, err := s.inPredicateGroup(row, s.sql.Group); err == nil {
			if exists {
				selectedFields := selectFields(row, s.sql)

				results = append(results, selectedFields)
			}
		} else {
			return results, err
		}
	}

	return results, nil
}

func NewExecutor(sql Query) *Executor {
	return &Executor{
		sql: sql,
	}
}
