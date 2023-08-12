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
	Eq  ComparisonOperator = "eq"
	Neq                    = "neq"
	Gt                     = "gt"
	Lt                     = "lt"
	Gte                    = "gte"
	Lte                    = "lte"
	In                     = "in"
)

type Leaf struct {
	field   string
	compare ComparisonOperator
	value   interface{}
}

type Tree struct {
	leaf  *Leaf
	group *PredicateGroup
}

func NewLeaf(data Leaf) Tree {
	return Tree{leaf: &data}
}

func NewGroup(data PredicateGroup) Tree {
	return Tree{group: &data}
}

// (x = 1 OR y = 2) and (y = 3)
type PredicateGroup struct {
	operator  GroupingOperator
	predicate []Tree
}

// select foo where ...
type Query struct {
	fields []string
	group  *PredicateGroup
}

func sliceCompare[T comparable](source []T, value T, op ComparisonOperator) (bool, error) {
	switch op {
	case In:
		return slices.Contains(source, value), nil
	default:
		return false, errors.New("Cannot process arrays without in clause")
	}
}

// A leaf comparison of the data row to know if it should be included in the final result or not
func compare(row input.DataRow, predicate *Leaf) (bool, error) {
	value := row[predicate.field]

	if value == nil && predicate.value != nil {
		return false, nil
	}

	slog.Info("Processing predicate",
		"predicate-value", fmt.Sprintf("%s", reflect.TypeOf(predicate.value)),
		"value", fmt.Sprintf("%s", reflect.TypeOf(value)),
	)

	if reflect.TypeOf(predicate.value).Kind() == reflect.Slice {
		// in clause if the predicate is an array
		switch in := predicate.value.(type) {
		case []string:
			return sliceCompare(in, value.(string), predicate.compare)
		case []int:
			return sliceCompare(in, value.(int), predicate.compare)
		case []float64:
			return sliceCompare(in, value.(float64), predicate.compare)
		case []interface{}:
			return sliceCompare(in, value, predicate.compare)
		}

		return false, errors.New("unsupported array type")
	}

	var result int
	switch casted := value.(type) {
	case string:
		result = cmp.Compare(casted, predicate.value.(string))
	case int:
		result = cmp.Compare(casted, predicate.value.(int))
	case float64:
		result = cmp.Compare(casted, predicate.value.(float64))
	default:
		return false, errors.New(fmt.Sprintf("unsupported type %s", reflect.TypeOf(value)))
	}

	switch predicate.compare {
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

	return false, errors.New("invalid predicate")
}

func inPredicateGroup(row input.DataRow, group *PredicateGroup) (bool, error) {
	exists := func(predicate Tree) (bool, error) {
		if predicate.leaf != nil {
			return compare(row, predicate.leaf)
		}

		if predicate.group != nil {
			return inPredicateGroup(row, predicate.group)
		}

		return false, nil
	}

	switch group.operator {
	case "":
		fallthrough
	case And:
		return util.Every(group.predicate, exists)
	case Or:
		return util.Some(group.predicate, exists)
	}

	return false, errors.New("invalid predicate operator")
}

// extracts selected fields
func selectFields(row input.DataRow, sql Query) input.DataRow {
	selected := make(input.DataRow)

	for key := range row {
		if slices.Contains(sql.fields, key) {
			selected[key] = row[key]
		}
	}

	return selected
}

func QueryData(data []input.DataRow, sql Query) ([]input.DataRow, error) {
	var results []input.DataRow

	for _, row := range data {
		if exists, err := inPredicateGroup(row, sql.group); err == nil {
			if exists {
				selectedFields := selectFields(row, sql)

				results = append(results, selectedFields)
			}
		} else {
			return results, err
		}
	}

	return results, nil
}
