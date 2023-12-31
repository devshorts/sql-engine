package sql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	sel   = "select"
	where = "where"
)

func Parse(raw string) (*Query, error) {
	stream := NewStreamTokenizer(Lex(raw))

	fields, err := parseFields(stream)
	if err != nil {
		if errors.Is(err, eof) {
			return &Query{
				Fields: fields,
			}, nil
		}
		return nil, err
	}

	group, err := parseGroup(stream)
	if err != nil {
		return nil, err
	}

	return &Query{
		Fields: fields,
		Group:  group,
	}, nil
}

func parseGroup(stream *streamTokenizer) (*PredicateGroup, error) {
	// end of file, return empty Group
	token, err := stream.Peek()
	if errors.Is(err, eof) {
		return &PredicateGroup{
			Operator:  And,
			Predicate: []Tree{},
		}, nil
	}

	if err != nil {
		return nil, err
	}

	// we've reached the end of a Group, bubble out
	if token == ")" {
		return nil, nil
	}

	var predicates []Tree

	// open parenth, try priority Group
	if token == "(" {
		group, err := parenthesisGroup(stream, token)
		if err != nil {
			return nil, err
		}

		predicates = append(predicates, NewGroup(group))
	}

	// if an Operator exists grab it
	operator, err := nextOperator(stream, token, err)
	if err != nil && !errors.Is(err, eof) {
		return nil, err
	}

	// do the next Leaf
	leaf, err := parseLeaf(stream)
	if err != nil && !errors.Is(err, eof) {
		return nil, err
	}

	// if we didn't have an operator try again
	if operator == nil {
		operator, err = nextOperator(stream, token, err)
		if err != nil && !errors.Is(err, eof) {
			return nil, err
		}

		if operator == nil {
			var op GroupingOperator = "and"
			operator = &op
		}
	}

	if leaf != nil {
		// we have a leaf, append it to our list of current predicates
		predicates = append(predicates, NewLeaf(*leaf))
	}

	// if we're at eof return the tree we have
	_, err = stream.Peek()
	if errors.Is(err, eof) {
		return &PredicateGroup{
			Operator:  *operator,
			Predicate: predicates,
		}, nil
	}

	if err != nil {
		return nil, err
	}

	// get the next Group if one exists, otherwise we've reached the Leaf
	group, err := parseGroup(stream)
	if err != nil {
		return nil, err
	}

	if group != nil {
		predicates = append(predicates, NewGroup(group))
	}

	return &PredicateGroup{
		Operator:  *operator,
		Predicate: predicates,
	}, nil
}

func nextOperator(stream *streamTokenizer, token string, err error) (*GroupingOperator, error) {
	token, _ = stream.Peek()

	var operator GroupingOperator
	switch GroupingOperator(token) {
	case And:
		_, err := stream.Consume()
		if err != nil {
			return nil, err
		}

		operator = And
	case Or:
		_, err := stream.Consume()
		if err != nil {
			return nil, err
		}

		operator = Or
	default:
		return nil, err
	}

	return &operator, nil
}

func parenthesisGroup(stream *streamTokenizer, token string) (*PredicateGroup, error) {
	token, err := stream.Consume()
	if token != "(" {
		return nil, errors.New("missing open parenthesis")
	}

	if err != nil {
		return nil, err
	}

	group, err := parseGroup(stream)

	token, err = stream.Consume()
	if err != nil {
		return nil, err
	}

	if token != ")" {
		return nil, errors.New("missing closing bracket")
	}

	return group, nil
}

func parseLeaf(stream *streamTokenizer) (*Leaf, error) {
	field, err := stream.Consume()
	if err != nil {
		return nil, err
	}

	operator, err := stream.Consume()
	if err != nil {
		return nil, err
	}

	var value interface{}
	value, err = stream.Consume()
	if err != nil {
		return nil, err
	}

	float, err := TryToNumeric(value)
	if err == nil {
		value = float
	}

	switch ComparisonOperator(operator) {
	case Eq:
		fallthrough
	case Neq:
		fallthrough
	case Gt:
		fallthrough
	case Lt:
		fallthrough
	case Gte:
		fallthrough
	case Lte:
		fallthrough
	case In:
	default:
		return nil, errors.New(fmt.Sprintf("%s is not a valid Operator", operator))
	}

	return &Leaf{
		Field:   field,
		Value:   value,
		Compare: ComparisonOperator(operator),
	}, nil
}

func parseFields(stream *streamTokenizer) ([]Field, error) {
	peek, err := stream.Consume()
	if err != nil || peek != sel {
		return nil, err
	}

	var fields []Field

	for {
		field, err := stream.Consume()

		if field == where {
			break
		}

		if err != nil {
			if errors.Is(err, eof) {
				return fields, err
			}

			return nil, err
		}

		next, err := stream.Peek()
		if err != nil {
			return nil, err
		}

		var alias KeyAlias
		var function Function

		switch next {
		case "as":
			alias, err = parseAlias(stream)
			if err != nil {
				return nil, err
			}
			// its actually a function
		case "(":
			function, err = parseFunction(stream)
			if err != nil {
				return nil, err
			}

			// a function has to be followed by an alias
			alias, err = parseAlias(stream)
			if err != nil {
				return nil, err
			}
		}

		fieldName := strings.TrimRight(field, ",")

		fields = append(fields, Field{
			Name:     fieldName,
			Alias:    tern(alias != "", alias, KeyAlias(fieldName)),
			Function: function,
		})
	}

	return fields, nil
}

func tern[T any](pred bool, left T, right T) T {
	if pred {
		return left
	}

	return right
}

func parseAlias(stream *streamTokenizer) (KeyAlias, error) {
	as, err := stream.Consume()
	if err != nil {
		return "", err
	}

	if as != "as" {
		return "", errors.New("invalid alias token, expected 'as'")
	}

	alias, err := stream.Consume()
	if err != nil {
		return "", err
	}

	return KeyAlias(strings.TrimRight(alias, ",")), nil
}

func parseFunction(stream *streamTokenizer) (Function, error) {
	_, err := stream.Consume()
	if err != nil {
		return "", err
	}

	function, err := stream.Consume()
	if err != nil {
		return "", err
	}

	closeBracket, err := stream.Consume()
	if err != nil {
		return "", err
	}

	if closeBracket != ")" {
		return "", errors.New("unclosed bracket after function definition")
	}

	return function, nil
}

func TryToNumeric(value interface{}) (float64, error) {
	return strconv.ParseFloat(fmt.Sprintf("%v", value), 64)
}
