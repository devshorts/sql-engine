package sql

import (
	"errors"
	"fmt"
	"strings"
)

const (
	sel   = "select"
	where = "where"
	eq    = "=="
	neq   = "!="
	in    = "in"
)

func Parse(raw string) (*Query, error) {
	stream := NewStreamTokenizer(Lex(raw))

	fields, err := parseFields(stream)
	if err != nil {
		if err == eof {
			return &Query{
				fields: fields,
			}, nil
		}
		return nil, err
	}

	group, err := parseGroup(stream)
	if err != nil {
		return nil, err
	}

	return &Query{
		fields: fields,
		group:  group,
	}, nil
}

func parseGroup(stream *streamTokenizer) (*PredicateGroup, error) {
	leaf, err := parseLeaf(stream)
	if err != nil {
		return nil, err
	}

	return &PredicateGroup{
		operator: And,
		predicate: []Tree{
			NewLeaf(*leaf),
		},
	}, nil
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

	value, err := stream.Consume()
	if err != nil {
		return nil, err
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
		return nil, errors.New(fmt.Sprintf("%s is not a valid operator", operator))
	}

	return &Leaf{
		field:   field,
		value:   value,
		compare: ComparisonOperator(operator),
	}, nil
}

func parseFields(stream *streamTokenizer) ([]string, error) {
	peek, err := stream.Consume()
	if err != nil || peek != sel {
		return nil, err
	}

	var fields []string

	for {
		field, err := stream.Consume()

		if err != nil {
			if err == eof {
				return fields, err
			}

			return nil, err
		}

		if field != where {
			fields = append(fields, strings.TrimRight(field, ","))
		} else {
			break
		}
	}

	return fields, nil
}
