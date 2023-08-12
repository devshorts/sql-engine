package sql

import "errors"

type streamTokenizer struct {
	tokens []string
	index  int
}

var (
	eof = errors.New("Cannot consume past end of stream")
)

func NewStreamTokenizer(tokens Tokens) *streamTokenizer {
	return &streamTokenizer{
		tokens: tokens,
		index:  0,
	}
}

func (c *streamTokenizer) Consume() (string, error) {
	if c.index > len(c.tokens)-1 {
		return "", eof
	}

	result := c.tokens[c.index]

	c.index++

	return result, nil
}

func (c *streamTokenizer) Peek() (string, error) {
	if c.index > len(c.tokens)-1 {
		return "", errors.New("Cannot peek past end of stream")
	}

	result := c.tokens[c.index]

	return result, nil
}
