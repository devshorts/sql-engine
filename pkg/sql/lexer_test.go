package sql

import (
	"slices"
	"testing"
)

func TestLexes(t *testing.T) {
	lexed := Lex(`select foo from bar where zap = "jim jam"`)

	result := []string{
		`select`,
		`foo`,
		`from`,
		`bar`,
		`where`,
		`zap`,
		`=`,
		`jim jam`,
	}

	if !slices.Equal(result, lexed) {
		t.Fail()
	}
}

func TestLexesWithParenth(t *testing.T) {
	lexed := Lex(`select foo from bar where (zap = "jim jam" and zip = 1) or boo = 3`)

	result := []string{
		`select`,
		`foo`,
		`from`,
		`bar`,
		`where`,
		`(`,
		`zap`,
		`=`,
		`jim jam`,
		`and`,
		`zip`,
		`=`,
		`1`,
		`)`,
		`or`,
		`boo`,
		`=`,
		`3`,
	}

	if !slices.Equal(result, lexed) {
		t.Fail()
	}
}

func TestLexesWithParenthAlias(t *testing.T) {
	lexed := Lex(`select average(foo) as avg from bar where (zap = "jim jam" and zip = 1) or boo = 3`)

	result := []string{
		`select`,
		`average`,
		`(`,
		`foo`,
		`)`,
		`as`,
		`avg`,
		`from`,
		`bar`,
		`where`,
		`(`,
		`zap`,
		`=`,
		`jim jam`,
		`and`,
		`zip`,
		`=`,
		`1`,
		`)`,
		`or`,
		`boo`,
		`=`,
		`3`,
	}

	if !slices.Equal(result, lexed) {
		t.Fail()
	}
}
