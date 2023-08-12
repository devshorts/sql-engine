package sql

import (
	"reflect"
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

	if !reflect.DeepEqual(result, lexed) {
		t.Fail()
	}
}
