package sql

import (
	"strings"
	"unicode"
)

type Tokens []string

func Lex(raw string) Tokens {
	var tokens []string

	parenth := false

	buff := ""
	for _, char := range []rune(raw) {
		if char == '(' || char == ')' {
			if buff != "" {
				tokens = append(tokens, strings.Trim(buff, " "))
			}

			tokens = append(tokens, string(char))
			buff = ""
			continue
		}

		// in a quoted string and ending the quote
		if char == '"' && parenth {
			if buff != "" {
				tokens = append(tokens, strings.Trim(buff, " "))
			}
			parenth = false
			buff = ""
			continue
		}

		// not in a quoted string
		if !parenth {
			// end of quoted string
			if char == '"' {
				buff = ""
				parenth = true
				continue
			}

			if unicode.IsSpace(char) && buff != "" {
				tokens = append(tokens, strings.Trim(buff, " "))
				buff = ""
				continue
			}
		}

		// continue lexing
		buff += string(char)
	}

	if buff != "" {
		tokens = append(tokens, strings.Trim(buff, " "))
	}

	return tokens
}
