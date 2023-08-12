package sql

import "unicode"

type Tokens []string

func Lex(raw string) Tokens {
	var tokens []string

	parenth := false

	buff := ""
	for _, char := range []rune(raw) {
		// in a quoted string and ending the quote
		if char == '"' && parenth {
			tokens = append(tokens, buff)
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

			if unicode.IsSpace(char) {
				tokens = append(tokens, buff)
				buff = ""
				continue
			}
		}

		// continue lexing
		buff += string(char)
	}

	if buff != "" {
		tokens = append(tokens, buff)
	}

	return tokens
}
