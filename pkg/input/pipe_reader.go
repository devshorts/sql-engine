package input

import (
	"bufio"
	"encoding/json"
)

type DataRow map[string]interface{}

type Reader interface {
	// reads a set of newlines delimited json into a series of data rows
	Parse(buf *bufio.Reader) []DataRow
}

type StdinReader struct {
}

func (s StdinReader) Parse(buf *bufio.Reader) ([]DataRow, error) {
	scanner := bufio.NewScanner(buf)

	var lines []DataRow

	for scanner.Scan() {
		text := scanner.Text()

		var line DataRow

		if err := json.Unmarshal([]byte(text), &line); err != nil {
			return nil, err
		}

		lines = append(lines, line)
	}

	return lines, nil
}

func NewStdinReader() *StdinReader {
	return &StdinReader{}
}
