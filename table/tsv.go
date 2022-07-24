package table

import (
	b "bytes"
	"encoding/csv"
	"fmt"
	"io"
)

type tsvParser struct{}

var _ parser = (*tsvParser)(nil)

func (p *tsvParser) Parse(bytes []byte) (Table, error) {
	reader := csv.NewReader(b.NewReader(bytes))

	reader.Comma = '\t'
	reader.Comment = '#'
	reader.LazyQuotes = true

	table := Table{}

	for {
		if v, err := reader.Read(); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to read tsv file: %w", err)
		} else {
			table = append(table, v)
		}
	}

	return table, nil
}
