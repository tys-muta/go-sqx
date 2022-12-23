package table

import (
	b "bytes"
	"encoding/csv"
	"fmt"
	"io"
)

type csvParser struct{}

var _ parser = (*csvParser)(nil)

func (p *csvParser) Parse(bytes []byte) (Table, error) {
	reader := csv.NewReader(b.NewReader(bytes))

	reader.Comma = ','
	reader.Comment = '#'
	reader.LazyQuotes = true

	table := Table{}

	for {
		if v, err := reader.Read(); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("failed to read csv file: %w", err)
		} else {
			table = append(table, v)
		}
	}

	return table, nil
}
