package table

import (
	b "bytes"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/tys-muta/go-sqx/cmd/sqlite/types"
)

type tsvParser struct{}

var _ parser = (*tsvParser)(nil)

func (p *tsvParser) Parse(bytes []byte) (types.Rows, error) {
	reader := csv.NewReader(b.NewReader(bytes))
	reader.Comma = '\t'
	reader.Comment = '#'
	reader.LazyQuotes = true

	rows := types.Rows{}
	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read tsv file: %w", err)
		}

		rows = append(rows, row)
	}

	return rows, nil
}
