package table

import (
	"fmt"

	"github.com/tys-muta/go-sqx/cmd/sqlite/types"
	"github.com/tys-muta/go-sqx/fs"
)

type parser interface {
	Parse([]byte) (types.Rows, error)
}

func NewParser(fileType fs.FileType) (parser, error) {
	switch fileType {
	case fs.FileTypeXLSX:
		return &xlsxParser{}, nil
	case fs.FileTypeCSV:
		return &csvParser{}, nil
	case fs.FileTypeTSV:
		return &tsvParser{}, nil
	default:
		return nil, fmt.Errorf("unsupported parser type")
	}
}
