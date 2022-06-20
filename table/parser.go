package table

import (
	"fmt"

	"github.com/tys-muta/go-sqx/fs"
)

type parser interface {
	Parse([]byte) (Table, error)
}

func NewParser(fileType fs.FileType) (parser, error) {
	switch fileType {
	case fs.FileTypeXLSX:
		return &xlsx{}, nil
	default:
		return nil, fmt.Errorf("unsupported parser type")
	}
}
