package table

import (
	"fmt"

	"github.com/go-git/go-billy/v5"
	"github.com/tys-muta/go-sqx/fs"
)

func Parse(bfs billy.Basic, file fs.File) (Data, error) {
	bytes := make([]byte, file.Size)

	f, err := bfs.Open(file.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	if _, err := f.Read(bytes); err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	parser, err := NewParser(file.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to parse [%s]: %w", file.Path, err)
	}

	data, err := parser.Parse(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse [%s]: %w", file.Path, err)
	}

	return data, nil
}
