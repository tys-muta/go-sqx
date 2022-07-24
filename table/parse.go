package table

import (
	"fmt"

	"github.com/go-git/go-billy/v5"
	"github.com/tys-muta/go-sqx/fs"
)

func Parse(bfs billy.Basic, file fs.File) (Table, error) {
	bytes := make([]byte, file.Size)

	if v, err := bfs.Open(file.Path); err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	} else {
		defer v.Close()
		if _, err := v.Read(bytes); err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}
	}

	var table Table
	if v, err := NewParser(file.Type); err != nil {
		return nil, fmt.Errorf("failed to parse [%s]: %w", file.Path, err)
	} else if v, err := v.Parse(bytes); err != nil {
		return nil, fmt.Errorf("failed to parse [%s]: %w", file.Path, err)
	} else {
		table = v
	}

	return table, nil
}
