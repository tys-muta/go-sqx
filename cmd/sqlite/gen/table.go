package gen

import (
	"fmt"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/iancoleman/strcase"
	"github.com/tys-muta/go-sqx/config"
	"github.com/tys-muta/go-sqx/config/sqlite"
	"github.com/tys-muta/go-sqx/fs"
	"github.com/tys-muta/go-sqx/table"
)

type Table struct {
	table.Table
	Index  string
	Name   string
	Config sqlite.GenTable
}

func ScanTables(bfs billy.Filesystem, path string, ext string) ([]Table, error) {
	tables := []Table{}

	var fileMap fs.FileMap
	if v, err := fs.Read(bfs, path, ext); err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	} else {
		fileMap = v
	}

	configs := config.Get().SQLite.Gen.Table

	for index, file := range fileMap {
		t := Table{Index: index}
		if v, err := table.Parse(bfs, file); err != nil {
			return nil, fmt.Errorf("failed to parse: %w", err)
		} else {
			t.Table = v
		}

		for k, v := range configs {
			if !strings.HasPrefix(index, k) {
				continue
			}
			index = k
			t.Config = v
		}

		t.Name = strcase.ToCamel(strings.Replace(index, "/", "_", -1))

		tables = append(tables, t)
	}

	return tables, nil
}
