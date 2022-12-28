package option

import "github.com/tys-muta/go-sqx/cmd/sqlite/types"

type CreateOptions struct {
	PrimaryKey   []string
	UniqueKeys   [][]string
	IndexKeys    [][]string
	ForeignKeys  []types.ForeignKey
	ShardColumns []types.Column
}
