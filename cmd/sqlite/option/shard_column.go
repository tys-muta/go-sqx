package option

import "github.com/tys-muta/go-sqx/cmd/sqlite/types"

func WithShardColumn(v ...types.Column) func(any) {
	return func(options any) {
		switch o := options.(type) {
		case *CreateOptions:
			o.ShardColumns = append(o.ShardColumns, v...)
		}
	}
}
