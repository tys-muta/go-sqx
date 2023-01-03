package option

import "github.com/tys-muta/go-sqx/cmd/sqlite/types"

func WithForeignKey(v ...types.ForeignKey) func(any) {
	return func(options any) {
		switch o := options.(type) {
		case *CreateOptions:
			o.ForeignKeys = append(o.ForeignKeys, v...)
		}
	}
}
