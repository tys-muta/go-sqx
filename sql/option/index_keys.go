package option

import "github.com/tys-muta/go-opt"

type indexKeys [][]string

var _ opt.Option = (*indexKeys)(nil)

func (o indexKeys) Validate() error {
	return nil
}

func (o indexKeys) Apply(options any) {
	switch v := options.(type) {
	case *CreateOptions:
		v.IndexKeys = o
	}
}

func WithIndexKeys(v [][]string) opt.Option {
	return indexKeys(v)
}
