package option

import "github.com/tys-muta/go-opt"

type primaryKey []string

var _ opt.Option = (*primaryKey)(nil)

func (o primaryKey) Validate() error {
	return nil
}

func (o primaryKey) Apply(options any) {
	switch v := options.(type) {
	case *CreateOptions:
		v.PrimaryKey = o
	}
}

func WithPrimaryKey(v []string) opt.Option {
	return primaryKey(v)
}
