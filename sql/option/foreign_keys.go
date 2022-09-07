package option

import "github.com/tys-muta/go-opt"

type ForeignKey struct {
	Column    string
	Reference string
}

type foreignKeys []ForeignKey

var _ opt.Option = (*foreignKeys)(nil)

func (o foreignKeys) Validate() error {
	return nil
}

func (o foreignKeys) Apply(options any) {
	switch v := options.(type) {
	case *CreateOptions:
		v.ForeignKeys = o
	}
}

func WithForeignKeys(v []ForeignKey) opt.Option {
	return foreignKeys(v)
}
