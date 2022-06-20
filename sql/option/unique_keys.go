package option

import "github.com/tys-muta/go-opt"

type uniqueKeys [][]string

var _ opt.Option = (*uniqueKeys)(nil)

func (o uniqueKeys) Validate() error {
	return nil
}

func (o uniqueKeys) Apply(options any) {
	if v, ok := options.(*CreateOptions); ok {
		v.UniqueKeys = o
	}
}

func WithUniqueKeys(v [][]string) opt.Option {
	return uniqueKeys(v)
}
