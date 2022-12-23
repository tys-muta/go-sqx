package option

type ForeignKey struct {
	Column    string
	Reference string
}

func WithForeignKey(v ...ForeignKey) func(any) {
	return func(options any) {
		switch o := options.(type) {
		case *CreateOptions:
			o.ForeignKeys = append(o.ForeignKeys, v...)
		}
	}
}
