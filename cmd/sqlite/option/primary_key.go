package option

func WithPrimaryKey(v []string) func(any) {
	return func(options any) {
		switch o := options.(type) {
		case *CreateOptions:
			o.PrimaryKey = v
		}
	}
}
