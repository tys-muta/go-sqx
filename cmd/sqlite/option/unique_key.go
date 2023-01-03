package option

func WithUniqueKey(v ...[]string) func(any) {
	return func(options any) {
		switch o := options.(type) {
		case *CreateOptions:
			o.UniqueKeys = append(o.UniqueKeys, v...)
		}
	}
}
