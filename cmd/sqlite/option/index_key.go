package option

func WithIndexKey(v ...[]string) func(any) {
	return func(options any) {
		switch o := options.(type) {
		case *CreateOptions:
			o.IndexKeys = append(o.IndexKeys, v...)
		}
	}
}
