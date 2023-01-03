package option

import (
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
)

func WithReference(refs string) CloneOption {
	return func(options any) {
		switch o := options.(type) {
		case *git.CheckoutOptions:
			o.Branch = plumbing.ReferenceName(refs)
		}
	}
}
