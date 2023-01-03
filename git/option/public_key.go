package option

import (
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
)

func WithPublicKeys(key ssh.PublicKeys) CloneOption {
	return func(options any) {
		switch o := options.(type) {
		case *git.CloneOptions:
			o.Auth = &key
		}
	}
}
