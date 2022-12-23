package sqlite

import (
	"fmt"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/tys-muta/go-sqx/cmd/sqlite/config"
	"github.com/tys-muta/go-sqx/git"
	git_option "github.com/tys-muta/go-sqx/git/option"
)

func clone(repo string) (billy.Filesystem, error) {
	cfg := config.Get().Remote

	options := []git_option.CloneOption{}
	if v := cfg.PrivateKey; v.FilePath != "" {
		// TODO: 合っているはずの秘密鍵でも key mismatch になってしまうため要調査
		pKey, err := ssh.NewPublicKeysFromFile("user", v.FilePath, v.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to open public key: %w", err)
		}
		options = append(options, git_option.WithPublicKeys(*pKey))
	}
	if v := cfg.BasicAuth; v.Username != "" {
		options = append(options, git_option.WithBasicAuth(http.BasicAuth{
			Username: v.Username,
			Password: v.Password,
		}))
	}
	if v := cfg.Refs; v != "" {
		options = append(options, git_option.WithReference(v))
	}

	bfs, err := git.Clone(repo, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to clone: %w", err)
	}

	return bfs, nil
}
