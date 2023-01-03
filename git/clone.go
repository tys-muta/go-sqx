package git

import (
	"fmt"
	"os"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/tys-muta/go-sqx/git/option"
)

func Clone(url string, options ...option.CloneOption) (billy.Filesystem, error) {
	if url == "" {
		return nil, fmt.Errorf("git repository reference is required")
	}

	fs := memfs.New()

	// リポジトリをクローンする
	cloneOptions := git.CloneOptions{URL: url, Progress: os.Stdout}
	for _, o := range options {
		o(&cloneOptions)
	}

	repo, err := git.Clone(memory.NewStorage(), fs, &cloneOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to clone: %w", err)
	}

	// ブランチをチェックアウトする
	checkoutOptions := git.CheckoutOptions{}
	for _, o := range options {
		o(&checkoutOptions)
	}
	if branch := checkoutOptions.Branch; branch != "" {
		workTree, err := repo.Worktree()
		if err != nil {
			return nil, fmt.Errorf("failed to work tree: %w", err)
		}

		if err := workTree.Checkout(&checkoutOptions); err != nil {
			return nil, fmt.Errorf("failed to checkout: %w", err)
		}
	}

	return fs, nil
}
