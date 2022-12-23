package sqlite

import (
	"fmt"
	"log"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
	"github.com/tys-muta/go-sqx/cmd/sqlite/config"
	"github.com/tys-muta/go-sqx/cmd/sqlite/query"
)

type g struct {
	Cmd *cobra.Command
	Cfg config.Config
}

type arg struct {
	name    string
	columns []query.Column
	options []func(any)
}

var Gen = &g{
	Cmd: &cobra.Command{
		Use:   "gen",
		Short: "Output SQLite database file",
		Long: `Reads table data (.xlsx, .tsv, .csv ) from git repository
and output SQLite database file`,
	},
}

func init() {
	Gen.Cmd.RunE = Gen.Run
	Gen.Cfg = config.Get()

	// Gen.Cmd.Flags().StringVarP(&c.Cfg.Clone.Repo, "repo", "", c.Cfg.Clone.Repo, "git repository.")

	// 以下の情報はコマンドラインで渡すのはセキュアではないため、フラグは用意しない
	// - SSH プライベートキーのパスワード
	// - Basic 認証のユーザーとパスワード
}

func (c *g) Run(command *cobra.Command, args []string) (retErr error) {
	if len(args) == 0 {
		return fmt.Errorf("database file is not specified")
	}

	defer func() {
		if err := c.cleanUp(); err != nil {
			retErr = err
		}
	}()

	// 元となるデータの保存先によってファイルシステムを切り替える
	var bfs billy.Filesystem
	var err error
	switch {
	case c.Cfg.Local.Path != "":
		bfs = osfs.New(c.Cfg.Local.Path)
		log.Printf("🔽 Local [path: %s]", c.Cfg.Local.Path)
	case c.Cfg.Remote.Repo != "":
		bfs, err = clone(c.Cfg.Remote.Repo)
		log.Printf("🔽 Remote [repository: %s, branch: %s]", c.Cfg.Remote.Repo, c.Cfg.Remote.Refs)
	}
	if err != nil {
		return fmt.Errorf("filed to setup file system: %w", err)
	}
	if bfs == nil {
		return fmt.Errorf("no file system")
	}

	// データベースファイルを作成する
	if err := createDB(bfs, args[0]); err != nil {
		return fmt.Errorf("failed to setup: %w", err)
	}

	return nil
}

func (c *g) cleanUp() error {
	return nil
}
