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
		path := c.Cfg.Local.Path
		log.Printf("🔽 Local [%s]", path)
		bfs = osfs.New(path)
	case c.Cfg.Remote.Repo != "":
		repo := c.Cfg.Remote.Repo
		log.Printf("🔽 Remote [repository: %s, branch: %s]", repo, c.Cfg.Remote.Refs)
		bfs, err = clone(repo)
		if err != nil {
			return fmt.Errorf("failed to clone: %w", err)
		}
	}
	if bfs == nil {
		return fmt.Errorf("no file system")
	}

	log.Printf("🔽 Create database")
	db, err := createDatabase(args[0])
	if err != nil {
		return fmt.Errorf("failed to setup: %w", err)
	}
	defer db.Close()

	log.Printf("🔽 Create tables")
	argMap, err := createTables(db, bfs)
	if err != nil {
		return fmt.Errorf("failed to create: %w", err)
	}

	log.Printf("🔽 Insert records")
	err = insertRecords(db, bfs, argMap)
	if err != nil {
		return fmt.Errorf("failed to insert: %w", err)
	}

	return nil
}

func (c *g) cleanUp() error {
	return nil
}
