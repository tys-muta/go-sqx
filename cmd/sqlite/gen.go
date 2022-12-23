package sqlite

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/iancoleman/strcase"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
	"github.com/tys-muta/go-sqx/cmd/sqlite/config"
	"github.com/tys-muta/go-sqx/cmd/sqlite/option"
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
		Long: `Reads table data (.xlsx, .tsv, .csv ) from a Git repository
and Output SQLite database file.`,
	},
}

var dbFile string

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

	dbFile = args[0]

	defer func() {
		if err := c.cleanUp(); err != nil {
			retErr = err
		}
	}()

	var bfs billy.Filesystem
	var err error

	switch {
	case c.Cfg.Local.Path != "":
		path := c.Cfg.Local.Path
		log.Printf("🔽 Local [%s]", path)
		bfs = osfs.New(path)
	case c.Cfg.Remote.Repo != "":
		repo := c.Cfg.Remote.Repo
		log.Printf("🔽 Remote [%s]", repo)
		bfs, err = c.clone(repo)
		if err != nil {
			return fmt.Errorf("failed to clone: %w", err)
		}
	}
	if bfs == nil {
		return fmt.Errorf("no file system")
	}

	log.Printf("🔽 create database")
	db, err := c.newDB()
	if err != nil {
		return fmt.Errorf("failed to setup: %w", err)
	}
	defer db.Close()

	log.Printf("🔽 create tables")
	argMap, err := c.createTables(db, bfs)
	if err != nil {
		return fmt.Errorf("failed to create: %w", err)
	}

	log.Printf("🔽 Insert records")
	err = c.insertRecords(db, bfs, argMap)
	if err != nil {
		return fmt.Errorf("failed to insert: %w", err)
	}

	return nil
}

func (c *g) cleanUp() error {
	return nil
}

func (c *g) clone(ref string) (billy.Filesystem, error) {
	if ref == "" {
		return nil, fmt.Errorf("git repository reference is required")
	}

	options := &git.CloneOptions{
		URL:      ref,
		Progress: os.Stdout,
	}

	if v := c.Cfg.Remote.PrivateKey; v.FilePath != "" {
		// TODO: 合っているはずの秘密鍵でも key mismatch になってしまうため要調査
		pKey, err := ssh.NewPublicKeysFromFile("user", v.FilePath, v.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to open public key: %w", err)
		}
		options.Auth = pKey
	}

	if v := c.Cfg.Remote.BasicAuth; v.Username != "" {
		options.Auth = &http.BasicAuth{
			Username: v.Username,
			Password: v.Password,
		}
	}

	fs := memfs.New()

	repo, err := git.Clone(memory.NewStorage(), fs, options)
	if err != nil {
		return nil, fmt.Errorf("failed to clone: %w", err)
	}

	if v := c.Cfg.Remote.Refs; v != "" {
		log.Printf("🔽 Checkout [%s]", v)
		workTree, err := repo.Worktree()
		if err != nil {
			return nil, fmt.Errorf("failed to work tree: %w", err)
		}
		if err := workTree.Checkout(&git.CheckoutOptions{
			Branch: plumbing.ReferenceName(v),
		}); err != nil {
			return nil, fmt.Errorf("failed to checkout: %w", err)
		}
	}

	return fs, nil
}

func (c *g) newDB() (*sql.DB, error) {
	if err := os.RemoveAll(dbFile); err != nil {
		return nil, fmt.Errorf("failed to remove db file: %w", err)
	}

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection with database")
	}

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return nil, fmt.Errorf("failed to enable foreign key: %w", err)
	}

	return db, nil
}

func (c *g) createTables(db *sql.DB, bfs billy.Filesystem) (map[string]arg, error) {
	argMap := map[string]arg{}

	tables, err := ScanTables(bfs, c.Cfg.Head.Path, c.Cfg.Head.Ext)
	if err != nil {
		return nil, fmt.Errorf("failed to scan: %w", err)
	}

	for _, t := range tables {
		arg := arg{}

		nameRow, err := t.Row(c.Cfg.Head.ColumnNameRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get name row[%s]: %w", t.Index, err)
		}

		typeRow, err := t.Row(c.Cfg.Head.ColumnTypeRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get type row[%s]: %w", t.Index, err)
		}

		if len(nameRow) != len(typeRow) {
			return nil, fmt.Errorf("mismatch length of columns. name: %d, type: %d", len(nameRow), len(typeRow))
		}

		arg.options = append(arg.options, option.WithPrimaryKey(t.Config.PrimaryKey))
		arg.options = append(arg.options, option.WithUniqueKey(t.Config.UniqueKeys...))
		arg.options = append(arg.options, option.WithIndexKey(t.Config.IndexKeys...))
		for _, v := range t.Config.ForeignKeys {
			arg.options = append(arg.options, option.WithForeignKey(option.ForeignKey{
				Column:    v.Column,
				Reference: v.Reference,
			}))
		}

		if t.Config.ShardColumnName != "" {
			arg.columns = append(arg.columns, query.Column{
				Type: query.ColumnType(t.Config.ShardColumnType),
				Name: strcase.ToCamel(t.Config.ShardColumnName),
			})
		}

		if _, ok := argMap[t.Name]; ok {
			// 分割されているテーブルは重複を除外する
			continue
		}

		arg.name = t.Name
		for i, v := range typeRow {
			arg.columns = append(arg.columns, query.Column{
				Type: query.ColumnType(v),
				Name: strcase.ToCamel(nameRow[i]),
			})
		}
		argMap[t.Name] = arg
	}

	for _, arg := range argMap {
		query, err := query.Create(arg.name, arg.columns, arg.options...)
		if err != nil {
			return nil, fmt.Errorf("failed to generate creation query: %w", err)
		}

		if _, err := db.Exec(query); err != nil {
			return nil, fmt.Errorf("failed to execute creation query: %w", err)
		}
	}

	return argMap, nil
}

func (c *g) insertRecords(db *sql.DB, bfs billy.Filesystem, argMap map[string]arg) error {
	var tables []Table
	if v, err := ScanTables(bfs, c.Cfg.Body.Path, c.Cfg.Body.Ext); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	} else {
		tables = v
	}

	queries := []string{}
	for _, t := range tables {
		startRow := c.Cfg.Body.StartRow
		if t.RowLength() < startRow {
			return fmt.Errorf("not enough rows. rows: %d, start row: %d", t.RowLength(), startRow)
		}

		arg, ok := argMap[t.Name]
		if !ok {
			return fmt.Errorf("not exists arg. table name: %s", t.Name)
		}

		values := [][]string(t.Table[startRow-1:])

		if t.Config.ShardColumnName != "" {
			base := filepath.Base(t.Index)
			for i, v := range values {
				values[i] = append([]string{base}, v...)
			}
		}

		query, err := query.Insert(arg.name, arg.columns, values)
		if err != nil {
			return fmt.Errorf("failed to generate insertion query: %w", err)
		}

		queries = append(queries, query)
	}

	// 外部キー制約によって失敗する事を考慮してインサート＆リトライを行う
	retryCounts := map[string]int{}
	insert := func(queries []string) ([]string, error) {
		failedQueries := []string{}
		for _, query := range queries {
			_, err := db.Exec(query)
			log.Printf("%s", query)
			if strings.Contains(fmt.Sprintf("%s", err), "FOREIGN KEY constraint failed") {
				failedQueries = append(failedQueries, query)
				retryCounts[query]++
				if retryCounts[query] > 10 {
					return nil, fmt.Errorf("failed to execute insertion query retry: %w", err)
				}
			} else if err != nil {
				return nil, fmt.Errorf("failed to execute insertion query: %w", err)
			}
		}
		return failedQueries, nil
	}

	for {
		queries, err := insert(queries)
		if err != nil {
			return fmt.Errorf("failed to insert: %w", err)
		}
		if len(queries) == 0 {
			break
		}
	}

	return nil
}
