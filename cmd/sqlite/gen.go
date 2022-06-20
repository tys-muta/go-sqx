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
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/iancoleman/strcase"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
	"github.com/tys-muta/go-opt"
	"github.com/tys-muta/go-sqx/cfg"
	"github.com/tys-muta/go-sqx/cfg/sqlite"
	s_fs "github.com/tys-muta/go-sqx/fs"
	s_log "github.com/tys-muta/go-sqx/log"
	s_sql "github.com/tys-muta/go-sqx/sql"
	s_sql_option "github.com/tys-muta/go-sqx/sql/option"
	s_table "github.com/tys-muta/go-sqx/table"
)

type gen struct {
	Cfg sqlite.Gen
	Cmd *cobra.Command
}

type table struct {
	value s_table.Table
	index string
	name  string
	cfg   sqlite.GenTable
}

type arg struct {
	name    string
	columns []s_sql.Column
	options []opt.Option
}

var Gen = &gen{
	Cfg: cfg.Value.Sqlite.Gen,
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

	Gen.Cmd.Flags().StringVarP(&Gen.Cfg.Repo, "repo", "", Gen.Cfg.Repo, "git repository.")
	Gen.Cmd.Flags().StringVarP(&Gen.Cfg.Refs, "refs", "", Gen.Cfg.Refs, "git repository reference.")
	Gen.Cmd.Flags().StringVarP(&Gen.Cfg.PrivateKey.FilePath, "key", "k", Gen.Cfg.PrivateKey.FilePath, "PEM format ssh private key file path. required for ssh access to private repository.")

	// 以下の情報はコマンドラインで渡すのはセキュアではないため、フラグは用意しない
	// - SSH プライベートキーのパスワード
	// - Basic 認証のユーザーとパスワード
}

func (c *gen) Run(command *cobra.Command, args []string) (retErr error) {
	l := len(args)
	if l > 0 {
		dbFile = args[0]
	} else {
		return fmt.Errorf("DB file is not specified")
	}

	var repo string
	if l > 1 {
		repo = args[1]
	} else {
		repo = c.Cfg.Repo
	}

	defer func() {
		if err := c.cleanup(); err != nil {
			retErr = err
		}
	}()

	log.Printf("🔽 Clone repository [%s]", repo)
	var bfs billy.Filesystem
	if v, err := c.clone(repo); err != nil {
		return fmt.Errorf("failed to clone: %w", err)
	} else {
		bfs = v
	}

	log.Printf("🔽 Setup database")
	var db *sql.DB
	if v, err := c.setup(); err != nil {
		return fmt.Errorf("failed to setup: %w", err)
	} else {
		defer v.Close()
		db = v
	}

	log.Printf("🔽 Create tables")
	var argMap map[string]arg
	if v, err := c.create(db, bfs); err != nil {
		return fmt.Errorf("failed to create: %w", err)
	} else {
		argMap = v
	}

	log.Printf("🔽 Insert records")
	if err := c.insert(db, bfs, argMap); err != nil {
		return fmt.Errorf("failed to insert: %w", err)
	}

	return nil
}

func (c *gen) cleanup() error {
	return nil
}

func (c *gen) clone(ref string) (billy.Filesystem, error) {
	if ref == "" {
		return nil, fmt.Errorf("git repository reference is required")
	}

	options := &git.CloneOptions{
		URL:      ref,
		Progress: s_log.Writer(),
	}

	if c.Cfg.PrivateKey.FilePath != "" {
		// TODO: 合っているはずの秘密鍵でも key mismatch になってしまうため要調査
		if v, err := ssh.NewPublicKeysFromFile("user", c.Cfg.PrivateKey.FilePath, c.Cfg.PrivateKey.Password); err != nil {
			return nil, fmt.Errorf("failed to open public key: %w", err)
		} else {
			options.Auth = v
		}
	}

	if c.Cfg.BasicAuth.Username != "" {
		options.Auth = &http.BasicAuth{
			Username: c.Cfg.BasicAuth.Username,
			Password: c.Cfg.BasicAuth.Password,
		}
	}

	fs := memfs.New()

	var repo *git.Repository
	if v, err := git.Clone(memory.NewStorage(), fs, options); err != nil {
		return nil, fmt.Errorf("failed to clone: %w", err)
	} else {
		repo = v
	}

	if c.Cfg.Refs != "" {
		log.Printf("🔽 Checkout [%s]", c.Cfg.Refs)
		if v, err := repo.Worktree(); err != nil {
			return nil, fmt.Errorf("failed to work tree: %w", err)
		} else if err := v.Checkout(&git.CheckoutOptions{
			Branch: plumbing.ReferenceName(c.Cfg.Refs),
		}); err != nil {
			return nil, fmt.Errorf("failed to checkout: %w", err)
		}
	}

	return fs, nil
}

func (c *gen) setup() (*sql.DB, error) {
	if err := os.RemoveAll(dbFile); err != nil {
		return nil, fmt.Errorf("failed to remove db file: %w", err)
	} else if v, err := sql.Open("sqlite3", dbFile); err != nil {
		return nil, fmt.Errorf("failed to open connection with database")
	} else {
		return v, nil
	}
}

func (c *gen) create(db *sql.DB, bfs billy.Filesystem) (map[string]arg, error) {
	argMap := map[string]arg{}

	var tables []table
	if v, err := c.scan(bfs, c.Cfg.Head.Path, c.Cfg.Head.Ext); err != nil {
		return nil, fmt.Errorf("failed to scan: %w", err)
	} else {
		tables = v
	}

	for _, t := range tables {
		arg := arg{}

		var nameRow []string
		if v, err := t.value.Row(c.Cfg.Head.ColumnNameRow); err != nil {
			return nil, fmt.Errorf("failed to get row: %w", err)
		} else {
			nameRow = v
		}

		var typeRow []string
		if v, err := t.value.Row(c.Cfg.Head.ColumnTypeRow); err != nil {
			return nil, fmt.Errorf("failed to get row: %w", err)
		} else {
			typeRow = v
		}

		if len(nameRow) != len(typeRow) {
			return nil, fmt.Errorf("mismatch length of columns. name: %d, type: %d", len(nameRow), len(typeRow))
		}

		if v := t.cfg.PrimaryKey; len(v) > 0 {
			arg.options = append(arg.options, s_sql_option.WithPrimaryKey(v))
		}
		if v := t.cfg.UniqueKeys; len(v) > 0 {
			arg.options = append(arg.options, s_sql_option.WithUniqueKeys(v))
		}
		if v := t.cfg.IndexKeys; len(v) > 0 {
			arg.options = append(arg.options, s_sql_option.WithIndexKeys(v))
		}
		if t.cfg.ShardColumnName != "" {
			arg.columns = append(arg.columns, s_sql.Column{
				Type: c.columnType(t.cfg.ShardColumnType),
				Name: strcase.ToCamel(t.cfg.ShardColumnName),
			})
		}

		if _, ok := argMap[t.name]; ok {
			// 分割されているテーブルは重複を除外する
			continue
		}

		arg.name = t.name
		for i, v := range typeRow {
			arg.columns = append(arg.columns, s_sql.Column{
				Type: c.columnType(v),
				Name: strcase.ToCamel(nameRow[i]),
			})
		}
		argMap[t.name] = arg
	}

	for _, arg := range argMap {
		if v, err := s_sql.Create(arg.name, arg.columns, arg.options...); err != nil {
			return nil, fmt.Errorf("failed to generate creation query: %w", err)
		} else if _, err := db.Exec(v); err != nil {
			return nil, fmt.Errorf("failed to execute query: %w", err)
		}
	}

	return argMap, nil
}

func (c *gen) insert(db *sql.DB, bfs billy.Filesystem, argMap map[string]arg) error {
	var tables []table
	if v, err := c.scan(bfs, c.Cfg.Body.Path, c.Cfg.Body.Ext); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	} else {
		tables = v
	}

	for _, t := range tables {
		startRow := c.Cfg.Body.StartRow
		if t.value.RowLength() < startRow {
			return fmt.Errorf("not enough rows. rows: %d, start row: %d", t.value.RowLength(), startRow)
		}

		var arg arg
		if v, ok := argMap[t.name]; !ok {
			return fmt.Errorf("not exists arg. table name: %s", t.name)
		} else {
			arg = v
		}

		values := [][]string(t.value[startRow-1:])

		if t.cfg.ShardColumnName != "" {
			base := filepath.Base(t.index)
			for i, v := range values {
				values[i] = append([]string{base}, v...)
			}
		}

		if v, err := s_sql.Insert(arg.name, arg.columns, values); err != nil {
			return fmt.Errorf("failed to generate insertion query: %w", err)
		} else if _, err := db.Exec(v); err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
	}

	return nil
}

func (c *gen) scan(bfs billy.Filesystem, path string, ext string) ([]table, error) {
	tables := []table{}

	var fileMap s_fs.FileMap
	if v, err := s_fs.Read(bfs, path, ext); err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	} else {
		fileMap = v
	}

	for index, file := range fileMap {
		table := table{index: index}
		if v, err := s_table.Parse(bfs, file); err != nil {
			return nil, fmt.Errorf("failed to parse: %w", err)
		} else {
			table.value = v
		}

		for k, v := range c.Cfg.Table {
			if !strings.HasPrefix(index, k) {
				continue
			}
			index = k
			table.cfg = v
		}

		table.name = strcase.ToCamel(strings.Replace(index, "/", "_", -1))

		tables = append(tables, table)
	}

	return tables, nil
}

func (c *gen) columnType(v string) s_sql.ColumnType {
	switch v {
	case "datetime":
		return s_sql.ColumnTypeDatetime
	case "int":
		return s_sql.ColumnTypeInteger
	case "float":
		return s_sql.ColumnTypeNumeric
	default:
		return s_sql.ColumnTypeText
	}
}
