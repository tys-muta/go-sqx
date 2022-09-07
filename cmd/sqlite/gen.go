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
	"github.com/tys-muta/go-opt"
	"github.com/tys-muta/go-sqx/cmd/sqlite/gen"
	"github.com/tys-muta/go-sqx/config"
	"github.com/tys-muta/go-sqx/config/sqlite"
	s_log "github.com/tys-muta/go-sqx/log"
	s_sql "github.com/tys-muta/go-sqx/sql"
	s_sql_option "github.com/tys-muta/go-sqx/sql/option"
)

type g struct {
	Cmd *cobra.Command
}

type arg struct {
	name    string
	columns []s_sql.Column
	options []opt.Option
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
var cfg sqlite.Gen

func init() {
	Gen.Cmd.RunE = Gen.Run

	cfg = config.Get().SQLite.Gen

	// Gen.Cmd.Flags().StringVarP(&cfg.Clone.Repo, "repo", "", cfg.Clone.Repo, "git repository.")

	// 以下の情報はコマンドラインで渡すのはセキュアではないため、フラグは用意しない
	// - SSH プライベートキーのパスワード
	// - Basic 認証のユーザーとパスワード
}

func (c *g) Run(command *cobra.Command, args []string) (retErr error) {
	l := len(args)
	if l > 0 {
		dbFile = args[0]
	} else {
		return fmt.Errorf("DB file is not specified")
	}

	defer func() {
		if err := c.cleanup(); err != nil {
			retErr = err
		}
	}()

	var bfs billy.Filesystem
	if v := cfg.Local.Path; v != "" {
		log.Printf("🔽 Local [%s]", v)
		bfs = osfs.New(v)
	} else if v := cfg.Remote.Repo; v != "" {
		log.Printf("🔽 Remote [%s]", v)
		if v, err := c.clone(v); err != nil {
			return fmt.Errorf("failed to clone: %w", err)
		} else {
			bfs = v
		}
	}
	if bfs == nil {
		return fmt.Errorf("no file system")
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

func (c *g) cleanup() error {
	return nil
}

func (c *g) clone(ref string) (billy.Filesystem, error) {
	if ref == "" {
		return nil, fmt.Errorf("git repository reference is required")
	}

	options := &git.CloneOptions{
		URL:      ref,
		Progress: s_log.Writer(),
	}

	if v := cfg.Remote.PrivateKey; v.FilePath != "" {
		// TODO: 合っているはずの秘密鍵でも key mismatch になってしまうため要調査
		if v, err := ssh.NewPublicKeysFromFile("user", v.FilePath, v.Password); err != nil {
			return nil, fmt.Errorf("failed to open public key: %w", err)
		} else {
			options.Auth = v
		}
	}

	if v := cfg.Remote.BasicAuth; v.Username != "" {
		options.Auth = &http.BasicAuth{
			Username: v.Username,
			Password: v.Password,
		}
	}

	fs := memfs.New()

	var repo *git.Repository
	if v, err := git.Clone(memory.NewStorage(), fs, options); err != nil {
		return nil, fmt.Errorf("failed to clone: %w", err)
	} else {
		repo = v
	}

	if v := cfg.Remote.Refs; v != "" {
		log.Printf("🔽 Checkout [%s]", v)
		if wt, err := repo.Worktree(); err != nil {
			return nil, fmt.Errorf("failed to work tree: %w", err)
		} else if err := wt.Checkout(&git.CheckoutOptions{
			Branch: plumbing.ReferenceName(v),
		}); err != nil {
			return nil, fmt.Errorf("failed to checkout: %w", err)
		}
	}

	return fs, nil
}

func (c *g) setup() (*sql.DB, error) {
	if err := os.RemoveAll(dbFile); err != nil {
		return nil, fmt.Errorf("failed to remove db file: %w", err)
	} else if v, err := sql.Open("sqlite3", dbFile); err != nil {
		return nil, fmt.Errorf("failed to open connection with database")
	} else if _, err := v.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return nil, fmt.Errorf("failed to enable foreign key: %w", err)
	} else {
		return v, nil
	}
}

func (c *g) create(db *sql.DB, bfs billy.Filesystem) (map[string]arg, error) {
	argMap := map[string]arg{}

	var tables []gen.Table
	if v, err := gen.ScanTables(bfs, cfg.Head.Path, cfg.Head.Ext); err != nil {
		return nil, fmt.Errorf("failed to scan: %w", err)
	} else {
		tables = v
	}

	for _, t := range tables {
		arg := arg{}

		var nameRow []string
		if v, err := t.Row(cfg.Head.ColumnNameRow); err != nil {
			return nil, fmt.Errorf("%s: %w", t.Index, err)
		} else {
			nameRow = v
		}

		var typeRow []string
		if v, err := t.Row(cfg.Head.ColumnTypeRow); err != nil {
			return nil, fmt.Errorf("%s: %w", t.Index, err)
		} else {
			typeRow = v
		}

		if len(nameRow) != len(typeRow) {
			return nil, fmt.Errorf("mismatch length of columns. name: %d, type: %d", len(nameRow), len(typeRow))
		}

		if v := t.Config.PrimaryKey; len(v) > 0 {
			arg.options = append(arg.options, s_sql_option.WithPrimaryKey(v))
		}
		if v := t.Config.UniqueKeys; len(v) > 0 {
			arg.options = append(arg.options, s_sql_option.WithUniqueKeys(v))
		}
		if v := t.Config.IndexKeys; len(v) > 0 {
			arg.options = append(arg.options, s_sql_option.WithIndexKeys(v))
		}
		if v := t.Config.ForeignKeys; len(v) > 0 {
			keys := []s_sql_option.ForeignKey{}
			for _, v := range v {
				keys = append(keys, v)
			}
			arg.options = append(arg.options, s_sql_option.WithForeignKeys(keys))
		}
		if t.Config.ShardColumnName != "" {
			arg.columns = append(arg.columns, s_sql.Column{
				Type: c.columnType(t.Config.ShardColumnType),
				Name: strcase.ToCamel(t.Config.ShardColumnName),
			})
		}

		if _, ok := argMap[t.Name]; ok {
			// 分割されているテーブルは重複を除外する
			continue
		}

		arg.name = t.Name
		for i, v := range typeRow {
			arg.columns = append(arg.columns, s_sql.Column{
				Type: c.columnType(v),
				Name: strcase.ToCamel(nameRow[i]),
			})
		}
		argMap[t.Name] = arg
	}

	for _, arg := range argMap {
		if v, err := s_sql.Create(arg.name, arg.columns, arg.options...); err != nil {
			return nil, fmt.Errorf("failed to generate creation query: %w", err)
		} else if _, err := db.Exec(v); err != nil {
			return nil, fmt.Errorf("failed to execute creation query: %w", err)
		}
	}

	return argMap, nil
}

func (c *g) insert(db *sql.DB, bfs billy.Filesystem, argMap map[string]arg) error {
	var tables []gen.Table
	if v, err := gen.ScanTables(bfs, cfg.Body.Path, cfg.Body.Ext); err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	} else {
		tables = v
	}

	queries := []string{}
	for _, t := range tables {
		startRow := cfg.Body.StartRow
		if t.RowLength() < startRow {
			return fmt.Errorf("not enough rows. rows: %d, start row: %d", t.RowLength(), startRow)
		}

		var arg arg
		if v, ok := argMap[t.Name]; !ok {
			return fmt.Errorf("not exists arg. table name: %s", t.Name)
		} else {
			arg = v
		}

		values := [][]string(t.Table[startRow-1:])

		if t.Config.ShardColumnName != "" {
			base := filepath.Base(t.Index)
			for i, v := range values {
				values[i] = append([]string{base}, v...)
			}
		}

		if v, err := s_sql.Insert(arg.name, arg.columns, values); err != nil {
			return fmt.Errorf("failed to generate insertion query: %w", err)
		} else {
			queries = append(queries, v)
		}
	}

	// 外部キー制約によって失敗する事を考慮してインサート＆リトライを行う
	retryCounts := map[string]int{}
	insert := func(queries []string) ([]string, error) {
		failedQueries := []string{}
		for _, query := range queries {
			if _, err := db.Exec(query); strings.Contains(fmt.Sprintf("%s", err), "FOREIGN KEY constraint failed") {
				failedQueries = append(failedQueries, query)
				retryCounts[query]++
				if retryCounts[query] > 10 {
					return nil, fmt.Errorf("failed to execute insertion query retry: %w", err)
				}
			} else if err != nil {
				return nil, fmt.Errorf("failed to execute insertion query: %w", err)
			} else {
				log.Printf("%s", query)
			}
		}
		return failedQueries, nil
	}

	var err error
	for {
		queries, err = insert(queries)
		if err != nil {
			return fmt.Errorf("failed to insert: %w", err)
		}
		if len(queries) == 0 {
			break
		}
	}

	return nil
}

func (c *g) columnType(v string) s_sql.ColumnType {
	switch v {
	case "time":
		return s_sql.ColumnTypeDateTime
	case "int":
		return s_sql.ColumnTypeInteger
	case "float":
		return s_sql.ColumnTypeNumeric
	default:
		return s_sql.ColumnTypeText
	}
}
