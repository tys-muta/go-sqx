package sqlite

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/iancoleman/strcase"
	"github.com/tys-muta/go-sqx/cmd/sqlite/config"
	"github.com/tys-muta/go-sqx/cmd/sqlite/option"
	"github.com/tys-muta/go-sqx/cmd/sqlite/query"
	"github.com/tys-muta/go-sqx/cmd/sqlite/table"
	"github.com/tys-muta/go-sqx/fs"
)

type Table struct {
	table.Data

	Index  string
	Name   string
	Config config.Table
}

func createDB(bfs billy.Filesystem, dbFile string) error {
	log.Printf("🔽 Create database")
	if err := os.RemoveAll(dbFile); err != nil {
		return fmt.Errorf("failed to remove db file: %w", err)
	}

	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		return fmt.Errorf("failed to open connection with database")
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("failed to enable foreign key: %w", err)
	}

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

func createTables(db *sql.DB, bfs billy.Filesystem) (map[string]arg, error) {
	argMap := map[string]arg{}

	head := config.Get().Head

	tables, err := scanTables(bfs, head.Path, head.Ext)
	if err != nil {
		return nil, fmt.Errorf("failed to scan: %w", err)
	}

	for _, t := range tables {
		arg := arg{}

		nameRow, err := t.Row(head.ColumnNameRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get name row[%s]: %w", t.Index, err)
		}

		typeRow, err := t.Row(head.ColumnTypeRow)
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

func insertRecords(db *sql.DB, bfs billy.Filesystem, argMap map[string]arg) error {
	body := config.Get().Body

	tables, err := scanTables(bfs, body.Path, body.Ext)
	if err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	}

	queries := []string{}
	for _, table := range tables {
		startRow := body.StartRow
		if table.RowLength() < startRow {
			return fmt.Errorf("not enough rows. rows: %d, start row: %d", table.RowLength(), startRow)
		}

		arg, ok := argMap[table.Name]
		if !ok {
			return fmt.Errorf("not exists arg. table name: %s", table.Name)
		}

		values := [][]string(table.Data[startRow-1:])

		if table.Config.ShardColumnName != "" {
			base := filepath.Base(table.Index)
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

func scanTables(bfs billy.Filesystem, path string, ext string) ([]Table, error) {
	tables := []Table{}

	fileMap, err := fs.Read(bfs, path, ext)
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}

	for index, file := range fileMap {
		data, err := table.Parse(bfs, file)
		if err != nil {
			return nil, fmt.Errorf("failed to parse: %w", err)
		}

		table := Table{Index: index, Data: data}

		for path, cfg := range config.Get().Table {
			if !strings.HasPrefix(index, path) {
				continue
			}
			name := strings.TrimPrefix(index, path)
			name = strings.TrimPrefix(name, "/")
			if strings.Contains(name, "/") {
				continue
			}
			switch {
			case cfg.ShardColumnType != "":
				if cfg.ShardColumnType == "int" {
					if _, err := strconv.ParseInt(name, 10, 64); err != nil {
						continue
					}
				}
				index = path
				table.Config = cfg
			case
				len(cfg.PrimaryKey) > 0,
				len(cfg.UniqueKeys) > 0,
				len(cfg.IndexKeys) > 0,
				len(cfg.ForeignKeys) > 0:
				if index == path {
					table.Config = cfg
				}
			}
		}

		table.Name = strcase.ToCamel(strings.Replace(index, "/", "_", -1))

		tables = append(tables, table)
	}

	return tables, nil
}
