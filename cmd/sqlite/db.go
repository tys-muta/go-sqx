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
	"github.com/tys-muta/go-sqx/cmd/sqlite/types"
	"github.com/tys-muta/go-sqx/fs"
)

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

func createTables(db *sql.DB, bfs billy.Filesystem) (map[string]types.Definition, error) {
	defMap := map[string]types.Definition{}

	head := config.Get().Head

	tables, err := scanTables(bfs, head.Path, head.Ext)
	if err != nil {
		return nil, fmt.Errorf("failed to scan: %w", err)
	}

	for _, table := range tables {
		def := types.Definition{}

		nameRow, err := table.Row(head.ColumnNameRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get name row[%s]: %w", table.Index, err)
		}

		typeRow, err := table.Row(head.ColumnTypeRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get type row[%s]: %w", table.Index, err)
		}

		if len(nameRow) != len(typeRow) {
			return nil, fmt.Errorf("mismatch length of columns. name: %d, type: %d", len(nameRow), len(typeRow))
		}

		def.Options = append(def.Options, option.WithPrimaryKey(table.PrimaryKey))
		def.Options = append(def.Options, option.WithUniqueKey(table.UniqueKeys...))
		def.Options = append(def.Options, option.WithIndexKey(table.IndexKeys...))
		for _, v := range table.ForeignKeys {
			def.Options = append(def.Options, option.WithForeignKey(option.ForeignKey{
				Column:    v.Column,
				Reference: v.Reference,
			}))
		}

		if table.ShardColumnName != "" {
			def.Columns = append(def.Columns, types.Column{
				Type: types.ColumnType(table.ShardColumnType),
				Name: strcase.ToCamel(table.ShardColumnName),
			})
		}

		if _, ok := defMap[table.Name]; ok {
			// 分割されているテーブルは重複を除外する
			continue
		}

		def.Name = table.Name
		for i, v := range typeRow {
			def.Columns = append(def.Columns, types.Column{
				Type: types.ColumnType(v),
				Name: strcase.ToCamel(nameRow[i]),
			})
		}

		defMap[table.Name] = def
	}

	for _, def := range defMap {
		query, err := query.Create(def.Name, def.Columns, def.Options...)
		if err != nil {
			return nil, fmt.Errorf("failed to generate creation query: %w", err)
		}

		if _, err := db.Exec(query); err != nil {
			return nil, fmt.Errorf("failed to execute creation query: %w", err)
		}
	}

	return defMap, nil
}

func insertRecords(db *sql.DB, bfs billy.Filesystem, defMap map[string]types.Definition) error {
	body := config.Get().Body

	tables, err := scanTables(bfs, body.Path, body.Ext)
	if err != nil {
		return fmt.Errorf("failed to scan: %w", err)
	}

	queries := []string{}
	for _, table := range tables {
		startRow := body.StartRow
		if table.Length() < startRow {
			return fmt.Errorf("not enough rows. rows: %d, start row: %d", table.Length(), startRow)
		}

		def, ok := defMap[table.Name]
		if !ok {
			return fmt.Errorf("not exists arg. table name: %s", table.Name)
		}

		values := [][]string(table.Rows[startRow-1:])

		if table.ShardColumnName != "" {
			base := filepath.Base(table.Index)
			for i, v := range values {
				values[i] = append([]string{base}, v...)
			}
		}

		query, err := query.Insert(def.Name, def.Columns, values)
		if err != nil {
			return fmt.Errorf("failed to generate insertion query: %w", err)
		}

		queries = append(queries, query)
	}

	// 外部キー制約によって失敗する事を考慮してインサート＆リトライを行う
	retryCounts := map[string]int{}
	for {
		failedQueries := []string{}
		for _, query := range queries {
			log.Printf("%s", query)
			_, err := db.Exec(query)
			if err != nil {
				if strings.Contains(fmt.Sprintf("%s", err), "FOREIGN KEY constraint failed") {
					failedQueries = append(failedQueries, query)
					retryCounts[query]++
					if retryCounts[query] > 10 {
						return fmt.Errorf("failed to execute insertion query retry: %w", err)
					}
				}
				return fmt.Errorf("failed to execute insertion query: %w", err)
			}
		}
		if len(failedQueries) == 0 {
			break
		}
	}

	return nil
}

func scanTables(bfs billy.Filesystem, path string, ext string) ([]types.Table, error) {
	tables := []types.Table{}

	fileMap, err := fs.Read(bfs, path, ext)
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}

	for index, file := range fileMap {
		rows, err := table.Parse(bfs, file)
		if err != nil {
			return nil, fmt.Errorf("failed to parse: %w", err)
		}

		table := types.Table{Index: index, Rows: rows}

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
				table.Table = cfg
			case
				len(cfg.PrimaryKey) > 0,
				len(cfg.UniqueKeys) > 0,
				len(cfg.IndexKeys) > 0,
				len(cfg.ForeignKeys) > 0:
				if index == path {
					table.Table = cfg
				}
			}
		}

		table.Name = strcase.ToCamel(strings.Replace(index, "/", "_", -1))

		tables = append(tables, table)
	}

	return tables, nil
}
