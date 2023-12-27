package sqlite

import (
	"database/sql"
	"fmt"
	"log"
	"os"
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
		def.Options = append(def.Options, option.WithForeignKey(table.ForeignKeys...))
		def.Options = append(def.Options, option.WithShardColumn(table.ShardColumns...))

		def.Columns = append(def.Columns, table.ShardColumns...)

		if _, ok := defMap[table.Name]; ok {
			// 分割されているテーブルでは定義が複数発生しうるため、定義が既に存在する場合はスキップする
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

		rows := table.Rows[startRow-1:]

		// 分割されたテーブルの場合、分割キーを先頭に追加する
		shardColumns := []string{}
		for _, column := range table.ShardColumns {
			shardColumns = append(shardColumns, column.Value)
		}

		if len(shardColumns) > 0 {
			for i, row := range rows {
				rows[i] = append(shardColumns, row...)
			}
		}

		query, err := query.Insert(def.Name, def.Columns, rows)
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
			_, err := db.Exec(query)
			if err != nil {
				if strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
					// 外部キー制約によるエラーの場合はリトライする
					failedQueries = append(failedQueries, query)
					retryCounts[query]++
					if retryCounts[query] > 10 {
						log.Printf("%s", query)
						return fmt.Errorf("failed to retry insertion query: %w", err)
					}
				} else {
					// その他のエラーはリトライせずにエラーとする
					//
					// e.g.
					// foreign key mismatch - 外部キーの参照先カラムがユニークではない
					log.Printf("%s", query)
					return fmt.Errorf("failed to execute insertion query: %w", err)
				}
			} else {
				log.Printf("%s", query)
			}
		}
		if len(failedQueries) == 0 {
			break
		}
		queries = failedQueries
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

		table := types.Table{
			Index: index,
			Name:  strcase.ToCamel(strings.Replace(index, "/", "_", -1)),
			Rows:  rows,
		}

		// ファイルに対応する設定があれば適用する
		for k, v := range config.Get().Table {
			associate(&table, k, v)
		}

		tables = append(tables, table)
	}

	return tables, nil
}

// テーブルの索引キーに対応する設定があれば関連付ける
func associate(table *types.Table, key string, cfg config.Table) {
	// 設定キーから接頭辞とパラメータ名を抽出する
	paramNameMap := map[int]string{}
	paramNames := []string{}
	paramIndexes := []int{}
	keyIDs := []string{}
	for i, v := range strings.Split(key, "/") {
		if strings.HasPrefix(v, ":") {
			paramNameMap[i] = strings.TrimPrefix(v, ":")
			paramNames = append(paramNames, strings.TrimPrefix(v, ":"))
			paramIndexes = append(paramIndexes, i)
		} else {
			keyIDs = append(keyIDs, v)
		}
	}
	keyID := strings.Join(keyIDs, "/")

	// テーブルの索引キーと設定の一意キーが完全一致しない場合はシャードテーブルか判定
	if table.Index != keyID {
		values := strings.Split(table.Index, "/")
		tableIDs := []string{}
		for i, value := range values {
			if _, ok := paramNameMap[i]; !ok {
				tableIDs = append(tableIDs, value)
			}
		}
		tableID := strings.Join(tableIDs, "/")
		if tableID != keyID {
			// シャードテーブルではないので関連付けない
			return
		}

		// シャードテーブルの場合はパラメータの値を取得する
		j := 0
		for k, paramName := range paramNames {
			i := paramIndexes[k]
			if len(values) <= i || len(cfg.ShardTypes) <= j {
				return
			}
			paramValue := values[i]
			shardType := cfg.ShardTypes[j]
			column := types.Column{
				Name:  strcase.ToCamel(paramName),
				Type:  types.ColumnType(shardType),
				Value: paramValue,
			}
			switch column.Type {
			case types.ColumnTypeInteger:
				// シャードキーの型が整数でない場合は関連付けない
				_, err := strconv.Atoi(paramValue)
				if err != nil {
					return
				}
			case types.ColumnTypeString:
				// do nothing
			default:
				// シャードキーの型が整数でも文字列でもない場合は関連付けない
				return
			}

			table.ShardColumns = append(table.ShardColumns, column)

			j++
		}
	}

	table.Name = strcase.ToCamel(strings.Replace(keyID, "/", "_", -1))
	table.PrimaryKey = cfg.PrimaryKey
	table.UniqueKeys = cfg.UniqueKeys
	table.IndexKeys = cfg.IndexKeys
	table.ForeignKeys = cfg.ForeignKeys
}
