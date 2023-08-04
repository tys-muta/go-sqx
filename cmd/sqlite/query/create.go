package query

import (
	"fmt"
	"log"
	"strings"

	"github.com/tys-muta/go-sqx/cmd/sqlite/option"
	"github.com/tys-muta/go-sqx/cmd/sqlite/types"
)

func Create(tableName string, columns []types.Column, options ...func(any)) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("columns is empty")
	}

	o := option.CreateOptions{}
	for _, v := range options {
		v(&o)
	}

	body := []string{}
	for _, column := range columns {
		body = append(body, fmt.Sprintf("`%s` %s", column.Name, column.Type.AsSQL()))
	}
	if len(o.PrimaryKey) > 0 {
		keys := []string{}
		for _, v := range o.PrimaryKey {
			keys = append(keys, fmt.Sprintf("`%s`", v))
		}
		body = append(body, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(keys, ", ")))
	} else {
		// プライマリーキーの指定が無い場合は先頭カラムをプライマリキーにする
		body = append(body, fmt.Sprintf("PRIMARY KEY (`%s`)", columns[0].Name))
	}

	// 外部キー制約
	for _, v := range o.ForeignKeys {
		body = append(body, fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s", v.Column, v.Reference))
	}

	queries := []string{}
	queries = append(queries, fmt.Sprintf("CREATE TABLE `%s` (%s)", tableName, strings.Join(body, ", ")))

	for _, v := range o.UniqueKeys {
		queries = append(queries, fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s` (%s)",
			strings.Join(v, "-"),
			tableName,
			strings.Join(v, ", "),
		))
	}
	for _, v := range o.IndexKeys {
		queries = append(queries, fmt.Sprintf("CREATE INDEX `%s` ON `%s` (%s)",
			strings.Join(v, "-"),
			tableName,
			strings.Join(v, ", "),
		))
	}

	query := strings.Join(queries, "; ")

	log.Printf("%s", query)

	return query, nil
}
