package sql

import (
	"fmt"
	"log"
	"strings"

	"github.com/iancoleman/strcase"
	"github.com/tys-muta/go-opt"
	"github.com/tys-muta/go-sqx/sql/option"
)

func Create(tableName string, columns []Column, options ...opt.Option) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("columns is empty")
	}

	o := &option.CreateOptions{}
	if err := opt.Reflect(o, options...); err != nil {
		return "", fmt.Errorf("failed to reflect: %w", err)
	}

	body := []string{}
	for _, column := range columns {
		body = append(body, fmt.Sprintf("`%s` %s", column.Name, column.Type))
	}
	if len(o.PrimaryKey) > 0 {
		keys := []string{}
		for _, v := range o.PrimaryKey {
			keys = append(keys, fmt.Sprintf("`%s`", strcase.ToCamel(v)))
		}
		body = append(body, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(keys, ", ")))
	} else {
		// プライマリーキーの指定が無い場合は先頭カラムをプライマリキーにする
		body = append(body, fmt.Sprintf("PRIMARY KEY (`%s`)", strcase.ToCamel(columns[0].Name)))
	}

	queries := []string{}
	queries = append(queries, fmt.Sprintf("CREATE TABLE `%s` (%s)", tableName, strings.Join(body, ", ")))

	var toCamel = func(slice []string) []string {
		ret := []string{}
		for _, v := range slice {
			ret = append(ret, strcase.ToCamel(v))
		}
		return ret
	}

	for _, v := range o.UniqueKeys {
		v = toCamel(v)
		queries = append(queries, fmt.Sprintf("CREATE UNIQUE INDEX `%s` ON `%s` (%s)",
			strings.Join(v, "-"),
			tableName,
			strings.Join(v, ", "),
		))
	}
	for _, v := range o.IndexKeys {
		v = toCamel(v)
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
