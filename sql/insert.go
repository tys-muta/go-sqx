package sql

import (
	"fmt"
	"log"
	"strings"
)

func Insert(tableName string, columns []Column, values [][]string) (string, error) {
	columnSlice := []string{}
	for _, v := range columns {
		columnSlice = append(columnSlice, fmt.Sprintf("`%s`", v.Name))
	}

	valueSlice := []string{}
	for _, v := range values {
		tmp := []string{}
		for i, v := range v {
			if v, err := parse(columns[i], v); err != nil {
				return "", fmt.Errorf("failed to parse: %w", err)
			} else {
				tmp = append(tmp, v)
			}
		}
		valueSlice = append(valueSlice, fmt.Sprintf("(%s)", strings.Join(tmp, ", ")))
	}

	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES %s;", tableName,
		strings.Join(columnSlice, ", "),
		strings.Join(valueSlice, ", "),
	)

	log.Printf("%s", query)

	return query, nil
}
