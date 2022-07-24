package sql

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

func Insert(tableName string, columns []Column, values [][]string) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("columns is empty")
	}

	columnSlice := []string{}
	for _, v := range columns {
		columnSlice = append(columnSlice, fmt.Sprintf("`%s`", v.Name))
	}

	valueSlice := []string{}
	for _, v := range values {
		tmp := []string{}
		for i, v := range v {
			if v, err := cast(columns[i], v); err != nil {
				return "", fmt.Errorf("failed to cast: %w", err)
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

func cast(column Column, value string) (string, error) {
	switch column.Type {
	case ColumnTypeInteger:
		if v, err := strconv.ParseFloat(value, 64); err != nil {
			return "", fmt.Errorf("failed to parse float: %w", err)
		} else {
			return fmt.Sprintf(`%d`, int(v)), nil
		}
	case ColumnTypeNumeric:
		if v, err := strconv.ParseFloat(value, 64); err != nil {
			return "", fmt.Errorf("failed to parse float, %w", err)
		} else {
			return fmt.Sprintf(`%g`, v), nil
		}
	default:
		// SQL に合わせたダブルクォーてション処理
		value = strings.Replace(value, `"`, `""`, -1)
		return fmt.Sprintf(`"%s"`, value), nil
	}
}
