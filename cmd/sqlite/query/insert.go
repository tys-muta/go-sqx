package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/tys-muta/go-sqx/cmd/sqlite/config"
	"github.com/tys-muta/go-sqx/cmd/sqlite/types"
)

func Insert(tableName string, columns []types.Column, values [][]string) (string, error) {
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
				return "", fmt.Errorf("failed to cast table[%s]: %w", tableName, err)
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

	return query, nil
}

func cast(column types.Column, value string) (string, error) {
	switch column.Type {
	case types.ColumnTypeInteger:
		if value == "" {
			return "0", nil
		}
		if v, err := strconv.ParseInt(value, 10, 64); err != nil {
			return "", fmt.Errorf("failed to parse int: %w", err)
		} else {
			return fmt.Sprintf(`%d`, int(v)), nil
		}
	case types.ColumnTypeNumeric:
		if value == "" {
			return "0", nil
		}
		if v, err := strconv.ParseFloat(value, 64); err != nil {
			return "", fmt.Errorf("failed to parse float, %w", err)
		} else {
			return fmt.Sprintf(`%g`, v), nil
		}
	case types.ColumnTypeDateTime:
		if _, err := time.Parse(time.RFC3339, value); err == nil {
			return fmt.Sprintf(`"%s"`, value), nil
		}
		// タイムゾーンが含まれないフォーマットの場合、コンフィグに基づきタイムゾーンを設定
		if v, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
			loc := config.Get().Location
			v = v.In(&loc)
			_, offset := v.Zone()
			v = v.Add(time.Duration(offset) * -time.Second)
			return fmt.Sprintf(`"%s"`, v.Format(time.RFC3339)), nil
		}
		return fmt.Sprintf(`"%s"`, value), nil
	default:
		// SQL に合わせたダブルクォーてション処理
		value = strings.Replace(value, `"`, `""`, -1)
		return fmt.Sprintf(`"%s"`, value), nil
	}
}
