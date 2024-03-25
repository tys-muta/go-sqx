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

func Insert(tableName string, columns []types.Column, rows types.Rows) (string, error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("columns is empty")
	}

	columnSlice := []string{}
	for _, v := range columns {
		columnSlice = append(columnSlice, fmt.Sprintf("`%s`", v.Name))
	}

	valueSlice := []string{}
	for _, row := range rows {
		tmp := []string{}
		for i, value := range row {
			v, err := cast(columns[i], value)
			if err != nil {
				return "", fmt.Errorf("failed to cast table[%s]: %w", tableName, err)
			}
			tmp = append(tmp, v)
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
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return "", fmt.Errorf("failed to parse int: %w", err)
		}
		return fmt.Sprintf(`%d`, int(v)), nil
	case types.ColumnTypeNumeric:
		if value == "" {
			return "0", nil
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return "", fmt.Errorf("failed to parse float, %w", err)
		}
		return fmt.Sprintf(`%g`, v), nil
	case types.ColumnTypeDateTime:
		if v, err := time.Parse(time.RFC3339, value); err == nil {
			return fmt.Sprintf(`%d`, v.Unix()), nil
		}
		// タイムゾーンが含まれないフォーマットの場合、コンフィグに基づきタイムゾーンを設定
		if v, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
			loc := config.Get().Location
			v = v.In(&loc)
			_, offset := v.Zone()
			v = v.Add(time.Duration(offset) * -time.Second)
			return fmt.Sprintf(`%d`, v.Unix()), nil
		}
		return "0", nil
	case types.ColumnTypeNullString:
		if value == "" {
			return "null", nil
		}
		// SQL に合わせたダブルクォーテーション処理
		value = strings.Replace(value, `"`, `""`, -1)
		return fmt.Sprintf(`"%s"`, value), nil
	default:
		// SQL に合わせたダブルクォーテーション処理
		value = strings.Replace(value, `"`, `""`, -1)
		return fmt.Sprintf(`"%s"`, value), nil
	}
}
