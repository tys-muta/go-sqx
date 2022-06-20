package sql

import (
	"fmt"
	"strconv"
)

func init() {

}

func parse(column Column, value string) (string, error) {
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
		return fmt.Sprintf(`"%s"`, value), nil
	}
}
