package types

type Column struct {
	Type  columnType
	Name  string
	Value string
}

type columnType string

const (
	ColumnTypeString     = columnType("STRING")
	ColumnTypeNullString = columnType("NULL_STRING")
	ColumnTypeDateTime   = columnType("DATETIME")
	ColumnTypeInteger    = columnType("INTEGER")
	ColumnTypeNumeric    = columnType("NUMERIC")
)

func ColumnType(v string) columnType {
	switch v {
	case "time":
		return ColumnTypeDateTime
	case "int":
		return ColumnTypeInteger
	case "float":
		return ColumnTypeNumeric
	case "null_string":
		return ColumnTypeNullString
	default:
		return ColumnTypeString
	}
}

func (c columnType) AsSQL() string {
	switch c {
	case ColumnTypeDateTime:
		return "`INTEGER(TIMESTAMP)` NOT NULL"
	case ColumnTypeInteger:
		return "INTEGER NOT NULL"
	case ColumnTypeNumeric:
		return "NUMERIC NOT NULL"
	case ColumnTypeNullString:
		return "TEXT NULL"
	default:
		return "TEXT NOT NULL"
	}
}
