package types

type Column struct {
	Type  columnType
	Name  string
	Value string
}

type columnType string

const (
	ColumnTypeText     = columnType("TEXT")
	ColumnTypeString   = ColumnTypeText
	ColumnTypeDateTime = columnType("DATETIME")
	ColumnTypeInteger  = columnType("INTEGER")
	ColumnTypeNumeric  = columnType("NUMERIC")
)

func ColumnType(v string) columnType {
	switch v {
	case "time":
		return ColumnTypeDateTime
	case "int":
		return ColumnTypeInteger
	case "float":
		return ColumnTypeNumeric
	default:
		return ColumnTypeText
	}
}

func (c columnType) AsSQL() string {
	switch c {
	case ColumnTypeInteger, ColumnTypeDateTime:
		return "INTEGER"
	case ColumnTypeNumeric:
		return "NUMERIC"
	default:
		return "TEXT"
	}
}
