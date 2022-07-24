package sql

type ColumnType string

const (
	ColumnTypeText    = ColumnType("TEXT")
	ColumnTypeTime    = ColumnType("TIME")
	ColumnTypeInteger = ColumnType("INTEGER")
	ColumnTypeNumeric = ColumnType("NUMERIC")
)

type Column struct {
	Type ColumnType
	Name string
}
