package sql

type ColumnType string

const (
	ColumnTypeText     = ColumnType("TEXT")
	ColumnTypeDatetime = ColumnType("DATETIME")
	ColumnTypeInteger  = ColumnType("INTEGER")
	ColumnTypeNumeric  = ColumnType("NUMERIC")
)

type Column struct {
	Type ColumnType
	Name string
}
