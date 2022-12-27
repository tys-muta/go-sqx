package types

type Table struct {
	Rows

	Index string
	Name  string

	PrimaryKey   []string
	UniqueKeys   [][]string
	IndexKeys    [][]string
	ForeignKeys  []ForeignKey
	ShardColumns []Column
}
