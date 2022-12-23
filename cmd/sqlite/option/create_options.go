package option

type CreateOptions struct {
	PrimaryKey  []string
	UniqueKeys  [][]string
	IndexKeys   [][]string
	ForeignKeys []ForeignKey
}
