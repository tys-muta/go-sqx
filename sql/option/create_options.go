package option

type CreateOptions struct {
	PrimaryKey  primaryKey
	UniqueKeys  uniqueKeys
	IndexKeys   indexKeys
	ForeignKeys foreignKeys
}
