package sqlite

type Sqlite struct {
	Gen Gen
}

type Gen struct {
	Repo       string
	Refs       string
	PrivateKey struct {
		FilePath string
		Password string
	}
	BasicAuth struct {
		Username string
		Password string
	}
	Head struct {
		Ext           string
		Path          string
		ColumnNameRow int
		ColumnTypeRow int
	}
	Body struct {
		Ext      string
		Path     string
		StartRow int
	}
	XLSX struct {
		Sheet string
	}
	Table map[string]GenTable
}

type GenTable struct {
	PrimaryKey      []string
	UniqueKeys      [][]string
	IndexKeys       [][]string
	ShardColumnName string
	ShardColumnType string
}
