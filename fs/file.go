package fs

type FileMap map[string]File

type FileType string

const (
	FileTypeXLSX = FileType("xlsx")
	FileTypeCSV  = FileType("csv")
	FileTypeTSV  = FileType("tsv")
)

type File struct {
	Path string
	Size int
	Type FileType
}
