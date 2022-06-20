package fs

type FileMap map[string]File

type FileType string

const (
	FileTypeXLSX = FileType("xlsx")
)

type File struct {
	Path string
	Size int
	Type FileType
}
