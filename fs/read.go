package fs

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
)

func Read(dir billy.Dir, path string, ext string) (FileMap, error) {
	if v, err := read(dir, path, "", ext); err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	} else {
		return v, nil
	}
}

func read(dir billy.Dir, rootPath string, path string, ext string) (FileMap, error) {
	if path == "" {
		path = rootPath
	}

	var infoSlice []fs.FileInfo
	if v, err := dir.ReadDir(path); err != nil {
		return nil, fmt.Errorf("failed to read dir on file system: %w", err)
	} else {
		infoSlice = v
	}

	fileMap := FileMap{}

	for _, info := range infoSlice {
		path := filepath.Join(path, info.Name())
		if !info.IsDir() && filepath.Ext(path) == ext {
			index := strings.TrimSuffix(path, ext)
			index = strings.TrimPrefix(index, rootPath)
			fileMap[index] = File{
				Path: path,
				Size: int(info.Size()),
				Type: FileType(strings.TrimPrefix(ext, ".")),
			}
			continue
		}
		if v, err := read(dir, rootPath, path, ext); err != nil {
			return nil, fmt.Errorf("failed to read: %w", err)
		} else {
			for index, file := range v {
				fileMap[index] = file
			}
		}
	}

	return fileMap, nil
}
