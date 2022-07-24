package fs

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
)

func Read(bfs billy.Filesystem, path string, ext string) (FileMap, error) {
	if v, err := read(bfs, path, "", ext); err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	} else {
		return v, nil
	}
}

func read(bfs billy.Filesystem, rootPath string, path string, ext string) (FileMap, error) {
	fileMap := FileMap{}

	if path == "" {
		path = rootPath
	}

	if v, err := bfs.Stat(path); err != nil {
		return nil, fmt.Errorf("failed to stat: %w", err)
	} else if !v.IsDir() {
		return fileMap, nil
	}

	var infoSlice []fs.FileInfo
	if v, err := bfs.ReadDir(path); err != nil {
		return nil, fmt.Errorf("failed to read dir on file system: %w", err)
	} else {
		infoSlice = v
	}

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
		if v, err := read(bfs, rootPath, path, ext); err != nil {
			return nil, fmt.Errorf("failed to read: %w", err)
		} else {
			for index, file := range v {
				fileMap[index] = file
			}
		}
	}

	return fileMap, nil
}
