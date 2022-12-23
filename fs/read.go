package fs

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
)

func Read(bfs billy.Filesystem, path string, ext string) (FileMap, error) {
	fileMap, err := read(bfs, path, "", ext)
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}
	return fileMap, nil
}

func read(bfs billy.Filesystem, rootPath string, path string, ext string) (FileMap, error) {
	if path == "" {
		path = rootPath
	}

	fileInfo, err := bfs.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("failed to stat: %w", err)
	}

	fileMap := FileMap{}

	if !fileInfo.IsDir() {
		return fileMap, nil
	}

	infoList, err := bfs.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read dir on file system: %w", err)
	}

	for _, info := range infoList {
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
		fileMap, err := read(bfs, rootPath, path, ext)
		if err != nil {
			return nil, fmt.Errorf("failed to read: %w", err)
		}
		for key, file := range fileMap {
			fileMap[key] = file
		}
	}

	return fileMap, nil
}
