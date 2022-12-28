package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/tys-muta/go-sqx/cmd/sqlite/types"
)

type Config struct {
	Timezone string
	Location time.Location
	Local    struct {
		Path string
	}
	Remote struct {
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
	Table map[string]Table
}

type Table struct {
	PrimaryKey  []string
	UniqueKeys  [][]string
	IndexKeys   [][]string
	ForeignKeys []types.ForeignKey
	ShardTypes  []string
}

const (
	defaultPath = "sqlite_gen.toml"
)

var (
	cfg Config
)

func init() {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	tomlPath := fmt.Sprintf("%s/%s", dir, defaultPath)
	info, err := os.Stat(tomlPath)
	if !errors.Is(err, os.ErrNotExist) && !info.IsDir() {
		if v, err := ioutil.ReadFile(tomlPath); err != nil {
			log.Fatal(err)
		} else if err := toml.Unmarshal(v, &cfg); err != nil {
			log.Fatal(err)
		}
	}

	if cfg.Timezone == "" {
		return
	}

	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		panic(err)
	}

	cfg.Location = *loc
}

func Get() Config {
	return cfg
}
