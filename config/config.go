package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/tys-muta/go-sqx/config/sqlite"
)

const (
	TomlFile string = ".sqx.toml"
)

type config struct {
	SQLite SQLite
}

type SQLite struct {
	Gen sqlite.Gen
}

var root config
var location time.Location

func init() {
	var tomlPath string
	if v, err := os.Getwd(); err != nil {
		log.Fatal(err)
	} else {
		tomlPath = fmt.Sprintf("%s/%s", v, TomlFile)
	}

	root = config{}

	if v, err := os.Stat(tomlPath); !errors.Is(err, os.ErrNotExist) && !v.IsDir() {
		if v, err := ioutil.ReadFile(tomlPath); err != nil {
			log.Fatal(err)
		} else if err := toml.Unmarshal(v, &root); err != nil {
			log.Fatal(err)
		}
	}

	if v := root.SQLite.Gen.Timezone; v == "" {
		return
	} else if v, err := time.LoadLocation(v); err != nil {
		panic(err)
	} else {
		location = *v
	}
}

func Get() config {
	return root
}

func Location() *time.Location {
	l := location
	return &l
}
