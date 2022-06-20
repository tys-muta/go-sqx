package cfg

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/pelletier/go-toml/v2"
	"github.com/tys-muta/go-sqx/cfg/sqlite"
)

var Value = struct {
	Sqlite sqlite.Sqlite
}{}

func init() {
	var tomlPath string
	if v, err := os.Getwd(); err != nil {
		log.Fatal(err)
	} else {
		tomlPath = fmt.Sprintf("%s/%s", v, TomlFile)
	}

	if v, err := os.Stat(tomlPath); !errors.Is(err, os.ErrNotExist) && !v.IsDir() {
		if v, err := ioutil.ReadFile(tomlPath); err != nil {
			log.Fatal(err)
		} else if err := toml.Unmarshal(v, &Value); err != nil {
			log.Fatal(err)
		}
	}
}
