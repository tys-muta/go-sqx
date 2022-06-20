package cmd

import (
	"github.com/spf13/cobra"
	"github.com/tys-muta/go-sqx/cmd/sqlite"
)

var SqliteCmd = &cobra.Command{
	Use:   "sqlite",
	Short: "",
	Long:  ``,
}

func init() {
	RootCmd.AddCommand(SqliteCmd)
	SqliteCmd.AddCommand(sqlite.Gen.Cmd)
}
