package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "sqx",
	Short: "",
	Long:  ``,
}

func init() {
	time.Local = time.FixedZone("Asia/Tokyo", 9*60*60)
}

func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
