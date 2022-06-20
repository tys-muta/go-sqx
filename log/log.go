package log

import (
	"io"
	"log"
	"os"

	"github.com/sirupsen/logrus"
)

var v = &logrus.Logger{
	Out:       os.Stdout,
	Level:     logrus.TraceLevel,
	Formatter: &logrus.TextFormatter{},
}

func init() {
	log.SetOutput(v.Writer())
}

func Writer() *io.PipeWriter {
	return v.Writer()
}
