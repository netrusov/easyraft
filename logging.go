package easyraft

import (
	"log"
	"os"

	hclog "github.com/hashicorp/go-hclog"
)

func defaultLogger() hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   "easyraft",
		Level:  hclog.Info,
		Output: os.Stderr,
	})
}

func standardLogger(logger hclog.Logger) *log.Logger {
	return logger.StandardLogger(&hclog.StandardLoggerOptions{
		InferLevels: true,
	})
}
