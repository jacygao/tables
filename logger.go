package tables

import (
	"fmt"
	"log"
)

// Logger is a generic interface
type Logger interface {
	Info(args ...interface{})
	Infof(template string, args ...interface{})
	Error(args ...interface{})
	Errorf(template string, args ...interface{})
}

// If no Logger implementation is provided, DefaultLogger is used for logging.
type defaultLogger struct{}

func (dl *defaultLogger) Info(args ...interface{}) {
	log.Print("INFO: " + fmt.Sprint(args...))
}

func (dl *defaultLogger) Infof(template string, args ...interface{}) {
	log.Print("INFO: " + fmt.Sprintf(template, args...))
}

func (dl *defaultLogger) Error(args ...interface{}) {
	log.Print("ERROR: " + fmt.Sprint(args...))
}

func (dl *defaultLogger) Errorf(template string, args ...interface{}) {
	log.Print("ERROR: " + fmt.Sprintf(template, args...))
}
