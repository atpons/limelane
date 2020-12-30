package logger

import "log"

type Logger struct {
	Debug bool
}

func (logger Logger) Debugf(format string, args ...interface{}) {
	if logger.Debug {
		log.Printf(format+"\n", args...)
	}
}

func (logger Logger) Infof(format string, args ...interface{}) {
	if logger.Debug {
		log.Printf(format+"\n", args...)
	}
}
func (logger Logger) Warnf(format string, args ...interface{}) {
	log.Printf(format+"\n", args...)
}

func (logger Logger) Errorf(format string, args ...interface{}) {
	log.Printf(format+"\n", args...)
}
