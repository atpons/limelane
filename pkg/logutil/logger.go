package logutil

import (
	"sync/atomic"

	"go.uber.org/zap/zapcore"

	"go.uber.org/zap"

	envoylog "github.com/envoyproxy/go-control-plane/pkg/log"
)

var (
	gL, gS atomic.Value
)

func init() {
	Init()
}

func Init() {
	logger := newLogger()
	gL.Store(logger)
	s := gL.Load().(*zap.Logger).Sugar()
	gS.Store(s)
}

func newLogger() *zap.Logger {
	log, _ := zap.NewDevelopment()
	return log
}

func L() *zap.Logger {
	return gL.Load().(*zap.Logger)
}

func S() *zap.SugaredLogger {
	return gS.Load().(*zap.SugaredLogger)
}

type EnvoyLogger interface {
	envoylog.Logger
}

type zapEnvoyLogger struct {
	g     *zap.Logger
	sugar *zap.SugaredLogger
}

func NewEnvoyLogger(g *zap.Logger) EnvoyLogger {
	return &zapEnvoyLogger{
		g:     g,
		sugar: g.Sugar(),
	}
}

func (logger *zapEnvoyLogger) Debugf(format string, args ...interface{}) {
	if logger.g.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	logger.sugar.Debugf(format, args...)
}

func (logger *zapEnvoyLogger) Infof(format string, args ...interface{}) {
	if logger.g.Core().Enabled(zapcore.DebugLevel) {
		return
	}
	logger.sugar.Infof(format, args...)
}
func (logger *zapEnvoyLogger) Warnf(format string, args ...interface{}) {
	logger.sugar.Warnf(format, args...)
}

func (logger *zapEnvoyLogger) Errorf(format string, args ...interface{}) {
	logger.sugar.Errorf(format, args...)
}
