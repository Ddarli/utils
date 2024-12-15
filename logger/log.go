package logger

import "go.uber.org/zap"

type logger struct {
	logger zap.SugaredLogger
}

func New() Logger {
	return &logger{}
}

type Logger interface {
	Info(msg string)
	Error(msg string)
}

func (l *logger) Info(msg string) {
	l.logger.Info(msg)
}

func (l *logger) Error(msg string) {
	l.Error(msg)
}
