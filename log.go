package taskmanager

type Logger interface {
	Info(msg string, args ...interface{})
	Error(err error, msg string, args ...interface{})
}
