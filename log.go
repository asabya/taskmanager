package taskmanager

type Logger interface {
	Infof(format string, args ...interface{})
	Info(args ...interface{})
	Errorf(format string, args ...interface{})
	Error(args ...interface{})
}
