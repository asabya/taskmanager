package taskmanager

type Logger interface {
	Tracef(format string, args ...interface{})
	Trace(args ...interface{})
	Debugf(format string, args ...interface{})
	Debug(args ...interface{})
	Infof(format string, args ...interface{})
	Info(args ...interface{})
	Warningf(format string, args ...interface{})
	Warning(args ...interface{})
	Errorf(format string, args ...interface{})
	Error(args ...interface{})
}
