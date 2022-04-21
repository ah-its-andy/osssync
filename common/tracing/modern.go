package tracing

import (
	"fmt"
	"runtime/debug"
)

func Error(err error) error {
	if err == nil {
		return nil
	}
	stack := debug.Stack()
	var stacktrace string
	if len(stack) > 0 {
		stacktrace = string(stack)
	}
	return &ModernError{
		msg:        err.Error(),
		stacktrace: stacktrace,
		inner:      err,
	}
}

func Errorf(message string, innerError error) error {
	if merr, ok := innerError.(*ModernError); ok {
		stack := debug.Stack()
		var stacktrace string
		if len(stack) > 0 {
			stacktrace = string(stack)
		}
		return &ModernError{
			inner:      merr,
			msg:        message,
			stacktrace: stacktrace,
		}
	}

	stack := debug.Stack()
	var stacktrace string
	if len(stack) > 0 {
		stacktrace = string(stack)
	}
	return &ModernError{
		msg: message,
		inner: &ModernError{
			msg:        innerError.Error(),
			stacktrace: stacktrace,
		},
	}
}

type ModernError struct {
	inner error

	msg        string
	stacktrace string
}

func (e *ModernError) Error() string {
	return sprintModernError(e)
}

func (e *ModernError) InnerError() error {
	return e.inner
}

func sprintModernError(e *ModernError) string {
	errText := fmt.Sprintf("%s\n Stacktrace: %s", e.msg, e.stacktrace)
	if e.inner != nil {
		if me, ok := e.inner.(*ModernError); ok {
			errText = fmt.Sprintf("%s\n Inner Error: %s", errText, sprintModernError(me))
		} else {
			errText = fmt.Sprintf("%s\n Inner Error: %s", errText, e.inner.Error())
		}
	}
	return errText
}

func IsError(e error, err error) bool {
	if e == err {
		return true
	}

	if m, ok := e.(*ModernError); !ok {
		return false
	} else {
		if m.inner != nil {
			if modern, ok := m.inner.(*ModernError); ok {
				return IsError(modern, err)
			} else {
				return m.inner == err
			}
		}
	}

	return false
}
