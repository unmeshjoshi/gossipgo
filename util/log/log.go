package log

import (
	"context"
	"errors"
	"fmt"
)
func Warningf(ctx context.Context, format string, args ...interface{}) {
	fmt.Printf(format, args)
}

func Errorf(format string, args ...interface{}) error {
	fmt.Printf(format, args)
	return errors.New(format)
}



func Fatalf(ctx context.Context, format string, args ...interface{}) {
	//logDepth(ctx, 1, severity.FATAL, format, args...)
	fmt.Printf(format, args)
}

func Safe(a interface{}) interface{} {
	return a
}

type safeWrapper struct {
	a interface{}
}

type SafeValue interface {
	SafeValue()
}


func Infof(ctx context.Context, format string, v ...interface{}) {
}

// Format implements the fmt.Formatter interface.
func (w safeWrapper) Format(s fmt.State, verb rune) {
	//reproducePrintf(s, s, verb, w.a)
}

// SafeMessage implements SafeMessager.
func (w safeWrapper) SafeMessage() string {
	return fmt.Sprintf("%v", w.a)
}

func SetExitFunc(hideStack bool, f func(interface{})) {
}
