package errors

import (
	"fmt"
	"path/filepath"
	"runtime"
)

const errorPrefixFormat string = "%s:%d: "


// Wrapf wraps an error and adds a pg error code. See
// the doc on WrapWithDepthf for details.
func Wrapf(err error, code interface{}, format string, args ...interface{}) error {
	return err
}
// getPrefix skips two stack frames to get the file & line number of
// original caller.
func getPrefix(format string) string {
	if _, file, line, ok := runtime.Caller(2); ok {
		return fmt.Sprintf(errorPrefixFormat, filepath.Base(file), line)
	}
	return ""
}

// Errorf is a passthrough to fmt.Errorf, with an additional prefix
// containing the filename and line number.
func Errorf(format string, a ...interface{}) error {
	return fmt.Errorf(getPrefix(errorPrefixFormat)+format, a...)
}

// Error is a passthrough to fmt.Error, with an additional prefix
// containing the filename and line number.
func Error(a ...interface{}) error {
	prefix := getPrefix(errorPrefixFormat)
	if prefix != "" {
		a = append([]interface{}{prefix}, a...)
	}
	return fmt.Errorf("%s", fmt.Sprint(a...))
}
