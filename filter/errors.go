package filter

import "fmt"

type CodedError interface {
	error
	Code() int
}

type basicError struct {
	CodedError
	message string
	code int
}

func Errorf(code int, message string, a ...interface{}) CodedError {
	return basicError{message: fmt.Sprintf(message, a...), code: code}
}

func (b basicError) Error() string {
	return b.message
}

func (b basicError) Code() int {
	return b.code
}
