package pipe_messages

import "fmt"

type Message interface {
	error
	Code() int
}

type basicMessage struct {
	Message
	message string
	code int
}

func Errorf(code int, message string, a ...interface{}) Message {
	return basicMessage{message: fmt.Sprintf(message, a...), code: code}
}

func (b basicMessage) Error() string {
	return b.message
}

func (b basicMessage) Code() int {
	return b.code
}
