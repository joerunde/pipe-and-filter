package filter

type CodedError interface {
	error
	Code() int
}

type basicError struct {
	CodedError
	message string
	code int
}

func Errorf(code int, message string) CodedError {
	return basicError{message: message, code: code}
}

func (b basicError) Error() string {
	return b.message
}

func (b basicError) Code() int {
	return b.code
}
