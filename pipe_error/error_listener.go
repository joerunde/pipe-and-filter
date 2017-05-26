package pipe_error

type ErrorListener interface {
	// HandleError will be called for all errors written by filters
	// Return true if anything was done with the error, otherwise false
	HandleError(err CodedError) bool
}
