package pipe_error

type MessageListener interface {
	// Handle will be called for all messages written by filters
	// Return true if anything was done with the message, otherwise false
	Handle(msg Message) bool
}
