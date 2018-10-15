package pipeline

import (
	"fmt"
	"time"
)

// The pipeline contains a message bus, which can be written to by any of your filters.
// Subscribing to messages can be very useful for handling error conditions, or adding operational visibility.
// For example:
// - When a remote resource required by a filter is unavailable, you may want to alert a data orchestration service to
//	place that batch of data back into the pipeline later
// - If a batch of data has irreconcilable errors caused by bad input, you may want to push a notification to a user to
//	help them fix the mistakes
// - For long running or failure-prone pipelines, a subscriber can be used to track the status of batches of data and
//	provide this to a healthcheck endpoint or heartbeat service
// - Instrumenting a subscriber with metrics support is a simple way to decouple your business logic from your
//	operational visibility
type MessageSubscriber interface {
	// Handle will be called for all messages written by filters or the pipeline framework
	// Return true if anything was done with the message, otherwise false
	Handle(msg DecoratedMessage) bool
	// TODO: add actual subscriptions :P
}

type Message interface {
	error
	Code() int
}

// Messages written by the filters will be decorated with some extra information.
// This allows message handlers to see which filter wrote a message, and when it was written relative to the start of the pipeline.
// The pipeline may also write its own messages to the message handlers, which are denoted with `Source:PIPELINE`
type DecoratedMessage struct {
	Message
	Source        MessageSource
	FilterType    string
	Written       time.Time
	PipelineStart time.Time
}

type MessageSource int

const (
	PIPELINE MessageSource = iota
	FILTER
)

// Codes for pipeline-sourced messages which will also be written to your message handlers
const (
	// This message is sent for each input that is still waiting to be read after all of your filter's workers finish
	UNREAD_INPUT_ERROR = iota - 123456789

	// This message is sent after all of a filter's workers return
	FILTER_COMPLETE

	// This message is sent after the last filter in the pipeline closes its output
	PIPELINE_COMPLETE
)

type basicMessage struct {
	Message
	message string
	code int
}

func Format(code int, message string, a ...interface{}) Message {
	return basicMessage{message: fmt.Sprintf(message, a...), code: code}
}

func (b basicMessage) Error() string {
	return b.message
}

func (b basicMessage) Code() int {
	return b.code
}
