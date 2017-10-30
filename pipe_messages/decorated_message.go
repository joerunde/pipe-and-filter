package pipe_messages

import "time"

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
