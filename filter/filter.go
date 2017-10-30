package filter

import (
	"fmt"
	m "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
	"reflect"
	"time"
)

const MESSAGE_BUFFER_SIZE = 1024

type FilterChannel interface{}

type FilterOutput interface{}

type Filter interface {
	Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- m.Message)
	VerifyInputChannel(inputChannel FilterChannel) bool
	MakeOutputChannel() FilterChannel
	GetParallelWorkerCount() int
}

type FilterRunner interface {
	Start(pipelineStartTimestamp time.Time)
	GetOutputChan() FilterChannel
}

type runner struct {
	FilterRunner

	filter                  Filter
	messageChannel          chan m.Message
	decoratedMessageChannel chan m.DecoratedMessage
	inputChannel            FilterChannel
	outputChannel           FilterChannel

	refCount                int
	finishedWorkersChannel  chan interface{}
	finishedMessagesChannel chan interface{}
	pipelineStart           time.Time
}

func NewFilterRunner(filter Filter, input FilterChannel, decoratedMessageChan chan m.DecoratedMessage) (FilterRunner, error) {

	if filter == nil {
		return nil, fmt.Errorf("Unexpected error from the pipeline: Filter cannot be nil")
	}
	if input == nil {
		return nil, fmt.Errorf("Unexpected error from the pipeline: Input cannot be nil")
	}
	if decoratedMessageChan == nil {
		return nil, fmt.Errorf("Unexpected error from the pipeline: Error channel cannot be nil")
	}

	t := reflect.TypeOf(input)
	if t.Kind() != reflect.Chan {
		return nil, fmt.Errorf("Input was not a channel! Unexpected type: %T", input)
	}

	if !filter.VerifyInputChannel(input) {
		return nil, fmt.Errorf("Wrong input Channel! Unexpected type: %T", input)
	}

	if filter.GetParallelWorkerCount() < 1 {
		return nil, fmt.Errorf("GetParallelWorkerCount returned %d, work requires at least one worker", filter.GetParallelWorkerCount())
	}

	output := filter.MakeOutputChannel()
	if output == nil || reflect.TypeOf(output).Kind() != reflect.Chan {
		return nil, fmt.Errorf("MakeOutputChannel returned type: %T, output must be a channel", output)
	}

	messageChan := make(chan m.Message, MESSAGE_BUFFER_SIZE)
	return runner{
		filter:                  filter,
		messageChannel:          messageChan,
		inputChannel:            input,
		outputChannel:           output,
		decoratedMessageChannel: decoratedMessageChan,
	}, nil
}

func (r runner) Start(pipelineStartTimestamp time.Time) {
	r.pipelineStart = pipelineStartTimestamp
	r.refCount = 0
	r.finishedWorkersChannel = make(chan interface{}, r.filter.GetParallelWorkerCount())
	r.finishedMessagesChannel = make(chan interface{}, 1)
	for r.refCount < r.filter.GetParallelWorkerCount() {
		r.refCount += 1
		go r.wrapRun()
	}
	go r.monitor()
	go r.decorateMessages()
}

func (r runner) monitor() {
	for r.refCount > 0 {
		<-r.finishedWorkersChannel
		r.refCount -= 1
	}

	// No more filter workers are running. Hopefully they waited for the input channel to close.
	// In case they didn't, we'll eat up the rest of the inputs. NOM NOM NOM
	vi := reflect.ValueOf(r.inputChannel)
	for in, ok := vi.Recv(); ok; in, ok = vi.Recv() {
		r.decoratedMessageChannel <- r.decorateMessage(m.Format(m.UNREAD_INPUT_ERROR, "%v", in), m.PIPELINE)
	}
	r.decoratedMessageChannel <- r.decorateMessage(m.Format(m.FILTER_COMPLETE, "Filter complete"), m.PIPELINE)

	// Close off the message channel for this filter's workers, then wait for the signal that all messages have
	// been decorated and sent back to the pipeline
	close(r.messageChannel)
	<- r.finishedMessagesChannel

	// Unfortunate reflection during pipeline runtime. But, we're fairly certain that this is indeed a channel
	v := reflect.ValueOf(r.outputChannel)
	// Close this filter's output at the very end, after we're done with absolutely everything
	v.Close()
}

func (r runner) decorateMessages() {
	for msg := range r.messageChannel {
		r.decoratedMessageChannel <- r.decorateMessage(msg, m.FILTER)
	}
	r.finishedMessagesChannel <- 0
}

func (r runner) wrapRun() {
	r.filter.Run(r.inputChannel, r.outputChannel, r.messageChannel)
	r.finishedWorkersChannel <- 1
}

func (r runner) GetOutputChan() FilterChannel {
	return r.outputChannel
}

func (r runner) decorateMessage(msg m.Message, source m.MessageSource) m.DecoratedMessage {
	return m.DecoratedMessage{
		FilterType:    fmt.Sprintf("%T", r.filter),
		Source:        source,
		Written:       time.Now(),
		PipelineStart: r.pipelineStart,
		Message:       msg,
	}
}
