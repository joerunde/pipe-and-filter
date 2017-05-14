package pipe_and_filter

import (
	"fmt"
	"reflect"
)

type FilterChannel interface{}

type FilterOutput interface{}

type Filter interface {
	Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- CodedError)
	VerifyInputChannel(inputChannel FilterChannel) bool
	MakeOutputChannel() FilterChannel
	GetParallelWorkerCount() int
}

type filterRunner interface {
	Start()
	GetOutputChan() FilterChannel
}

type runner struct {
	filterRunner

	filter        Filter
	errorChannel  chan CodedError
	inputChannel  FilterChannel
	outputChannel FilterChannel

	refCount      int
	closedChannel chan interface{}
}

func newFilterRunner(filter Filter, input FilterChannel, errorChan chan CodedError) (filterRunner, error) {

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
	return runner{filter: filter, errorChannel: errorChan, inputChannel: input, outputChannel: output}, nil
}

func (r runner) Start() {
	r.refCount = 0
	r.closedChannel = make(chan interface{}, r.filter.GetParallelWorkerCount())
	for r.refCount < r.filter.GetParallelWorkerCount() {
		r.refCount += 1
		go r.wrapRun()
	}
	go r.monitor()
}

func (r runner) monitor() {
	for r.refCount > 0 {
		<- r.closedChannel
		r.refCount -= 1
	}

	// Unfortunate reflection during pipeline runtime. But, we're fairly certain that this is indeed a channel
	v := reflect.ValueOf(r.outputChannel)
	v.Close()
}

func (r runner) wrapRun() {
	r.filter.Run(r.inputChannel, r.outputChannel, r.errorChannel)
	r.closedChannel <- 1
}

func (r runner) GetOutputChan() FilterChannel {
	return r.outputChannel
}
