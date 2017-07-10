package filter

import (
	"fmt"
	"reflect"
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
)

const UNREAD_INPUT_ERROR = -123456789

type FilterChannel interface{}

type FilterOutput interface{}

type Filter interface {
	Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- e.Message)
	VerifyInputChannel(inputChannel FilterChannel) bool
	MakeOutputChannel() FilterChannel
	GetParallelWorkerCount() int
}

type FilterRunner interface {
	Start()
	GetOutputChan() FilterChannel
}

type runner struct {
	FilterRunner

	filter        Filter
	errorChannel  chan e.Message
	inputChannel  FilterChannel
	outputChannel FilterChannel

	refCount      int
	closedChannel chan interface{}
}

func NewFilterRunner(filter Filter, input FilterChannel, errorChan chan e.Message) (FilterRunner, error) {

	if filter == nil {
		return nil, fmt.Errorf("Unexpected error from the pipeline: Filter cannot be nil")
	}
	if input == nil {
		return nil, fmt.Errorf("Unexpected error from the pipeline: Input cannot be nil")
	}
	if errorChan == nil {
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

	// No more filter instances are running. Hopefully they waited for the input channel to close.
	// In case they didn't, we'll eat up the rest of the inputs. NOM NOM NOM
	vi := reflect.ValueOf(r.inputChannel)
	for in, ok := vi.Recv(); ok; in, ok = vi.Recv() {
		r.errorChannel <- e.Errorf(UNREAD_INPUT_ERROR, "%v", in)
	}
}

func (r runner) wrapRun() {
	r.filter.Run(r.inputChannel, r.outputChannel, r.errorChannel)
	r.closedChannel <- 1
}

func (r runner) GetOutputChan() FilterChannel {
	return r.outputChannel
}
