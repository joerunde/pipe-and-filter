package pipeline

import (
	"fmt"
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
	"reflect"
	"time"
)

type Pipeline interface {
	Run() ([]FilterOutput, []e.DecoratedMessage)
}

func NewWithSource(source SourceFilter, filters []Filter, listeners []e.MessageListener) (Pipeline, error) {
	in := make(chan interface{})
	close(in)

	wrapper := sourceFilterWrapper{SourceFilter: source}
	filters = append([]Filter{wrapper}, filters...)
	return New(in, filters, listeners)
}

func New(input FilterChannel, filters []Filter, listeners []e.MessageListener) (Pipeline, error) {
	decoratedMessageChannel := make(chan e.DecoratedMessage, MESSAGE_BUFFER_SIZE)
	filterRunners := make([]filterRunner, len(filters))
	nextInputChannel := input
	var err error

	for i, filter := range filters {
		filterRunners[i], err = NewFilterRunner(filter, nextInputChannel, decoratedMessageChannel)
		if err != nil {
			return nil, err
		}
		nextInputChannel = filterRunners[i].GetOutputChan()
	}

	// make sure last step outputs a channel too
	lastChan := filterRunners[len(filterRunners)-1].GetOutputChan()
	t := reflect.TypeOf(lastChan)
	if t.Kind() != reflect.Chan {
		return nil, fmt.Errorf("Last step's output was not a channel! Unexpected type: %s", t)
	}
	// go bonkers here: wrap the last step's output in a generic channel, so we can accumulate outputs alongside errors
	wrappedOutputChannel := make(chan FilterOutput, 100)
	go func() {
		v := reflect.ValueOf(lastChan)
		for {
			x, ok := v.Recv()
			if !ok {
				close(wrappedOutputChannel)
				return
			}
			wrappedOutputChannel <- x.Interface()
		}
	}()

	return pipeline{
		input:            input,
		runners:          filterRunners,
		messageChannel:   decoratedMessageChannel,
		outputChannel:    wrappedOutputChannel,
		messageListeners: listeners,
	}, nil
}

type pipeline struct {
	Pipeline

	input            FilterChannel
	runners          []filterRunner
	messageListeners []e.MessageListener
	messageChannel   chan e.DecoratedMessage

	outputChannel chan FilterOutput
}

func (p pipeline) Run() ([]FilterOutput, []e.DecoratedMessage) {
	// TODO: Send a periodic message with the sizes of all the runners' output channels

	startTime := time.Now()
	for _, runner := range p.runners {
		runner.Start(startTime)
	}

	//lastStepOuputChannel := (p.runners[len(p.runners)-1].GetOutputChan()).(chan interface{})
	messages := make([]e.DecoratedMessage, 0)
	pipelineOutput := make([]FilterOutput, 0)

	for {
		select {
		case msg := <-p.messageChannel:
			messages = p.handleMessage(messages, msg)
		case out, open := <-p.outputChannel:
			if !open {
				for len(p.messageChannel) > 0 {
					messages = p.handleMessage(messages, <-p.messageChannel)
				}
				endOfPipeMessage := e.DecoratedMessage{
					Message:       e.Format(e.PIPELINE_COMPLETE, "Pipeline complete"),
					Written:       time.Now(),
					Source:        e.PIPELINE,
					PipelineStart: startTime,
				}
				messages = p.handleMessage(messages, endOfPipeMessage)
				return pipelineOutput, messages
			}
			pipelineOutput = append(pipelineOutput, out)
		}
	}
}

func (p pipeline) handleMessage(msgs []e.DecoratedMessage, msg e.DecoratedMessage) []e.DecoratedMessage {
	for _, l := range p.messageListeners {
		l.Handle(msg)
		// TODO: notify user of which errors were handled and which were not, probably in another message
	}

	return append(msgs, msg)
}
