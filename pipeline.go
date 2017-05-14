package pipe_and_filter

import (
	"fmt"
	"reflect"
)

type Pipeline interface {
	Run() ([]FilterOutput, []CodedError)
}

func New(input FilterChannel, filters []Filter) (Pipeline, error) {
	errorChan := make(chan CodedError, 10)
	filterRunners := make([]filterRunner, len(filters))
	nextInputChannel := input
	var err error

	for i, filter := range filters {
		filterRunners[i], err = newFilterRunner(filter, nextInputChannel, errorChan)
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

	return pipeline{input: input, runners: filterRunners, errorChannel: errorChan, outputChannel: wrappedOutputChannel}, nil
}

type pipeline struct {
	Pipeline

	input         FilterChannel
	runners       []filterRunner
	errorChannel  chan CodedError

	outputChannel chan FilterOutput
}

func (p pipeline) Run() ([]FilterOutput, []CodedError) {

	for _, runner := range p.runners {
		runner.Start()
	}

	//lastStepOuputChannel := (p.runners[len(p.runners)-1].GetOutputChan()).(chan interface{})
	errors := make([]CodedError, 0)
	pipelineOutput := make([]FilterOutput, 0)

	for {
		select {
		case err := <-p.errorChannel:
			errors = append(errors, err)
		case out, open := <-p.outputChannel:
			if !open {
				for len(p.errorChannel) > 0 {
					errors = append(errors, <-p.errorChannel)
				}
				return pipelineOutput, errors
			}
			pipelineOutput = append(pipelineOutput, out)
		}
	}
}
