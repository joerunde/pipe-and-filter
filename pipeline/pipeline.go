package pipeline

import (
	"fmt"
	"reflect"
	f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"
)

type Pipeline interface {
	Run() ([]f.FilterOutput, []f.CodedError)
}

func New(input f.FilterChannel, filters []f.Filter) (Pipeline, error) {
	errorChan := make(chan f.CodedError, 10)
	filterRunners := make([]f.FilterRunner, len(filters))
	nextInputChannel := input
	var err error

	for i, filter := range filters {
		filterRunners[i], err = f.NewFilterRunner(filter, nextInputChannel, errorChan)
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
	wrappedOutputChannel := make(chan f.FilterOutput, 100)
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

	input         f.FilterChannel
	runners       []f.FilterRunner
	errorChannel  chan f.CodedError

	outputChannel chan f.FilterOutput
}

func (p pipeline) Run() ([]f.FilterOutput, []f.CodedError) {

	for _, runner := range p.runners {
		runner.Start()
	}

	//lastStepOuputChannel := (p.runners[len(p.runners)-1].GetOutputChan()).(chan interface{})
	errors := make([]f.CodedError, 0)
	pipelineOutput := make([]f.FilterOutput, 0)

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
