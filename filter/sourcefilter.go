package filter

import (
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_error"
)

type SourceFilter interface {
	Run(outputChannel FilterChannel, errorChan chan<- e.CodedError)
	MakeOutputChannel() FilterChannel
	GetParallelWorkerCount() int
}

type SourceFilterWrapper struct {
	SourceFilter
}

func (s SourceFilterWrapper) VerifyInputChannel(inputChannel FilterChannel) bool {
	return true
}

func (s SourceFilterWrapper) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- e.CodedError) {
	s.SourceFilter.Run(outputChannel, errorChan)
}
