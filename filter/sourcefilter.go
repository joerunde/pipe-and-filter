package filter

import (
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
)

type SourceFilter interface {
	Run(outputChannel FilterChannel, errorChan chan<- e.Message)
	MakeOutputChannel() FilterChannel
	GetParallelWorkerCount() int
}

type SourceFilterWrapper struct {
	SourceFilter
}

func (s SourceFilterWrapper) VerifyInputChannel(inputChannel FilterChannel) bool {
	return true
}

func (s SourceFilterWrapper) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- e.Message) {
	s.SourceFilter.Run(outputChannel, errorChan)
}
