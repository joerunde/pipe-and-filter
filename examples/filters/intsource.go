package filters

import (
	f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_error"
)

const INT_SOURCE_TOTAL = 6

type IntSource struct {
	f.SourceFilter
}

func (d IntSource) Run(outputChannel f.FilterChannel, errorChan chan<- e.CodedError) {
	output := (outputChannel).(chan int)

	output <- 1;
	output <- 2;
	output <- 3;
}

func (d IntSource) MakeOutputChannel() f.FilterChannel {
	return make(chan int, 10)
}

func (d IntSource) GetParallelWorkerCount() int {
	return 1;
}
