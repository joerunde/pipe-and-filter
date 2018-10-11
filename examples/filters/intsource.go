package filters

import (
	"github.ibm.com/Joseph-Runde/pipe-and-filter/pipeline"
)

const INT_SOURCE_TOTAL = 6

type IntSource struct {
	pipeline.SourceFilter
}

func (d IntSource) Run(outputChannel pipeline.FilterChannel, errorChan chan<- pipeline.Message) {
	output := (outputChannel).(chan int)

	output <- 1;
	output <- 2;
	output <- 3;
}

func (d IntSource) MakeOutputChannel() pipeline.FilterChannel {
	return make(chan int, 10)
}

func (d IntSource) GetParallelWorkerCount() int {
	return 1;
}
