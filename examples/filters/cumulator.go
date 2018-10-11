package filters

import (
	"github.ibm.com/Joseph-Runde/pipe-and-filter/pipeline"
)


type Cumulator struct {
	pipeline.Filter
}

func (c Cumulator) VerifyInputChannel(inputChannel pipeline.FilterChannel) bool {
	_, ok := (inputChannel).(chan int)
	return ok
}

func (c Cumulator) GetParallelWorkerCount() int {
	return 1
}

func (c Cumulator) MakeOutputChannel() pipeline.FilterChannel {
	return make(chan int, 1)
}

func (c Cumulator) Run(verifiedInputChan pipeline.FilterChannel, outputChannel pipeline.FilterChannel, errorChan chan<- pipeline.Message) {
	res := 0
	input := (verifiedInputChan).(chan int)
	output := (outputChannel).(chan int)

	for x := range(input) {
		res += x
	}

	output <- res
}

