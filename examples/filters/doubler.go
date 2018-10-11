package filters

import (
	"github.ibm.com/Joseph-Runde/pipe-and-filter/pipeline"
)

type Doubler struct {
	pipeline.Filter
}

func (d Doubler) Run(verifiedInputChan pipeline.FilterChannel, outputChannel pipeline.FilterChannel, errorChan chan<- pipeline.Message) {
	input := (verifiedInputChan).(chan int)
	output := (outputChannel).(chan int)

	for x := range(input) {
		output <- x*2
	}
}

func (d Doubler) VerifyInputChannel(inputChannel pipeline.FilterChannel) bool {
	_, ok := (inputChannel).(chan int)
	return ok
}

func (d Doubler) MakeOutputChannel() pipeline.FilterChannel {
	return make(chan int, 10)
}

func (d Doubler) GetParallelWorkerCount() int {
	return 1;
}
