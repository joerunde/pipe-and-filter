package filters

import (
	f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_error"
)


type Cumulator struct {
	f.Filter
}

func (c Cumulator) VerifyInputChannel(inputChannel f.FilterChannel) bool {
	_, ok := (inputChannel).(chan int)
	return ok
}

func (c Cumulator) GetParallelWorkerCount() int {
	return 1
}

func (c Cumulator) MakeOutputChannel() f.FilterChannel {
	return make(chan int, 1)
}

func (c Cumulator) Run(verifiedInputChan f.FilterChannel, outputChannel f.FilterChannel, errorChan chan<- e.CodedError) {
	res := 0
	input := (verifiedInputChan).(chan int)
	output := (outputChannel).(chan int)

	for x := range(input) {
		res += x
	}

	output <- res
}

