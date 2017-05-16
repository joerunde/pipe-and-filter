package filters

import "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"

type Cumulator struct {
	filter.Filter
}

func (c Cumulator) VerifyInputChannel(inputChannel filter.FilterChannel) bool {
	_, ok := (inputChannel).(chan int)
	return ok
}

func (c Cumulator) GetParallelWorkerCount() int {
	return 1
}

func (c Cumulator) MakeOutputChannel() filter.FilterChannel {
	return make(chan int, 1)
}

func (c Cumulator) Run(verifiedInputChan filter.FilterChannel, outputChannel filter.FilterChannel, errorChan chan<- filter.CodedError) {
	res := 0
	input := (verifiedInputChan).(chan int)
	output := (outputChannel).(chan int)

	for x := range(input) {
		res += x
	}

	output <- res
}

