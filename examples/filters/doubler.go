package filters

import f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"

type Doubler struct {
	f.Filter
}

func (d Doubler) Run(verifiedInputChan f.FilterChannel, outputChannel f.FilterChannel, errorChan chan<- f.CodedError) {
	input := (verifiedInputChan).(chan int)
	output := (outputChannel).(chan int)

	for x := range(input) {
		output <- x*2
	}
}

func (d Doubler) VerifyInputChannel(inputChannel f.FilterChannel) bool {
	_, ok := (inputChannel).(chan int)
	return ok
}

func (d Doubler) MakeOutputChannel() f.FilterChannel {
	return make(chan int, 10)
}

func (d Doubler) GetParallelWorkerCount() int {
	return 1;
}
