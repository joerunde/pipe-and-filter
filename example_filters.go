package pipe_and_filter

import (
	"strconv"
	"time"
	"fmt"
)



//Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- CodedError)
//VerifyInputChannel(inputChannel FilterChannel) bool
//MakeOutputChannel() FilterChannel
//GetParallelWorkerCount() int

type atoi_parallel struct {
	Filter
}

func (a atoi_parallel) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- CodedError) {
	input := (verifiedInputChan).(chan string)
	output := (outputChannel).(chan int)

	for x := range(input) {
		i, err := strconv.Atoi(x)
		time.Sleep(100 * time.Millisecond)

		if err != nil {
			errorChan <- Errorf(ATOI_ERROR_NOT_A_NUMBER, fmt.Sprintf("%s is not a number", x))
		}
		output <- i
	}
}

func (a atoi_parallel) VerifyInputChannel(inputChannel FilterChannel) bool {
	_, ok := (inputChannel).(chan string)
	return ok
}

func (a atoi_parallel) MakeOutputChannel() FilterChannel {
	return make(chan int, 100)
}

func (a atoi_parallel) GetParallelWorkerCount() int {
	return 50;
}

const ATOI_ERROR_NOT_A_NUMBER = 747




type doubler struct {
	Filter
}

func (d doubler) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- CodedError) {
	input := (verifiedInputChan).(chan int)
	output := (outputChannel).(chan int)

	for x := range(input) {
		output <- x*2
	}
}

func (d doubler) VerifyInputChannel(inputChannel FilterChannel) bool {
	_, ok := (inputChannel).(chan int)
	return ok
}

func (d doubler) MakeOutputChannel() FilterChannel {
	return make(chan int, 10)
}

func (d doubler) GetParallelWorkerCount() int {
	return 1;
}





type cumulator struct {
	Filter
}

func (c cumulator) VerifyInputChannel(inputChannel FilterChannel) bool {
	_, ok := (inputChannel).(chan int)
	return ok
}

func (c cumulator) GetParallelWorkerCount() int {
	return 1
}

func (c cumulator) MakeOutputChannel() FilterChannel {
	return make(chan int, 1)
}

func (c cumulator) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- CodedError) {
	res := 0
	input := (verifiedInputChan).(chan int)
	output := (outputChannel).(chan int)

	for x := range(input) {
		res += x
	}

	output <- res
}


