package filters

import (
	"strconv"
	"time"
	"fmt"
	f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"
)

type Atoi_parallel struct {
	f.Filter
}

func (a Atoi_parallel) Run(verifiedInputChan f.FilterChannel, outputChannel f.FilterChannel, errorChan chan<- f.CodedError) {
	input := (verifiedInputChan).(chan string)
	output := (outputChannel).(chan int)

	for x := range(input) {
		i, err := strconv.Atoi(x)
		time.Sleep(100 * time.Millisecond)

		if err != nil {
			errorChan <- f.Errorf(ATOI_ERROR_NOT_A_NUMBER, fmt.Sprintf("%s is not a number", x))
		}
		output <- i
	}
}

func (a Atoi_parallel) VerifyInputChannel(inputChannel f.FilterChannel) bool {
	_, ok := (inputChannel).(chan string)
	return ok
}

func (a Atoi_parallel) MakeOutputChannel() f.FilterChannel {
	return make(chan int, 100)
}

func (a Atoi_parallel) GetParallelWorkerCount() int {
	return 50;
}

const ATOI_ERROR_NOT_A_NUMBER = 747