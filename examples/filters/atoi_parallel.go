package filters

import (
	"strconv"
	"time"
	f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
)

type Atoi_parallel struct {
	f.Filter
}

func (a Atoi_parallel) Run(verifiedInputChan f.FilterChannel, outputChannel f.FilterChannel, errorChan chan<- e.Message) {
	input := (verifiedInputChan).(chan string)
	output := (outputChannel).(chan int)

	for x := range(input) {
		i, err := strconv.Atoi(x)
		time.Sleep(100 * time.Millisecond)

		if err != nil {
			errorChan <- e.Errorf(ATOI_ERROR_NOT_A_NUMBER, "%v is not a number", x)
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
