package filters

import (
	"strconv"
	"time"
	"github.ibm.com/Joseph-Runde/pipe-and-filter/pipeline"
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
)

type Atoi_parallel struct {
	pipeline.Filter
}

func (a Atoi_parallel) Run(verifiedInputChan pipeline.FilterChannel, outputChannel pipeline.FilterChannel, errorChan chan<- e.Message) {
	input := (verifiedInputChan).(chan string)
	output := (outputChannel).(chan int)

	for x := range(input) {
		i, err := strconv.Atoi(x)
		time.Sleep(100 * time.Millisecond)

		if err != nil {
			errorChan <- e.Format(ATOI_ERROR_NOT_A_NUMBER, "%v is not a number", x)
		} else {
			output <- i
		}
	}
}

func (a Atoi_parallel) VerifyInputChannel(inputChannel pipeline.FilterChannel) bool {
	_, ok := (inputChannel).(chan string)
	return ok
}

func (a Atoi_parallel) MakeOutputChannel() pipeline.FilterChannel {
	return make(chan int, 100)
}

func (a Atoi_parallel) GetParallelWorkerCount() int {
	return 50;
}

const ATOI_ERROR_NOT_A_NUMBER = 747
