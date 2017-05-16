package pipeline

import (
	"testing"
	"fmt"
	"github.ibm.com/Joseph-Runde/pipe-and-filter/examples/filters"
	f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"
)

func TestSingleFilterPipeline(t *testing.T) {

	input := make(chan int, 10)

	pipe, err := New(input, []f.Filter{filters.Cumulator{}})
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	input <- 1
	input <- 7
	input <- 10
	close(input)

	outs, _ := pipe.Run()
	if outs[0] != 18 {
		t.Fail()
	}

}

func TestTwoFilterPipeline(t *testing.T) {

	input := make(chan int, 10)

	pipe, err := New(input, []f.Filter{filters.Doubler{}, filters.Cumulator{}})
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	input <- 1
	input <- 7
	input <- 10
	close(input)

	outs, _ := pipe.Run()
	if outs[0] != 36 {
		t.Fail()
	}
}

func TestParallelPipeline(t *testing.T) {

	input := make(chan string, 100)

	pipe, err := New(input, []f.Filter{filters.Atoi_parallel{}, filters.Doubler{}, filters.Cumulator{}})
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	i := 0
	for i < 50 {
		input <- "1"
		i += 1
	}
	close(input)

	outs, _ := pipe.Run()
	if outs[0] != 100 {
		t.Fail()
	}

}

func TestErrorReporting(t *testing.T) {
	input := make(chan string, 100)

	pipe, err := New(input, []f.Filter{filters.Atoi_parallel{}, filters.Doubler{}, filters.Cumulator{}})
	if err != nil {
		fmt.Println(err)
		t.Fail()
		return
	}

	input <- "1"
	input <- "two"
	input <- "3"
	input <- "four"
	close(input)

	_, errs := pipe.Run()
	if len(errs) != 2 {
		t.Fail()
	}
	if errs[0].Code() != filters.ATOI_ERROR_NOT_A_NUMBER {
		t.Fail()
	}
	if errs[1].Code() != filters.ATOI_ERROR_NOT_A_NUMBER {
		t.Fail()
	}

}

