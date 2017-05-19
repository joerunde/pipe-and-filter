package pipeline

import (
	"testing"
	"github.ibm.com/Joseph-Runde/pipe-and-filter/examples/filters"
	f "github.ibm.com/Joseph-Runde/pipe-and-filter/filter"
	//"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type PipelineTestSuite struct {
	suite.Suite
}

func TestPipelineTestSuite(t *testing.T) {
	suite.Run(t, new(PipelineTestSuite))
}

func (p *PipelineTestSuite) TestSingleFilterPipeline() {
	input := make(chan int, 10)

	pipe, err := New(input, []f.Filter{filters.Cumulator{}})
	p.Nil(err)

	input <- 1
	input <- 7
	input <- 10
	close(input)

	outs, _ := pipe.Run()
	p.Equal(18, outs[0])
}

func (p *PipelineTestSuite) TestTwoFilterPipeline() {
	input := make(chan int, 10)

	pipe, err := New(input, []f.Filter{filters.Doubler{}, filters.Cumulator{}})
	p.Nil(err)

	input <- 1
	input <- 7
	input <- 10
	close(input)

	outs, _ := pipe.Run()
	p.Equal(36, outs[0])
}

func (p *PipelineTestSuite) TestParallelPipeline() {
	input := make(chan string, 100)

	pipe, err := New(input, []f.Filter{filters.Atoi_parallel{}, filters.Doubler{}, filters.Cumulator{}})
	p.Nil(err)

	i := 0
	for i < 50 {
		input <- "1"
		i += 1
	}
	close(input)

	outs, _ := pipe.Run()
	p.Equal(100, outs[0])
}

func (p *PipelineTestSuite) TestErrorReporting() {
	input := make(chan string, 100)

	pipe, err := New(input, []f.Filter{filters.Atoi_parallel{}, filters.Doubler{}, filters.Cumulator{}})
	p.Nil(err)

	input <- "1"
	input <- "two"
	input <- "3"
	input <- "four"
	close(input)

	_, errs := pipe.Run()
	p.Equal(2, len(errs))
	p.Equal(filters.ATOI_ERROR_NOT_A_NUMBER, errs[0].Code())
	p.Equal(filters.ATOI_ERROR_NOT_A_NUMBER, errs[1].Code())
}
