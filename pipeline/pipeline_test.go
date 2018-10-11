package pipeline_test

import (
	"testing"
	"github.ibm.com/Joseph-Runde/pipe-and-filter/examples/filters"
	"github.com/stretchr/testify/suite"
	m "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
	"github.com/stretchr/testify/mock"
	. "github.ibm.com/Joseph-Runde/pipe-and-filter/pipeline"
)

type PipelineTestSuite struct {
	suite.Suite
	mock spyMessageListener
}

func TestPipelineTestSuite(t *testing.T) {
	suite.Run(t, new(PipelineTestSuite))
}

func (p *PipelineTestSuite) TestSingleFilterPipeline() {
	input := make(chan int, 10)

	pipe, err := New(input, []Filter{filters.Cumulator{}}, []m.MessageListener{})
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

	pipe, err := New(input, []Filter{filters.Doubler{}, filters.Cumulator{}}, []m.MessageListener{})
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

	pipe, err := New(input, []Filter{filters.Atoi_parallel{}, filters.Doubler{}, filters.Cumulator{}}, []m.MessageListener{})
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

func (p *PipelineTestSuite) TestPipelineWithSourceFilter() {
	pipe, err := NewWithSource(filters.IntSource{}, []Filter{filters.Cumulator{}}, []m.MessageListener{})
	p.Nil(err)

	outs, _ := pipe.Run()
	p.Equal(filters.INT_SOURCE_TOTAL, outs[0])
}

func (p *PipelineTestSuite) TestItReturnsAllMessagesAtTheEnd() {
	input := make(chan string, 100)

	pipe, err := New(input, []Filter{filters.Atoi_parallel{}, filters.Doubler{}, filters.Cumulator{}}, []m.MessageListener{})
	p.Nil(err)

	input <- "1"
	input <- "two"
	input <- "3"
	input <- "four"
	close(input)

	outs, msgs := pipe.Run()
	p.Equal(8, outs[0])
	p.Equal(6, len(msgs))

	msgMap := countMessageCodes(msgs)
	p.Equal(2, msgMap[filters.ATOI_ERROR_NOT_A_NUMBER])
	p.Equal(3, msgMap[m.FILTER_COMPLETE])
	p.Equal(1, msgMap[m.PIPELINE_COMPLETE])
}

func (p *PipelineTestSuite) TestItCallsMessageListeners() {
	input := make(chan string, 100)

	expectedNumberOfMessages := 3
	p.mock.On("Handle", mock.Anything).Return(true).Times(expectedNumberOfMessages)

	pipe, err := New(input, []Filter{filters.Atoi_parallel{}}, []m.MessageListener{&p.mock})
	p.Nil(err)

	input <- "not a number"
	close(input)

	_, msgs := pipe.Run()

	p.mock.AssertExpectations(p.T())
	p.Equal(3, len(msgs))
	msgMap := countMessageCodes(p.mock.messages)
	p.Equal(1, msgMap[filters.ATOI_ERROR_NOT_A_NUMBER])
	p.Equal(1, msgMap[m.FILTER_COMPLETE])
	p.Equal(1, msgMap[m.PIPELINE_COMPLETE])
}

func (p *PipelineTestSuite) TestItDecoratesMessagesWithTimestamps() {
	// todo
}

type spyMessageListener struct {
	mock.Mock
	messages []m.DecoratedMessage
	foobar string
}

func (s *spyMessageListener) Handle(msg m.DecoratedMessage) bool {
	s.messages = append(s.messages, msg)
	return s.Called(msg).Bool(0)
}

func countMessageCodes(msgs []m.DecoratedMessage) map[int]int {
	msgmap := make(map[int]int)
	for _, msg := range(msgs) {
		msgmap[msg.Code()] = msgmap[msg.Code()] + 1
	}
	return msgmap
}
