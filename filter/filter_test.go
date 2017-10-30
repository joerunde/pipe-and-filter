package filter

import (
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
	e "github.ibm.com/Joseph-Runde/pipe-and-filter/pipe_messages"
)

type FilterRunnerTestSuite struct {
	suite.Suite
	mock *MockFilter
}

func TestFilterTestSuite(t *testing.T) {
	suite.Run(t, new(FilterRunnerTestSuite))
}

func (f *FilterRunnerTestSuite) SetupTest() {
	f.mock = new(MockFilter)
}

func (f *FilterRunnerTestSuite) TestItRejectsNonChannelInputTypes() {
	notAChannel := "not a channel"
	runner, err := NewFilterRunner(f.mock, notAChannel, make(chan e.DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsWrongInputChannelTypes() {
	f.mock.On("VerifyInputChannel").Return(false).Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan e.DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsWorkerCountsLessThanOne() {
	f.mock.On("VerifyInputChannel").Return(false).Once()
	f.mock.On("GetParallelWorkerCount").Return(0).Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan e.DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsNonChannelOutputTypes() {
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1).Once()
	f.mock.On("MakeOutputChannel").Return("not a channel").Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan e.DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItCreatesAFilterRunner() {
	output := make(chan interface{})
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1).Once()
	f.mock.On("MakeOutputChannel").Return(output).Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan e.DecoratedMessage))
	f.NotNil(runner)
	f.Nil(err)
	f.Equal(output, runner.GetOutputChan())
}

func (f *FilterRunnerTestSuite) TestItEatsUnusedInputsAndReportsErrorsForEach() {
	output := make(chan interface{})
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1) // Called more times while running
	f.mock.On("MakeOutputChannel").Return(output).Once()

	inputChannel := make(chan int, 3)
	inputChannel <- 1
	inputChannel <- 2
	inputChannel <- 3
	close(inputChannel)

	decoratedMessageChan := make(chan e.DecoratedMessage, 100) // space for errors
	runner, _ := NewFilterRunner(f.mock, inputChannel, decoratedMessageChan)
	f.NotNil(runner)

	runner.Start()
	// Wait for the output to be closed after the filter's Run() is called
	<- output

	// We'll need to wait a tiny bit more, since the output is closed before unused inputs are eaten
	time.Sleep(time.Millisecond)

	f.Equal(0, len(inputChannel))
	f.Equal(4, len(decoratedMessageChan))
	f.Equal(e.UNREAD_INPUT_ERROR, (<-decoratedMessageChan).Code())
	f.Equal(e.UNREAD_INPUT_ERROR, (<-decoratedMessageChan).Code())
	f.Equal(e.UNREAD_INPUT_ERROR, (<-decoratedMessageChan).Code())
	f.Equal(e.FILTER_COMPLETE, (<-decoratedMessageChan).Code())
}

// ********************************
// Mock Setup
// ********************************

type MockFilter struct {
	mock.Mock
}

func (m *MockFilter) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- e.Message) {
	//args := m.Called()
	return
}

func (m *MockFilter) VerifyInputChannel(inputChannel FilterChannel) bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockFilter) MakeOutputChannel() FilterChannel {
	args := m.Called()
	return args.Get(0)
}

func (m *MockFilter) GetParallelWorkerCount() int {
	args := m.Called()
	return args.Int(0)
}
