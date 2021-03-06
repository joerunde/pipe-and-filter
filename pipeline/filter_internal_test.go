package pipeline

import (
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
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
	runner, err := newFilterRunner(f.mock, notAChannel, make(chan DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsWrongInputChannelTypes() {
	f.mock.On("VerifyInputChannel").Return(false).Once()
	runner, err := newFilterRunner(f.mock, make(chan interface{}), make(chan DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
	f.mock.AssertExpectations(f.T())
}

func (f *FilterRunnerTestSuite) TestItRejectsWorkerCountsLessThanOne() {
	f.mock.On("VerifyInputChannel").Return(false).Once()
	f.mock.On("GetParallelWorkerCount").Return(0).Once()
	runner, err := newFilterRunner(f.mock, make(chan interface{}), make(chan DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsNonChannelOutputTypes() {
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1).Once()
	f.mock.On("MakeOutputChannel").Return("not a channel").Once()
	runner, err := newFilterRunner(f.mock, make(chan interface{}), make(chan DecoratedMessage))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItCreatesAFilterRunner() {
	output := make(chan interface{})
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1).Once()
	f.mock.On("MakeOutputChannel").Return(output).Once()
	runner, err := newFilterRunner(f.mock, make(chan interface{}), make(chan DecoratedMessage))
	f.NotNil(runner)
	f.Nil(err)
	f.Equal(output, runner.GetOutputChan())
}

func (f *FilterRunnerTestSuite) TestItRunsAFilterInParallel() {
	expectedParallelCalls := 100
	output := make(chan interface{})
	input := make(chan interface{})

	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(expectedParallelCalls)
	f.mock.On("MakeOutputChannel").Return(output).Once()
	f.mock.On("Run").Return().Times(expectedParallelCalls) //This should be called as many times as GetParallelWorkerCount

	runner, err := newFilterRunner(f.mock, input, make(chan DecoratedMessage, expectedParallelCalls * 2))
	f.NotNil(runner)
	f.Nil(err)

	runner.Start(time.Now())
	close(input)
	<- output

	f.mock.AssertExpectations(f.T())
}

func (f *FilterRunnerTestSuite) TestItEatsUnusedInputsAndReportsErrorsForEach() {
	output := make(chan interface{})
	f.mock.On("Run").Return()
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1) // Called more times while running
	f.mock.On("MakeOutputChannel").Return(output).Once()

	inputChannel := make(chan int, 3)
	inputChannel <- 1
	inputChannel <- 2
	inputChannel <- 3
	close(inputChannel)

	decoratedMessageChan := make(chan DecoratedMessage, 100) // space for errors
	runner, _ := newFilterRunner(f.mock, inputChannel, decoratedMessageChan)
	f.NotNil(runner)

	runner.Start(time.Now())
	// Wait for the output to be closed after the filter's Run() is called
	<- output

	// We'll need to wait a tiny bit more, since the output is closed before unused inputs are eaten
	time.Sleep(time.Millisecond)

	f.Equal(0, len(inputChannel))
	f.Equal(4, len(decoratedMessageChan))
	f.Equal(UNREAD_INPUT_ERROR, (<-decoratedMessageChan).Code())
	f.Equal(UNREAD_INPUT_ERROR, (<-decoratedMessageChan).Code())
	f.Equal(UNREAD_INPUT_ERROR, (<-decoratedMessageChan).Code())
	f.Equal(FILTER_COMPLETE, (<-decoratedMessageChan).Code())
}

// ********************************
// Mock Setup
// ********************************

type MockFilter struct {
	mock.Mock
}

func (m *MockFilter) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- Message) {
	m.Called()
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
