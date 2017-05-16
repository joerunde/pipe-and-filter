package filter

import (
	"testing"
	"github.com/stretchr/testify/suite"
	"github.com/stretchr/testify/mock"
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
	runner, err := NewFilterRunner(f.mock, notAChannel, make(chan CodedError))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsWrongInputChannelTypes() {
	f.mock.On("VerifyInputChannel").Return(false).Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan CodedError))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsWorkerCountsLessThanOne() {
	f.mock.On("VerifyInputChannel").Return(false).Once()
	f.mock.On("GetParallelWorkerCount").Return(0).Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan CodedError))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItRejectsNonChannelOutputTypes() {
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1).Once()
	f.mock.On("MakeOutputChannel").Return("not a channel").Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan CodedError))
	f.Nil(runner)
	f.NotNil(err)
}

func (f *FilterRunnerTestSuite) TestItCreatesAFilterRunner() {
	output := make(chan interface{})
	f.mock.On("VerifyInputChannel").Return(true).Once()
	f.mock.On("GetParallelWorkerCount").Return(1).Once()
	f.mock.On("MakeOutputChannel").Return(output).Once()
	runner, err := NewFilterRunner(f.mock, make(chan interface{}), make(chan CodedError))
	f.NotNil(runner)
	f.Nil(err)
	f.Equal(output, runner.GetOutputChan())
}

// ********************************
// Mock Setup
// ********************************

type MockFilter struct {
	mock.Mock
}

func (m *MockFilter) Run(verifiedInputChan FilterChannel, outputChannel FilterChannel, errorChan chan<- CodedError) {
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
