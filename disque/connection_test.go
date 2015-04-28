package disque

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type URLHandlerSuite struct {
	suite.Suite
}

func TestURLHandlerSuite(t *testing.T) {
	suite.Run(t, new(URLHandlerSuite))
}

func (s *URLHandlerSuite) SetupTest() {
}

func (s *URLHandlerSuite) setupEnvVars() {
}

func (s *URLHandlerSuite) SetupSuite() {
	s.setupEnvVars()
}

func (s *URLHandlerSuite) TestInitWithOneNode() {
	hosts := []string{"127.0.0.1:7711"}
	d := NewDisque(hosts)
	assert.NotNil(s.T(), d)

	d.Initialize()
	assert.EqualValues(s.T(), 1, len(d.nodes))
}

func (s *URLHandlerSuite) TestInitWithZeroNodes() {
	hosts := []string{"127.0.0.1:8800"}
	d := NewDisque(hosts)
	assert.NotNil(s.T(), d)

	assert.NotNil(s.T(), d.Initialize())
	assert.EqualValues(s.T(), 0, len(d.nodes))
}
