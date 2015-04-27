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

func (s *URLHandlerSuite) TestURLFunction() {
	assert.Equal(s.T(), "disque://example.com", url("example.com"))
}
