package disque

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type MapSorterSuite struct {
	suite.Suite
}

func TestMapSorterSuite(t *testing.T) {
	suite.Run(t, new(MapSorterSuite))
}

func (s *MapSorterSuite) SetupTest() {
}

func (s *MapSorterSuite) SetupSuite() {
}

func (s *MapSorterSuite) TestReverseSortMapByValueWith3Entries() {
	m := make(map[string]int)
	m["host1"] = 500
	m["host2"] = 600
	m["host3"] = 550

	sortedPairs := reverseSortMapByValue(m)
	assert.Equal(s.T(), 3, len(sortedPairs))
	assert.Equal(s.T(), "host2", sortedPairs[0].Key)
	assert.Equal(s.T(), 600, sortedPairs[0].Value)
}

func (s *MapSorterSuite) TestReverseSortMapByValueWithZeroEntries() {
	m := make(map[string]int)

	sortedPairs := reverseSortMapByValue(m)
	assert.Equal(s.T(), 0, len(sortedPairs))
}
