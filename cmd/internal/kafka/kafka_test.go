package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//TestGetTransferChan - Unit Test
func TestGetTransferChan(t *testing.T) {
	//Test if we get a channel and it's the right size
	c := GetTransferChan(10)

	assert.NotEqual(t, nil, c, "The return value of GetTransferChan was nil")

	assert.Equal(t, 10, cap(c), "The capacity of the transfer channel should match the passed parameter")
}
