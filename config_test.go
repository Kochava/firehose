package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//TestInitConfig - Unit Test
func TestInitConfig(t *testing.T) {
	//Test if we return an empty config obj
	c := InitConfig()
	assert.Equal(t, Config{}, c, "These should be equal")
}
