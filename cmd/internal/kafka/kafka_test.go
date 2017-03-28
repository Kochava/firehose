package kafka

import (
	"testing"
)

//TestGetTransferChan - Unit Test
func TestGetTransferChan(t *testing.T) {
	//Test if we get a channel and it's the right size
	c := GetTransferChan(10)

	if c != nil {
		if cap(c) != 10 {
			t.Errorf("Capacity of channel doesn't equal passed arg. Expected <%d> got <%d>", 10, cap(c))
		}
	} else {
		t.Errorf("Returned channel was nil.")
	}
}
