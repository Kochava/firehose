// Copyright 2017 Kochava
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
