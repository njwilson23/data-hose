package main

import (
	"fmt"
	"testing"
)

func TestIdentity(t *testing.T) {
	tochan := make(chan *Row)
	fromchan := make(chan *Row, 10)
	go IdentityTransformer(fromchan, tochan)
	for i := 0; i != 10; i++ {
		fromchan <- &Row{[]string{"id", "data"}, []string{string(i), "a"}}
	}
	close(fromchan)

	count := 0
	for row := range tochan {
		if row.Values[0] != string(count) {
			fmt.Println("row value incorrect")
			t.Fail()
		}
		count++
	}
	if count != 10 {
		fmt.Println("final count incorrect")
		t.Fail()
	}
}
