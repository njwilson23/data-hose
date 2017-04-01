package main

import (
	"testing"
)

func TestReadSingleInput(t *testing.T) {
	buffer := make(chan string)
	errs := make(chan error)
	paths := []string{"testdata/sequence.txt"}
	go readInputs(paths, buffer, errs)
	count := 0
	for _ = range buffer {
		count++
	}
	_, ok := <-errs
	if ok {
		t.Error()
	}
	if count != 1001 {
		// 1001 because there is a final empty string
		t.Fail()
	}
}
