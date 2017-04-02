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
	if count != 1000 {
		t.Fail()
	}
}

func TestReadMultipleInput(t *testing.T) {
	buffer := make(chan string)
	errs := make(chan error)
	paths := []string{"testdata/sequence.txt", "testdata/sequence.txt"}
	go readInputs(paths, buffer, errs)
	count := 0
	for _ = range buffer {
		count++
	}
	_, ok := <-errs
	if ok {
		t.Error()
	}
	if count != 2000 {
		t.Fail()
	}
}
