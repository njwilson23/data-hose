package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestReadSingleInput(t *testing.T) {
	buffer := make(chan string)
	errs := make(chan error)

	f, err := os.Open("testdata/sequence.txt")
	if err != nil {
		fmt.Println(err)
		t.Error()
	}
	paths := []io.Reader{f}

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

	inputs := make([]io.Reader, len(paths))
	for i, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			fmt.Println(err)
			t.Error()
		}
		inputs[i] = f
	}

	go readInputs(inputs, buffer, errs)
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

func TestHandleLines(t *testing.T) {
	buffer := make(chan string, 100)
	for i := 0; i != 100; i++ {
		buffer <- fmt.Sprintf("line %d\n", i)
	}
	b := bytes.Buffer{}
	writer := bufio.NewWriter(&b)
	err := handleLines(writer, buffer, 100)
	if err != nil {
		fmt.Println(err)
		t.Error()
	}
	for i := 0; i != 100; i++ {
		line, err := b.ReadString('\n')
		if err != nil {
			t.Error()
		}
		if line != fmt.Sprintf("line %d\n", i) {
			t.Fail()
		}
	}
}
