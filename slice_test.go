package main

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func TestSlice(t *testing.T) {

	input := &TextReader{bufio.NewReader(bytes.NewBuffer(
		[]byte("line 1\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7\nline 8\n")))}
	output := bytes.Buffer{}
	outputBuffer := &TextWriter{bufio.NewWriter(&output)}

	err := slice(input, 2, 5, outputBuffer, &rowReadOptions{}, &rowWriteOptions{})
	if err != nil {
		fmt.Println(err)
		t.Error()
	}
	if output.String() != "line 3\nline 4\nline 5\n" {
		t.Fail()
	}
}

func TestSliceInvertedToFromError(t *testing.T) {
	input := &TextReader{bufio.NewReader(bytes.NewBuffer(
		[]byte("line 1\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7\nline 8\n")))}
	output := bytes.Buffer{}
	outputBuffer := &TextWriter{bufio.NewWriter(&output)}

	err := slice(input, 5, 2, outputBuffer, &rowReadOptions{}, &rowWriteOptions{})
	if err == nil {
		t.Fail()
	}
}

func TestSliceNegativeFromError(t *testing.T) {
	input := &TextReader{bufio.NewReader(bytes.NewBuffer(
		[]byte("line 1\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7\nline 8\n")))}
	output := bytes.Buffer{}
	outputBuffer := &TextWriter{bufio.NewWriter(&output)}

	err := slice(input, -1, 5, outputBuffer, &rowReadOptions{}, &rowWriteOptions{})
	if err == nil {
		t.Fail()
	}
}

func TestSliceOutOfRangeToError(t *testing.T) {
	input := &TextReader{bufio.NewReader(bytes.NewBuffer(
		[]byte("line 1\nline 2\nline 3\nline 4\nline 5\nline 6\nline 7\nline 8\n")))}
	output := bytes.Buffer{}
	outputBuffer := &TextWriter{bufio.NewWriter(&output)}

	err := slice(input, 11, 15, outputBuffer, &rowReadOptions{}, &rowWriteOptions{})
	if err == nil {
		t.Fail()
	}
}
