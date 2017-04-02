package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestMerge(t *testing.T) {
	inputA := bytes.Buffer{}
	inputA.WriteString("line 1, A\n")
	inputA.WriteString("line 2, A\n")
	inputB := bytes.Buffer{}
	inputB.WriteString("line 1, B\n")
	inputB.WriteString("line 2, B\n")

	output := bytes.Buffer{}
	outputBuffer := bufio.NewWriter(&output)
	err := merge([]io.Reader{&inputA, &inputB}, outputBuffer)
	if err != nil {
		fmt.Println(err)
		t.Error()
	}
	if output.String() != "line 1, A\nline 2, A\nline 1, B\nline 2, B\n" {
		t.Fail()
	}
}