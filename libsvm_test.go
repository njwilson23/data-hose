package main

import (
	"bufio"
	"bytes"
	"testing"
)

func NewTestLibSVMWriter() (*LibSVMWriter, *bytes.Buffer) {
	buffer := bytes.Buffer{}
	return &LibSVMWriter{bufio.NewWriter(&buffer)}, &buffer
}

func TestRowToLibSVM(t *testing.T) {
	f, b := NewTestLibSVMWriter()

	row := &Row{Schema: []int{-1, 0, 1, 2}, Values: []string{"10.00", "1.5", "2.5", "3.5"}}
	f.WriteRow(row, &rowWriteOptions{targetCol: -1})
	f.buffer.Flush()

	if b.String() != "10.00 0:1.5 1:2.5 2:3.5\n" {
		t.Fail()
	}

	// Test label in different position
	f, b = NewTestLibSVMWriter()

	row = &Row{Schema: []int{1, 2, 3}, Values: []string{"2.5", "3.5", "-10"}}
	f.WriteRow(row, &rowWriteOptions{targetCol: 3})
	f.buffer.Flush()

	if b.String() != "-10 1:2.5 2:3.5\n" {
		t.Fail()
	}

	// Test missing column
	f, b = NewTestLibSVMWriter()

	row = &Row{Schema: []int{0, 2, 3}, Values: []string{"2.5", "3.5", "-10"}}
	f.WriteRow(row, &rowWriteOptions{targetCol: 3})
	f.buffer.Flush()

	if b.String() != "-10 0:2.5 2:3.5\n" {
		t.Fail()
	}
}
