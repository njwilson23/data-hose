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

func TestWriteLibSVMRow(t *testing.T) {
	f, b := NewTestLibSVMWriter()

	row := &Row{ColumnNames: []string{"A", "B", "C", "D"}, Values: []string{"10.00", "1.5", "2.5", "3.5"}}
	f.Write(row, 0)
	f.Flush()

	if b.String() != "10.00 0:1.5 1:2.5 2:3.5\n" {
		t.Fail()
	}

	// Test label in different position
	f, b = NewTestLibSVMWriter()

	row = &Row{ColumnNames: []string{"A", "B", "C"}, Values: []string{"2.5", "3.5", "-10"}}
	f.Write(row, 2)
	f.Flush()

	if b.String() != "-10 0:2.5 1:3.5\n" {
		t.Fail()
	}

	// Test missing column
	f, b = NewTestLibSVMWriter()

}
