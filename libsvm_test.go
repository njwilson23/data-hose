package main

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

func NewTestLibSVMWriter() (*LibSVMWriter, *bytes.Buffer) {
	buffer := bytes.Buffer{}
	return &LibSVMWriter{bufio.NewWriter(&buffer)}, &buffer
}

func TestWriteLibSVMRow(t *testing.T) {
	f, b := NewTestLibSVMWriter()

	row := &Row{Schema: []int{-1, 0, 1, 2}, Values: []string{"10.00", "1.5", "2.5", "3.5"}}
	f.WriteRow(row, &WriteOptions{targetCol: -1})
	f.buffer.Flush()

	if b.String() != "10.00 0:1.5 1:2.5 2:3.5\n" {
		t.Fail()
	}

	// Test label in different position
	f, b = NewTestLibSVMWriter()

	row = &Row{Schema: []int{1, 2, 3}, Values: []string{"2.5", "3.5", "-10"}}
	f.WriteRow(row, &WriteOptions{targetCol: 3})
	f.buffer.Flush()

	if b.String() != "-10 1:2.5 2:3.5\n" {
		t.Fail()
	}

	// Test missing column
	f, b = NewTestLibSVMWriter()

	row = &Row{Schema: []int{0, 2, 3}, Values: []string{"2.5", "3.5", "-10"}}
	f.WriteRow(row, &WriteOptions{targetCol: 3})
	f.buffer.Flush()

	if b.String() != "-10 0:2.5 2:3.5\n" {
		t.Fail()
	}
}

func TestReadLibSVMRow(t *testing.T) {
	b := &bytes.Buffer{}
	b.WriteString("0 1:4.5 3:5.6 4:-1.7 5:2.1\n")
	b.WriteString("0 1:3.3 2:1.8 3:-0.1 4:-1.7\n")
	b.WriteString("1 1:0.2 2:1.8 4:1.2\n")
	rowReader := LibSVMReader{bufio.NewReader(b)}

	options := ReadOptions{nSkipRows: 0, nRows: -1}
	row, err := rowReader.ReadRow(&options)
	if err != nil {
		fmt.Println(err)
		t.Error()
	}
	if !stringSlicesEqual(row.Values, []string{"0", "4.5", "5.6", "-1.7", "2.1"}) {
		t.Fail()
	}
	if !intSlicesEqual(row.Schema, []int{0, 2, 4, 5, 6}) {
		t.Fail()
	}

	row, err = rowReader.ReadRow(&options)
	if err != nil {
		fmt.Println(err)
		t.Error()
	}
	if !stringSlicesEqual(row.Values, []string{"0", "3.3", "1.8", "-0.1", "-1.7"}) {
		t.Fail()
	}
	if !intSlicesEqual(row.Schema, []int{0, 2, 3, 4, 5}) {
		t.Fail()
	}

	row, err = rowReader.ReadRow(&options)
	if err != nil {
		fmt.Println(err)
		t.Error()
	}
	if !stringSlicesEqual(row.Values, []string{"1", "0.2", "1.8", "1.2"}) {
		t.Fail()
	}
	if !intSlicesEqual(row.Schema, []int{0, 2, 3, 5}) {
		t.Fail()
	}

}
