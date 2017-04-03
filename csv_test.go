package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"testing"
)

func NewTestCSVWriter() (*CSVWriter, *bytes.Buffer) {
	buffer := bytes.Buffer{}
	return &CSVWriter{bufio.NewWriter(&buffer)}, &buffer
}

func TestWriteCSVRow(t *testing.T) {
	f, b := NewTestCSVWriter()
	names := ColumnNames([]string{"a", "b", "c"})

	row := &Row{
		Schema: []int{0, 1, 2},
		Values: []string{"1.5", "2.5", "3.5"},
		Names:  &names}

	f.WriteRow(row, &rowWriteOptions{})
	f.buffer.Flush()

	if b.String() != "1.5,2.5,3.5\n" {
		t.Fail()
	}

	// missing value
	f, b = NewTestCSVWriter()
	names = ColumnNames([]string{"a", "b", "c", "d"})
	row = &Row{
		Schema: []int{0, 1, 3},
		Values: []string{"1.5", "2.5", "3.5"},
		Names:  &names}

	f.WriteRow(row, &rowWriteOptions{})
	f.buffer.Flush()

	if b.String() != "1.5,2.5,,3.5\n" {
		t.Fail()
	}
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func intSlicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReadDenseCSV(t *testing.T) {
	f, err := os.Open("testdata/test_dense.csv")
	if err != nil {
		t.Error()
	}
	fileBuffer := bufio.NewReader(f)

	section, err := readCSV(fileBuffer, &rowReadOptions{nSkipRows: 1, nRows: -1})
	if err != nil {
		fmt.Println(err)
		t.Error()
	}

	if len(*section) != 6 {
		fmt.Println(len(*section))
		t.Fail()
	}
	if !stringSlicesEqual((*section)[0].Values, []string{"0", "1", "2", "3"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[1].Values, []string{"1", "1", "0", "2"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[2].Values, []string{"0", "0.5", "0", "1"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[3].Values, []string{"0.5", "2", "0.25", "-3"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[4].Values, []string{"0", "1.5", "2", "1"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[5].Values, []string{"1", "-1", "0.5", "0.75"}) {
		t.Fail()
	}
	for _, row := range *section {
		if !intSlicesEqual(row.Schema, []int{0, 1, 2, 3}) {
			t.Fail()
		}
	}
}

func TestReadSparseCSVWithNA(t *testing.T) {
	f, err := os.Open("testdata/test_sparse_NA.csv")
	if err != nil {
		t.Error()
	}
	fileBuffer := bufio.NewReader(f)

	section, err := readCSV(fileBuffer, &rowReadOptions{nSkipRows: 1, nRows: -1})
	if err != nil {
		t.Error()
	}

	if len(*section) != 6 {
		fmt.Println(len(*section))
		t.Fail()
	}
	if !stringSlicesEqual((*section)[0].Values, []string{"0", "1", "2"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[1].Values, []string{"1", "1", "2"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[2].Values, []string{"0", "0.5", "0", "1"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[3].Values, []string{"0.5", "2", "0.25", "-3"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[4].Values, []string{"0", "2", "1"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[5].Values, []string{"1", "-1", "0.75"}) {
		t.Fail()
	}

	if !intSlicesEqual((*section)[0].Schema, []int{0, 1, 2}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[1].Schema, []int{0, 1, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[2].Schema, []int{0, 1, 2, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[3].Schema, []int{0, 1, 2, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[4].Schema, []int{0, 2, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[5].Schema, []int{0, 1, 3}) {
		t.Fail()
	}
}

func TestReadSparseCSVWithBlank(t *testing.T) {
	f, err := os.Open("testdata/test_sparse_blank.csv")
	if err != nil {
		t.Error()
	}
	fileBuffer := bufio.NewReader(f)

	section, err := readCSV(fileBuffer, &rowReadOptions{nSkipRows: 1, nRows: -1})
	if err != nil {
		t.Error()
	}

	if len(*section) != 6 {
		fmt.Println(len(*section))
		t.Fail()
	}
	if !stringSlicesEqual((*section)[0].Values, []string{"0", "1", "2"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[1].Values, []string{"1", "1", "2"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[2].Values, []string{"0", "0.5", "0", "1"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[3].Values, []string{"0.5", "2", "0.25", "-3"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[4].Values, []string{"0", "2", "1"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[5].Values, []string{"1", "-1", "0.75"}) {
		t.Fail()
	}

	if !intSlicesEqual((*section)[0].Schema, []int{0, 1, 2}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[1].Schema, []int{0, 1, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[2].Schema, []int{0, 1, 2, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[3].Schema, []int{0, 1, 2, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[4].Schema, []int{0, 2, 3}) {
		t.Fail()
	}
	if !intSlicesEqual((*section)[5].Schema, []int{0, 1, 3}) {
		t.Fail()
	}
}

func TestPartialCSV(t *testing.T) {
	f, err := os.Open("testdata/test_dense.csv")
	if err != nil {
		t.Error()
	}
	fileBuffer := bufio.NewReader(f)

	section, err := readCSV(fileBuffer, &rowReadOptions{nSkipRows: 1, nRows: 3})
	if err != nil {
		fmt.Println(err)
		t.Error()
	}

	if len(*section) != 3 {
		fmt.Println(len(*section))
		t.Fail()
	}
	if !stringSlicesEqual((*section)[0].Values, []string{"0", "1", "2", "3"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[1].Values, []string{"1", "1", "0", "2"}) {
		t.Fail()
	}
	if !stringSlicesEqual((*section)[2].Values, []string{"0", "0.5", "0", "1"}) {
		t.Fail()
	}
	for _, row := range *section {
		if !intSlicesEqual(row.Schema, []int{0, 1, 2, 3}) {
			t.Fail()
		}
	}
}
