package main

import "errors"

// ColumnNames is a collection of column name strings
type ColumnNames []string

func (cn *ColumnNames) Length() int {
	cnt := 0
	for _ = range *cn {
		cnt++
	}
	return cnt
}

// Row represents a line of numerical data from a CSV or libSVM file, mapping a series of
// features to a label
type Row struct {
	Schema []int
	Values []string
	Names  *ColumnNames
}

// Section is an array of Rows representing the contens of a file or a section
// of a file
type Section []Row

// ReadableFormat is an interface for possible input formats
type RowBasedReader interface {
	ReadRow(*ReadOptions) (*Row, error)
}

type RowBasedWriter interface {
	WriteRow(*Row, *WriteOptions) error
	Flush() error
}

type ReadOptions struct {
	nSkipRows int
	nRows     int
}

type WriteOptions struct {
	targetCol int
	nRows     int
	append    bool
}

var EMPTY_LINE_ERROR = errors.New("empty line encountered")
