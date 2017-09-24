package main

import "errors"

/* This file declares the primary data structures used throughout */

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

// RowBasedReader is an interface for possible input formats
type RowBasedReader interface {
	ReadRow(*ReadOptions) (*Row, error)
}

type RowBasedWriter interface {
	Init(*ColumnNames, []int) error
	WriteRow(*Row, *WriteOptions) error
	Flush() error
}

// ReadOptions describes the parameters that may be required by a reader
type ReadOptions struct {
	nSkipRows int // how many rows should be skipped before reading?
	nRows     int // how many rows should be read?
}

// WriteOptions describes the parameters that may be required by a writer
type WriteOptions struct {
	targetCol int  // which column is the target value (libSVM)
	nRows     int  // how many rows should be written
	append    bool // is this an append operation?
	header    bool // should the writer begin with a header, if applicable?
}

// EmptyLineError indicates that a line contained no content
var EmptyLineError = errors.New("empty line encountered")
