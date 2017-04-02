package main

// ColumnNames is a collection of column name strings
type ColumnNames []string

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
type ReadableFormat interface {
	ReadRow(*rowReadOptions) (*Row, error)
}

type WriteableFormat interface {
	WriteRow(*Row, *rowWriteOptions) error
}

type rowReadOptions struct {
}

type rowWriteOptions struct {
	targetCol int
	precision int
}
