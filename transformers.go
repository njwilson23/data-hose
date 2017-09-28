package main

import (
	"errors"
	"strings"
)

// Transformer is a function that takes a Row pointer and a row number and returns a Row pointer
type Transformer func(input <-chan *Row, output chan<- *Row)

// Filter is a function that takes a row and indicates whether it is to be accepted or excluded
type Filter func(*Row) bool

// RowSkipper returns a transformation that skips *n* rows
func RowSkipper(n int) Transformer {
	return func(input <-chan *Row, output chan<- *Row) {
		count := 0
		for row := range input {
			if count >= n {
				output <- row
			}
			count++
		}
		close(output)
	}
}

// RowLimiter returns a transformation that stops after *n* rows
func RowLimiter(n int) Transformer {
	return func(input <-chan *Row, output chan<- *Row) {
		count := 0
		for row := range input {
			output <- row
			count++
			if count == n {
				break
			}
		}
		close(output)
	}
}

func contains(set []int, item int) bool {
	for _, i := range set {
		if i == item {
			return true
		}
	}
	return false
}

func argin(args []string, m string) (int, error) {
	for i, a := range args {
		if a == m {
			return i, nil
		}
	}
	return -1, errors.New("not found")
}

// ColumnIntSelector creates a Transformer that retains a subset of columns based on column indices
func ColumnIntSelector(indices []int) Transformer {
	return func(input <-chan *Row, output chan<- *Row) {
		columns := make([]string, len(indices))
		row := <-input
		if row == nil {
			// There are no rows to process, so shutter
			close(output)
			return
		}
		for i, idx := range indices {
			if idx >= len(row.ColumnNames) {
				panic("column index out of range")
			}
			columns[i] = row.ColumnNames[idx]
		}

		values := make([]string, len(indices))
		for i, idx := range indices {
			values[i] = row.Values[idx]
		}
		newRow := &Row{columns, values}
		output <- newRow

		for row := range input {
			values = make([]string, len(indices))
			for i, idx := range indices {
				values[i] = row.Values[idx]
			}
			newRow = &Row{columns, values}
			output <- newRow
		}
		close(output)
	}
}

// ColumnStringSelector creates a Transformer that retains a subset of columns
func ColumnStringSelector(columns []string) Transformer {
	return func(input <-chan *Row, output chan<- *Row) {
		indices := make([]int, len(columns))
		row := <-input
		if row == nil {
			// There are no rows to process, so shutter
			close(output)
			return
		}
		for i, col := range columns {
			idx, err := argin(row.ColumnNames, col)
			if err != nil {
				panic(err)
			}
			indices[i] = idx
		}

		values := make([]string, len(indices))
		for i, idx := range indices {
			values[i] = row.Values[idx]
		}
		newRow := &Row{columns, values}
		output <- newRow

		for row := range input {
			values = make([]string, len(indices))
			for i, idx := range indices {
				values[i] = row.Values[idx]
			}
			newRow = &Row{columns, values}
			output <- newRow
		}
		close(output)
	}
}

func predicateAsFunc(predicate string) Filter {
	parts := strings.SplitN(predicate, "=", 2)
	if len(parts) != 2 {
		panic("misunderstood predicate")
	}
	return func(row *Row) bool {
		colIdx, err := argin(row.ColumnNames, strings.TrimSpace(parts[0]))
		if err != nil {
			panic(err)
		}
		return row.Values[colIdx] == strings.TrimSpace(parts[1])
	}
}

// Predicator converts a string predicate into a function that applies it
func Predicator(predicate string) Transformer {
	filter := predicateAsFunc(predicate)
	return func(input <-chan *Row, output chan<- *Row) {
		for row := range input {
			if filter(row) {
				output <- row
			}
		}
		close(output)
	}
}

// IdentityTransformer passes input through unmodified
func IdentityTransformer(input <-chan *Row, output chan<- *Row) {
	for row := range input {
		output <- row
	}
	close(output)
}
