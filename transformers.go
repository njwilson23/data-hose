package main

import (
	"errors"
)

// Transformer is a function that takes a Row pointer and a row number and returns a Row pointer
type Transformer func(input <-chan *Row, output chan<- *Row)

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

func argin(args []string, m string) (int, error) {
	for i, a := range args {
		if a == m {
			return i, nil
		}
	}
	return -1, errors.New("not found")
}

func ColumnSelector(columns []string) Transformer {
	return func(input <-chan *Row, output chan<- *Row) {
		indices := make([]int, len(columns))
		row := <-input
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

func IdentityTransformer(input <-chan *Row, output chan<- *Row) {
	for row := range input {
		output <- row
	}
	close(output)
}
