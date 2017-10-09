package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
)

func getColumnNames(csvReader *csv.Reader) []string {
	columnNames, err := csvReader.Read()
	if err != nil {
		panic(err)
	}

	for i, colName := range columnNames {
		columnNames[i] = strings.TrimSpace(colName)
	}
	return columnNames
}

// readInputRows reads all rows from a slice of io.Readers, inserting them into
// a channel in order. If the column names don't match across files, a panic
// occurs
func readInputRows(readers []io.Reader, ch chan<- *Row) {

	var csvReaders []*csv.Reader
	var colNames []string
	firstFile := true

	// Ensure column names match
	for _, reader := range readers {
		csvReader := csv.NewReader(reader)
		csvReaders = append(csvReaders, csvReader)

		if firstFile {
			firstFile = !firstFile
			colNames = getColumnNames(csvReader)
			continue
		}

		nextColNames := getColumnNames(csvReader)
		for i, col := range colNames {
			if nextColNames[i] != col {
				panic(fmt.Sprintf("column mismatch in file %d\n", i))
			}
		}
	}

	rowCount := 0
	var row *Row
	for _, csvReader := range csvReaders {
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}

			for i, value := range record {
				record[i] = strings.TrimSpace(value)
			}

			row = &Row{colNames, record}
			ch <- row
			rowCount++
		}
	}
	close(ch)
	return
}

func writeCSVRows(writer io.Writer, ch <-chan *Row) error {
	csvWriter := csv.NewWriter(writer)
	first := true
	for row := range ch {
		if first {
			first = !first
			err := csvWriter.Write(row.ColumnNames)
			if err != nil {
				return err
			}
		}
		err := csvWriter.Write(row.Values)
		if err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return nil
}

func readInputRowsFast(readers []io.Reader, ch chan<- *Row) {

	for _, reader := range readers {
		err := rowsFromReaderFast(reader, ch)
		if err != nil {
			panic(err)
		}
	}
}

func rowsFromReaderFast(reader io.Reader, ch chan<- *Row) error {
	bufferedReader := bufio.NewReader(reader)

	// get column names
	b, err := bufferedReader.ReadBytes('\n')
	if err != nil {
		return err
	}
	firstRow, err := parseRow(b, []string{})
	if err != nil {
		return err
	}
	colNames := firstRow.Values

	for {
		b, err := bufferedReader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		row, err := parseRow(b, colNames)
		if err != nil {
			return err
		}
		ch <- &row
	}
	return nil
}

// parseRow takes a slice of bytes and a slice of expected columns and attempts
// to return a Row filled with values from the byte slice
func parseRow(bs []byte, columnNames []string) (Row, error) {
	var values []string
	var currentValue []byte

	maxQuoteDepth := 3
	quotes := make([]byte, 0, maxQuoteDepth)

	for _, b := range bs {

		switch b {
		case '"', '\'':
			if len(quotes) != 0 && b == quotes[len(quotes)-1] { // delete last item in quote slice
				currentValue = append(currentValue, b)
				quotes = quotes[:len(quotes)-1]
			} else if len(quotes) < maxQuoteDepth { // increment quote slice
				currentValue = append(currentValue, b)
				quotes = append(quotes, b)
			} else {
				return Row{}, errors.New("quoting exceeded max depth")
			}
		case ',':
			if len(quotes) == 0 {
				values = append(values, string(currentValue))
				currentValue = []byte{}
			} else {
				currentValue = append(currentValue, b)
			}
		default:
			currentValue = append(currentValue, b)
		}
	}
	values = append(values, string(currentValue))

	var err error
	if len(columnNames) != 0 && len(columnNames) != len(values) {
		err = errors.New("unexpected number of columns")
	}

	return Row{columnNames, values}, err
}
