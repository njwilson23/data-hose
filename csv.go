package main

import (
	"encoding/csv"
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
