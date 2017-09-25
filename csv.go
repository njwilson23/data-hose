package main

import (
	"encoding/csv"
	"io"
)

func readInputRows(reader io.Reader, ch chan<- *Row) {
	csvReader := csv.NewReader(reader)
	columnNames, err := csvReader.Read()
	if err != nil {
		panic(err)
	}
	rowCount := 0

	var row *Row
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			close(ch)
			break
		} else if err != nil {
			panic(err)
		}
		row = &Row{columnNames, record}
		ch <- row
		rowCount++
	}
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
