package main

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

type CSVReader struct {
	buffer *bufio.Reader
}

type CSVWriter struct {
	buffer *bufio.Writer
}

func (rowReader *CSVReader) ReadRow(options *rowReadOptions) (*Row, error) {
	buffer := bytes.Buffer{}

	line, err := rowReader.buffer.ReadString('\n')
	if err != nil {
		return &Row{}, err
	}
	if len(line) == 1 {
		return &Row{}, EMPTY_LINE_ERROR
	}

	var s string
	colNum := 0
	schema := []int{}
	values := []string{}
	for _, rn := range line {
		switch rn {
		case ',':
			s = strings.Trim(buffer.String(), " \t\r\n")
			buffer.Reset()
			if len(s) != 0 {
				schema = append(schema, colNum)
				values = append(values, s)
			}
			colNum++
		default:
			buffer.WriteRune(rn)
		}
	}
	s = strings.Trim(buffer.String(), " \t\r\n")
	buffer.Reset()
	if len(s) != 0 {
		schema = append(schema, colNum)
		values = append(values, s)
	}
	return &Row{Schema: schema, Values: values}, nil
}

func (rowWriter *CSVWriter) WriteRow(row *Row, options *rowWriteOptions) error {
	buffer := bytes.Buffer{}
	col := 0
	for i := 0; i != len(*row.Names); i++ {
		if (col < len(row.Schema)) && (i == row.Schema[col]) {
			_, err := buffer.WriteString(row.Values[col])
			if err != nil {
				return err
			}
			col++
		}
		if i < len(*row.Names)-1 {
			_, err := buffer.WriteRune(',')
			if err != nil {
				return err
			}
		}
	}
	_, err := buffer.WriteRune('\n')
	if err != nil {
		return err
	}
	rowWriter.buffer.WriteString(buffer.String())
	return nil
}

func readCSV(reader *bufio.Reader, options *rowReadOptions) (*Section, error) {
	for i := 0; i != options.nSkipCols; i++ {
		reader.ReadString('\n')
	}

	var row *Row
	var err error
	var capacity int
	if options.nReadCols < 0 {
		capacity = 10
	} else {
		capacity = options.nReadCols
	}
	section := Section(make([]Row, 0, capacity))
	rowReader := CSVReader{reader}
	for i := 0; i != options.nReadCols; i++ {
		row, err = rowReader.ReadRow(options)
		if err == io.EOF {
			break
		} else if err == EMPTY_LINE_ERROR {
			// Skip
		} else if err != nil {
			return &section, err
		} else {
			section = append(section, *row)
		}
	}
	return &section, nil
}
