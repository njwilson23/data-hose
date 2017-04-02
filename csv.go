package main

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
)

type CSVFormat struct {
	readBuffer  bufio.Reader
	writeBuffer bufio.Writer
}

func (f *CSVFormat) ReadRow(options *rowReadOptions) (*Row, error) {
	buffer := bytes.Buffer{}

	line, err := f.readBuffer.ReadString('\n')
	if err != nil {
		return &Row{}, err
	}

	var s string
	colNum := 0
	schema := []int{}
	values := []string{}
	for _, rn := range line {
		switch rn {
		case ',':
			s = strings.Trim(buffer.String(), " \t\r")
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
	s = strings.Trim(buffer.String(), " \t\r")
	buffer.Reset()
	if len(s) != 0 {
		schema = append(schema, colNum)
		values = append(values, s)
	}
	return &Row{Schema: schema, Values: values}, nil
}

func (f *CSVFormat) WriteRow(row *Row, options *rowWriteOptions) error {
	return errors.New("CSV write not implemented")
}
