package main

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
)

type CSVReader struct {
	buffer *bufio.Reader
}

type CSVWriter struct {
	buffer *bufio.Writer
}

func (rowReader *CSVReader) ReadRow(options *ReadOptions) (*Row, error) {
	buffer := bytes.Buffer{}

	line, err := rowReader.buffer.ReadString('\n')
	if err != nil {
		return &Row{}, err
	}
	if len(line) == 1 {
		return &Row{}, EmptyLineError
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

// Init writes the columns names as header
func (rowWriter *CSVWriter) Init(names *ColumnNames, schema []int) (err error) {
	for idx, i := range schema {
		rowWriter.buffer.WriteString((*names)[i])
		if idx < len(schema)-1 {
			rowWriter.buffer.WriteRune(',')
		}
	}
	rowWriter.buffer.WriteRune('\n')
	return
}

func (rowWriter *CSVWriter) WriteRow(row *Row, options *WriteOptions) error {
	if row.Names == nil {
		return errors.New("missing names record")
	}

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

func (writer *CSVWriter) Flush() error {
	return writer.buffer.Flush()
}

func readCSV(reader *bufio.Reader, options *ReadOptions) (*Section, error) {
	for i := 0; i != options.nSkipRows; i++ {
		reader.ReadString('\n')
	}

	var row *Row
	var err error
	var capacity int
	if options.nRows < 0 {
		capacity = 10
	} else {
		capacity = options.nRows
	}
	section := Section(make([]Row, 0, capacity))
	rowReader := CSVReader{reader}
	for i := 0; i != options.nRows; i++ {
		row, err = rowReader.ReadRow(options)
		if err == io.EOF {
			break
		} else if err == EmptyLineError {
			// Skip
		} else if err != nil {
			return &section, err
		} else {
			section = append(section, *row)
		}
	}
	return &section, nil
}
