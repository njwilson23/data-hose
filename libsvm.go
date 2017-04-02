package main

import (
	"bufio"
	"bytes"
	"errors"
	"strconv"
)

type LibSVMReader struct {
	buffer *bufio.Reader
}

type LibSVMWriter struct {
	buffer *bufio.Writer
}

// WriteString outputs a libSVM representation of a Row
func (rowWriter *LibSVMWriter) WriteRow(row *Row, options *rowWriteOptions) error {
	var buffer bytes.Buffer

	targetEmpty := true
	for i, colNum := range row.Schema {
		if colNum == options.targetCol {
			buffer.WriteString(row.Values[i])
			targetEmpty = false
			break
		}
	}
	if targetEmpty {
		return errors.New("target column missing")
	}

	for i, colNum := range row.Schema {
		if colNum == options.targetCol {
			continue
		}
		buffer.WriteRune(' ')
		buffer.WriteString(strconv.Itoa(row.Schema[i]))
		buffer.WriteRune(':')
		buffer.WriteString(row.Values[i])
	}
	buffer.WriteRune('\n')
	rowWriter.buffer.WriteString(buffer.String())
	return nil
}

func (rowReader *LibSVMReader) ReadRow(options *rowReadOptions) (*Row, error) {
	line, err := rowReader.buffer.ReadString('\n')
	buffer := bytes.Buffer{}

	cnt := 0
	for _, b := range line {
		cnt++
		switch b {
		case ' ':
			break
		default:
			buffer.WriteRune(b)
		}
	}

	schema := []int{-1}
	values := []string{buffer.String()}
	buffer.Reset()

	var colNum int

	readingSchema := true
	for _, b := range line[cnt:] {
		if b == ' ' {
			if readingSchema && buffer.Len() != 0 {
				colNum, err = strconv.Atoi(buffer.String())
				if err != nil {
					return &Row{}, err
				}
				schema = append(schema, colNum)
				buffer.Reset()
			} else if !readingSchema {
				values = append(values, buffer.String())
				buffer.Reset()
				readingSchema = true
			}
		} else if b == ':' {
			readingSchema = false
		} else {
			buffer.WriteRune(b)
		}
	}

	row := Row{Schema: schema, Values: values}
	return &row, nil
}
