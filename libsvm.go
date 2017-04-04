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
func (rowWriter *LibSVMWriter) WriteRow(row *Row, options *WriteOptions) error {
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

func (rowReader *LibSVMReader) ReadRow(options *ReadOptions) (*Row, error) {
	line, err := rowReader.buffer.ReadString('\n')
	buffer := bytes.Buffer{}

	// Read the target value
	cnt := 0
	for _, b := range line {
		cnt++
		if b == ' ' {
			break
		}
		buffer.WriteRune(b)
	}

	schema := []int{0}
	values := []string{buffer.String()}
	buffer.Reset()

	// Read the feature values
	var colNum int
	readingValue := true
	for _, b := range line[cnt:] {
		switch b {
		case ' ', '\n':
			if readingValue && buffer.Len() != 0 {
				values = append(values, buffer.String())
				buffer.Reset()
				readingValue = false
			}
		case ':':
			if buffer.Len() != 0 {
				colNum, err = strconv.Atoi(buffer.String())
				if err != nil {
					return &Row{}, err
				}
				schema = append(schema, colNum+1)
				buffer.Reset()
			}
			readingValue = true
		default:
			buffer.WriteRune(b)
		}
	}

	row := Row{Schema: schema, Values: values}
	return &row, nil
}

func (writer *LibSVMWriter) Flush() error {
	return writer.buffer.Flush()
}
