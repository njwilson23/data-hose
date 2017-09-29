package main

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
)

func writeLibSVMRows(writer io.Writer, ch <-chan *Row, labelCol int) error {
	bufferedWriter := bufio.NewWriter(writer)
	libSVMWriter := LibSVMWriter{bufferedWriter}
	for row := range ch {
		err := libSVMWriter.Write(row, labelCol)
		if err != nil {
			return err
		}
	}
	libSVMWriter.Flush()
	return nil
}

type LibSVMReader struct {
	buffer *bufio.Reader
}

type LibSVMWriter struct {
	buffer *bufio.Writer
}

// WriteRow outputs a libSVM representation of a Row
func (rowWriter *LibSVMWriter) Write(row *Row, label int) error {
	var buffer bytes.Buffer

	// Write label
	buffer.WriteString(row.Values[label])

	var key string

	for i, value := range row.Values {
		if i == label {
			continue
		} else if i < label {
			key = strconv.Itoa(i)
		} else {
			key = strconv.Itoa(i - 1)
		}

		fp, err := strconv.ParseFloat(value, 64)
		if err != nil || fp == 0.0 {
			continue
		}
		buffer.WriteRune(' ')
		buffer.WriteString(key)
		buffer.WriteRune(':')
		buffer.WriteString(strconv.FormatFloat(fp, 'f', -1, 64))
	}
	buffer.WriteRune('\n')
	rowWriter.buffer.WriteString(buffer.String())
	return nil
}

func (writer *LibSVMWriter) Flush() error {
	return writer.buffer.Flush()
}
