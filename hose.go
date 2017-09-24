package main

import (
	"bufio"
	"io"
	"os"

	"github.com/urfave/cli"
)

const BufferSize = 10000

var NoReaderError = cli.NewExitError("input file type not known", 1)
var NoWriterError = cli.NewExitError("output file type not known", 1)

func getReader(filetype string, buffer *bufio.Reader) (RowBasedReader, error) {
	switch filetype {
	case "csv":
		return &CSVReader{buffer}, nil
	case "json":
		return &JSONReader{buffer}, nil
	case "svm", "libsvm":
		return &LibSVMReader{buffer}, nil
	case "txt":
		return &TextReader{buffer}, nil
	default:
		return &TextReader{&bufio.Reader{}}, NoReaderError
	}
}

func getWriter(filetype string, buffer *bufio.Writer) (RowBasedWriter, error) {
	switch filetype {
	case "csv":
		return &CSVWriter{buffer}, nil
	case "json":
		return &JSONWriter{buffer, make([][]byte, 32)}, nil
	case "svm", "libsvm":
		return &LibSVMWriter{buffer}, nil
	case "txt":
		return &TextWriter{buffer}, nil
	default:
		return &TextWriter{&bufio.Writer{}}, NoWriterError
	}
}

// ReadInputs opens a list of file paths sequentially, sending their contents
// into a Row channel
func ReadInputs(files []RowBasedReader, buffer chan *Row, errorChan chan error, options *ReadOptions) {
	defer close(buffer)
	defer close(errorChan)

	rowCount := 0
	for _, reader := range files {

		fileDone := false

		for !fileDone && (rowCount != options.nRows) {
			row, err := reader.ReadRow(options)
			if err == io.EOF {
				fileDone = true
			} else if err == EmptyLineError {
			} else if err != nil {
				errorChan <- cli.NewExitError(err, 2)
				return
			}
			buffer <- row
			rowCount++
		}
	}
	return
}

// WriteRows writes up to nRows from a Row channel to a buffered target
func WriteRows(target RowBasedWriter, ch chan *Row, options *WriteOptions) error {
	i := 0
	var row *Row
	var err error
	if options.header {
		row = <-ch
		err = target.Init(row.Names, row.Schema)
		if err != nil {
			return cli.NewExitError(err, 1)
		}
		err = target.WriteRow(row, options)
		if err != nil {
			return cli.NewExitError(err, 1)
		}
		i++
	}
	for row = range ch {
		err = target.WriteRow(row, options)
		if err != nil {
			return cli.NewExitError(err, 1)
		}

		i++
		if i == options.nRows {
			break
		}
	}
	target.Flush()
	return nil
}

// createOutput abstracts writing to a file versus writing to stdout
func createOutput(path string) (*bufio.Writer, error) {
	var writer io.Writer
	var err error
	if path == "" {
		writer = os.Stdout
	} else {
		writer, err = os.Create(path)
		if err != nil {
			return &bufio.Writer{}, cli.NewExitError(err, 3)
		}
	}
	buffer := bufio.NewWriter(writer)
	return buffer, nil
}

// Merge combines the rows from multiple sources and writes them to a single output
func Merge(inputs []RowBasedReader, output RowBasedWriter, readOptions *ReadOptions, writeOptions *WriteOptions) error {
	pending := make(chan *Row, BufferSize)
	errorChan := make(chan error)

	go ReadInputs(inputs, pending, errorChan, readOptions)
	err, ok := <-errorChan
	if ok {
		return err
	}

	return WriteRows(output, pending, writeOptions)
}
