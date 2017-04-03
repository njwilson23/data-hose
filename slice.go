package main

import (
	"os"

	"github.com/urfave/cli"
)

func slice(input RowBasedReader, from, to int, output RowBasedWriter, readOptions *rowReadOptions, writeOptions *rowWriteOptions) error {
	if to != -1 && (to <= from) {
		os.Stderr.WriteString("--from argument must be greater than --to argument\n")
		return USAGE_ERROR
	}
	if from < 0 {
		os.Stderr.WriteString("--from must be greater than or equal to 0\n")
		return USAGE_ERROR
	}

	pending := make(chan *Row, BUFFER_SIZE)
	errorChan := make(chan error)

	inputs := []RowBasedReader{input}
	go readInputs(inputs, pending, errorChan, readOptions)
	err, ok := <-errorChan
	if ok {
		return err
	}

	i := 0
	for i != from {
		_, ok := <-pending
		if !ok {
			return cli.NewExitError("slice beginning not reached", 2)
		}
		i++
	}

	// Ensure that the nRows member of the write options is consistent with the
	// number of lines wanted
	writeOptionsLocal := &rowWriteOptions{}
	*writeOptionsLocal = *writeOptions
	writeOptionsLocal.nRows = to - from

	return handleLines(output, pending, writeOptionsLocal)
}
