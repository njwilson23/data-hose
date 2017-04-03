package main

func merge(inputs []RowBasedReader, output RowBasedWriter, readOptions *rowReadOptions, writeOptions *rowWriteOptions) error {
	pending := make(chan *Row, BUFFER_SIZE)
	errorChan := make(chan error)

	go readInputs(inputs, pending, errorChan, readOptions)
	err, ok := <-errorChan
	if ok {
		return err
	}

	return handleLines(output, pending, writeOptions)
}
