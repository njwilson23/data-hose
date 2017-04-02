package main

import (
	"bufio"
	"io"
)

func merge(inputs []io.Reader, output *bufio.Writer) error {
	pending := make(chan string, BUFFER_SIZE)
	ret := make(chan error)

	go readInputs(inputs, pending, ret)
	err, ok := <-ret
	if ok {
		return err
	}
	handleLines(output, pending, -1)
	return nil
}
