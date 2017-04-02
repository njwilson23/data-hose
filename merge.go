package main

import (
	"bufio"

	"github.com/urfave/cli"
)

func merge(inputs cli.Args, output *bufio.Writer) error {
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
