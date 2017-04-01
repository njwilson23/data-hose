package main

import (
	"bufio"
	"io"
	"os"

	"github.com/urfave/cli"
)

const BUFFER_SIZE = 10000

var USAGE_ERROR = cli.NewExitError("invalid usage", 1)
var MISSING_FILE_ERROR = cli.NewExitError("input file not found", 1)

func readInputs(paths []string, buffer chan string, errorChan chan error) {
	defer close(buffer)
	defer close(errorChan)
	for _, path := range paths {

		if _, err := os.Stat(path); os.IsNotExist(err) {
			errorChan <- MISSING_FILE_ERROR
			return
		}

		f, err := os.Open(path)
		if err != nil {
			errorChan <- cli.NewExitError("failed to open file", 2)
			return
		}

		reader := bufio.NewReader(f)
		fileDone := false

		for !fileDone {

			line, err := reader.ReadString('\n')
			if err == io.EOF {
				fileDone = true
			} else if err != nil {
				errorChan <- cli.NewExitError("failed to read line from file", 2)
				return
			}

			buffer <- line
		}
	}
	return
}

func handleLines(target *bufio.Writer, buffer chan string, nLines int) error {
	defer target.Flush()
	i := 0
	for line := range buffer {
		_, err := target.WriteString(line)
		if err != nil {
			return cli.NewExitError("failed to write line to file", 3)
		}

		i++
		if i == nLines {
			break
		}
	}
	return nil
}

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

func slice(input string, from, to int, output *bufio.Writer) error {
	if from < 0 {
		return USAGE_ERROR
	}

	pending := make(chan string, BUFFER_SIZE)
	ret := make(chan error)

	inputs := []string{}
	inputs = append(inputs, string(input))
	go readInputs(inputs, pending, ret)
	err, ok := <-ret
	if ok {
		return err
	}

	i := 0
	for i != from {
		line, ok := <-pending
		if !ok {
			return cli.NewExitError("slice beginning not reached", 2)
		}
		pending <- line
		i++
	}
	handleLines(output, pending, to-from)
	return nil
}

func createOutput(path string) (*bufio.Writer, error) {
	var writer io.Writer
	var err error
	if path == "" {
		writer = os.Stdout
	} else {
		writer, err = os.Create(path)
		if err != nil {
			return &bufio.Writer{}, cli.NewExitError("failed to create output file", 3)
		}
	}
	buffer := bufio.NewWriter(writer)
	return buffer, nil
}

func main() {
	app := cli.NewApp()
	app.Name = "hose"
	app.Version = "0.1.1"
	app.Usage = "Merge and slice big text row-based datasets"
	app.Authors = []cli.Author{cli.Author{Name: "Nat Wilson"}}

	app.Commands = []cli.Command{

		{
			Name:  "merge",
			Usage: "concatenate files",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output, o",
					Value: "",
					Usage: "`PATH` to direct output to (if not given, writes to stdout)",
				},
			},

			Action: func(c *cli.Context) error {
				if len(c.Args()) < 1 {
					cli.ShowCommandHelp(c, "merge")
					return USAGE_ERROR
				}

				fout, err := createOutput(c.String("output"))
				if err != nil {
					return err
				}
				return merge(c.Args(), fout)
			},
		},

		{
			Name:  "slice",
			Usage: "slice rows from a file",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output, o",
					Value: "",
					Usage: "`PATH` to direct output to (if not given, writes to stdout)",
				},
				cli.IntFlag{
					Name:  "from, f",
					Value: 0,
					Usage: "`ROW` to slice from",
				},
				cli.IntFlag{
					Name:  "to, t",
					Value: -1,
					Usage: "`ROW` to slice to",
				},
			},

			Action: func(c *cli.Context) error {
				if (c.Int("to") != -1) && (c.Int("to") <= c.Int("from")) {
					return cli.NewExitError("--from argument must be greater than --to argument", 1)
				}
				if len(c.Args()) < 1 {
					cli.ShowCommandHelp(c, "slice")
					return USAGE_ERROR
				}

				fout, err := createOutput(c.String("output"))
				if err != nil {
					return err
				}
				return slice(c.Args().First(), c.Int("from"), c.Int("to"), fout)
			},
		},
	}

	app.Run(os.Args)
}
