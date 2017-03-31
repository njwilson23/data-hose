package main

import (
	"bufio"
	"io"
	"os"

	"github.com/urfave/cli"
)

const BUFFER_SIZE = 10000

var INVALID_USAGE = cli.NewExitError("invalid usage", 1)
var MISSING_FILE_ERROR = cli.NewExitError("file not found", 1)

func slice(input string, from, to int, output string) error {
	pending := make(chan string, BUFFER_SIZE)

	inputs := []string{}
	inputs = append(inputs, string(input))
	go readInputs(inputs, pending)

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

func readInputs(paths []string, buffer chan string) error {
	defer close(buffer)
	for _, path := range paths {

		if _, err := os.Stat(path); os.IsNotExist(err) {
			return MISSING_FILE_ERROR
		}

		f, err := os.Open(path)
		if err != nil {
			return cli.NewExitError("failed to open file", 2)
		}

		reader := bufio.NewReader(f)
		fileDone := false

		for !fileDone {

			line, err := reader.ReadString('\n')
			if err == io.EOF {
				fileDone = true
			} else if err != nil {
				return cli.NewExitError("failed to read line from file", 2)
			}

			buffer <- line
		}
	}
	return nil
}

func handleLines(target string, buffer chan string, nLines int) error {
	i := 0
	if len(target) == 0 {

		for line := range buffer {
			_, err := os.Stdout.WriteString(line)
			if err != nil {
				return cli.NewExitError("failed to write line", 3)
			}

			i++
			if i == nLines {
				break
			}
		}

	} else {

		f, err := os.Create(target)
		if err != nil {
			return cli.NewExitError("failed to create output file", 3)
		}
		fout := bufio.NewWriter(f)
		for line := range buffer {
			_, err := fout.WriteString(line)
			if err != nil {
				return cli.NewExitError("failed to write line to file", 3)
			}

			i++
			if i == nLines {
				break
			}
		}

		err = fout.Flush()
		if err != nil {
			return cli.NewExitError("failed to flush write buffer", 3)
		}
	}
	return nil
}

func merge(inputs cli.Args, output string) error {
	pending := make(chan string, BUFFER_SIZE)

	go readInputs(inputs, pending)
	handleLines(output, pending, -1)
	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "hose"
	app.Version = "0.1.0"
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
					return INVALID_USAGE
				}
				return merge(c.Args(), c.String("output"))
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
					return INVALID_USAGE
				}
				return slice(c.Args().First(), c.Int("from"), c.Int("to"), c.String("output"))
			},
		},
	}

	app.Run(os.Args)
}
