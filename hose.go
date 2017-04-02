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

// readInputs opens a list of file paths sequentially, sending their contents
// line-by-line into a string channel
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
			} else {
				buffer <- line
			}
		}
	}
	return
}

// handleLines writes strings up to nLines from a channel to a buffered target
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

// createOutput abstracts writing to a file versus writing to stdout
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
	app.Version = "0.2.0"
	app.Usage = "Utility for managing big row-based datasets"
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

		{
			Name:  "convert",
			Usage: "convert from one text-based format to another",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output, o",
					Value: "",
					Usage: "`PATH` to direct output to (if not given, writes to stdout)",
				},
				cli.StringFlag{
					Name:  "from, f",
					Value: "",
					Usage: "`FORMAT` to convert from",
				},
				cli.StringFlag{
					Name:  "to, t",
					Value: "",
					Usage: "`FORMAT` to convert to",
				},
			},

			Action: func(c *cli.Context) error {
				/*if len(c.Args()) < 1 {
					cli.ShowCommandHelp(c, "convert")
					return USAGE_ERROR
				}*/

				/*fout, err := createOutput(c.String("output"))
				if err != nil {
					return err
				}*/
				return cli.NewExitError("not implemented", 3)
			},
		},
	}

	app.Run(os.Args)
}
