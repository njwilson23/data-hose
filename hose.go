package main

import (
	"bufio"
	"io"
	"os"
	"path/filepath"

	"github.com/urfave/cli"
)

const BUFFER_SIZE = 10000

var USAGE_ERROR = cli.NewExitError("invalid usage", 1)
var MISSING_FILE_ERROR = cli.NewExitError("input file not found", 1)
var NO_READER_ERROR = cli.NewExitError("input file type not known", 1)
var NO_WRITER_ERROR = cli.NewExitError("output file type not known", 1)

func getReader(filetype string, buffer *bufio.Reader) (RowBasedReader, error) {
	switch filetype {
	case "csv":
		return &CSVReader{buffer}, nil
	case "svm", "libsvm":
		return &LibSVMReader{buffer}, nil
	default:
		return &CSVReader{&bufio.Reader{}}, NO_READER_ERROR
	}
}

func getWriter(filetype string, buffer *bufio.Writer) (RowBasedWriter, error) {
	switch filetype {
	case "csv":
		return &CSVWriter{buffer}, nil
	case "svm", "libsvm":
		return &LibSVMWriter{buffer}, nil
	default:
		return &CSVWriter{&bufio.Writer{}}, NO_WRITER_ERROR
	}
}

// readInputs opens a list of file paths sequentially, sending their contents
// into a Row channel
func readInputs(files []RowBasedReader, buffer chan *Row, errorChan chan error, options *rowReadOptions) {
	defer close(buffer)
	defer close(errorChan)
	for _, reader := range files {

		fileDone := false

		for !fileDone {
			row, err := reader.ReadRow(options)
			if err == io.EOF {
				fileDone = true
			} else if err != nil {
				errorChan <- cli.NewExitError(err, 2)
				return
			} else {
				buffer <- row
			}
		}
	}
	return
}

// handleLines writes up to nRows from a Row channel to a buffered target
func handleLines(target RowBasedWriter, ch chan *Row, options *rowWriteOptions) error {
	i := 0
	for row := range ch {
		err := target.WriteRow(row, options)
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

func main() {
	app := cli.NewApp()
	app.Name = "hose"
	app.Version = "0.2.0"
	app.Usage = "Utility for managing big row-based datasets"
	app.Authors = []cli.Author{cli.Author{Name: "Nat Wilson"}}

	app.Commands = []cli.Command{

		{
			Name:    "merge",
			Aliases: []string{"cat"},
			Usage:   "concatenate files",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "output, o",
					Value: "",
					Usage: "`PATH` to direct output to (if not given, writes to stdout)",
				},
				cli.StringFlag{
					Name:  "f",
					Value: "",
					Usage: "input `FORMAT` (if not given, guessed from extension)",
				},
				cli.StringFlag{
					Name:  "t",
					Value: "",
					Usage: "output `PATH` (if not given, guessed from extension)",
				},
			},

			Action: func(c *cli.Context) error {
				if len(c.Args()) < 1 {
					cli.ShowCommandHelp(c, "merge")
					return USAGE_ERROR
				}

				readers := make([]RowBasedReader, len(c.Args()))
				for i, path := range c.Args() {
					if _, err := os.Stat(path); os.IsNotExist(err) {
						return MISSING_FILE_ERROR
					}

					f, err := os.Open(path)
					if err != nil {
						return cli.NewExitError("failed to open file", 2)
					}

					extension := filepath.Ext(path)
					if len(extension) == 0 {
						return cli.NewExitError("filetype could not be inferred", 1)
					}

					reader, err := getReader(extension[1:], bufio.NewReader(f))
					if err != nil {
						return err
					}

					readers[i] = reader
				}

				fout, err := createOutput(c.String("output"))
				if err != nil {
					return err
				}

				extension := filepath.Ext(c.String("output"))
				if len(extension) == 0 {
					return cli.NewExitError("filetype could not be inferred for output", 1)
				}

				writer, err := getWriter(extension[1:], fout)
				if err != nil {
					return err
				}

				return merge(readers, writer, &rowReadOptions{}, &rowWriteOptions{})
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
					Name:  "start, s",
					Value: 0,
					Usage: "`ROW` to slice from",
				},
				cli.IntFlag{
					Name:  "end, e",
					Value: -1,
					Usage: "`ROW` to slice to",
				},
				cli.StringFlag{
					Name:  "f",
					Value: "",
					Usage: "input `FORMAT` (if not given, guessed from extension)",
				},
				cli.StringFlag{
					Name:  "t",
					Value: "",
					Usage: "output `PATH` (if not given, guessed from extension)",
				},
			},

			Action: func(c *cli.Context) error {
				if len(c.Args()) < 1 {
					cli.ShowCommandHelp(c, "slice")
					return USAGE_ERROR
				}

				_, err := createOutput(c.String("output"))
				if err != nil {
					return err
				}

				if _, err := os.Stat(c.Args().First()); os.IsNotExist(err) {
					return MISSING_FILE_ERROR
				}

				_, err = os.Open(c.Args().First())
				if err != nil {
					return cli.NewExitError(err, 1)
				}

				//return slice(f, c.Int("from"), c.Int("to"), fout)
				return nil
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
