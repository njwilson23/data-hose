package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli"
)

var USAGE_ERROR = cli.NewExitError("invalid usage", 1)
var MISSING_FILE_ERROR = cli.NewExitError("input file not found", 1)

func main() {
	app := cli.NewApp()
	app.Name = "hose"
	app.Version = "0.2.0"
	app.Usage = "Utility for managing big row-based datasets"
	app.Authors = []cli.Author{cli.Author{Name: "Nat Wilson"}}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "output, o",
			Value: "",
			Usage: "`PATH` to direct output to (if not given, writes to stdout)",
		},
		cli.StringFlag{
			Name:  "from, f",
			Value: "",
			Usage: "input `FORMAT` (if not given, guessed from extension)",
		},
		cli.StringFlag{
			Name:  "to, t",
			Value: "",
			Usage: "output `FORMAT` (if not given, guessed from extension)",
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
	}

	app.Action = func(c *cli.Context) error {
		if c.NArg() < 1 {
			cli.ShowCommandHelp(c, "")
			return USAGE_ERROR
		}

		var filetype string
		readers := make([]RowBasedReader, len(c.Args()))
		for i, path := range c.Args() {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				fmt.Println(path)
				return MISSING_FILE_ERROR
			}

			f, err := os.Open(path)
			if err != nil {
				return cli.NewExitError("failed to open file", 2)
			}

			if c.String("from") != "" {
				filetype = c.String("from")
			} else {
				filetype = strings.Trim(filepath.Ext(path), ".")
			}
			if len(filetype) == 0 {
				return cli.NewExitError("input filetype unknown", 1)
			}

			reader, err := getReader(filetype, bufio.NewReader(f))
			if err != nil {
				return err
			}

			readers[i] = reader
		}

		fout, err := createOutput(c.String("output"))
		if err != nil {
			return err
		}

		filetype = ""
		if c.String("to") != "" {
			filetype = c.String("to")
		} else if c.String("output") != "" {
			filetype = strings.Trim(filepath.Ext(c.String("output")), ".")
		}
		if filetype == "" {
			return cli.NewExitError("output filetype unknown", 1)
		}

		writer, err := getWriter(filetype, fout)
		if err != nil {
			return err
		}

		readOpt := &ReadOptions{
			nSkipRows: c.Int("start"),
			nRows:     c.Int("end") - c.Int("start"),
		}

		writeOpt := &WriteOptions{}

		return Merge(readers, writer, readOpt, writeOpt)
	}

	app.Run(os.Args)
}
