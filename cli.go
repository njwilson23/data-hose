package main

import (
	"errors"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli"
)

// UsageError is the error returned when the CLI parameters make no sense
var UsageError = cli.NewExitError("invalid usage", 1)

// MissingFileError is the error returned when a file was not found at the specified location
var MissingFileError = cli.NewExitError("input file not found", 1)

func main() {
	app := cli.NewApp()
	app.Name = "hose"
	app.Version = "0.3.0"
	app.Usage = "Streaming tool for big row-based datasets"
	app.Authors = []cli.Author{cli.Author{Name: "Nat Wilson"}}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "o, output",
			Value: "",
			Usage: "`PATH` to direct output to (if not given, writes to stdout)",
		},
		cli.StringFlag{
			Name:  "f, format",
			Value: "csv",
			Usage: "output `FORMAT` (if not given, assumed CSV)",
		},
		cli.IntFlag{
			Name:  "libsvm-label",
			Value: 0,
			Usage: "column to use as label when exporting to libSVM format",
		},
		cli.StringFlag{
			Name:  "c, columns",
			Value: "",
			Usage: "specifies columns to take, by name or index",
		},
		cli.StringFlag{
			Name:  "p, predicate",
			Value: "",
			Usage: "specifies a row-dependent predicate to filter rows",
		},
		cli.IntFlag{
			Name:  "s, skip",
			Value: 0,
			Usage: "`ROW` to slice from",
		},
		cli.IntFlag{
			Name:  "n, nrows",
			Value: 0,
			Usage: "number of rows to take",
		},
	}

	app.Action = func(c *cli.Context) error {

		var err error
		cin := make(chan *Row)
		cout := make(chan *Row)

		// Create input channel
		var reader io.Reader
		if c.NArg() == 0 {
			reader = os.Stdin
		} else {
			reader, err = os.Open(c.Args().Get(0))
			if err != nil {
				return err
			}
		}
		go readInputRows(reader, cin)

		// Create processor channels
		pipeline := Pipeline{}

		if c.Int("skip") != 0 {
			pipeline.Add(RowSkipper(c.Int("skip")))
		}

		if c.Int("nrows") != 0 {
			pipeline.Add(RowLimiter(c.Int("nrows")))
		}

		if c.String("predicate") != "" {
			pipeline.Add(Predicator(c.String("predicate")))
		}

		if c.String("columns") != "" {
			pipeline.Add(ColumnSelector(strings.Split(c.String("columns"), ",")))
		}

		if pipeline.Length() == 0 {
			// Need at least on transformation to connect input and output channels
			pipeline.Add(IdentityTransformer)
		}

		go pipeline.Run(cin, cout)

		// Create an output
		var writer io.Writer
		if c.String("output") == "" {
			writer = os.Stdout
		} else {
			writer, err = os.Create(c.String("output"))
			if err != nil {
				return err
			}
		}

		if c.String("f") == "csv" {
			return writeCSVRows(writer, cout)
		} else if c.String("format") == "libsvm" {
			return writeLibSVMRows(writer, cout, c.Int("libsvm-label"))
		}
		return errors.New("unhandled output type")
	}

	app.Run(os.Args)
}
