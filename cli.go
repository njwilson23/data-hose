package main

import (
	"errors"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/urfave/cli"
)

// UsageError is the error returned when the CLI parameters make no sense
var UsageError = cli.NewExitError("invalid usage", 1)

// MissingFileError is the error returned when a file was not found at the specified location
var MissingFileError = cli.NewExitError("input file not found", 1)

func main() {
	app := cli.NewApp()
	app.Name = "flt"
	app.Version = "0.3.0"
	app.Usage = "Streaming tool for big row-based datasets"
	app.Authors = []cli.Author{cli.Author{Name: "Nat Wilson"}}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "o, output",
			Value: "",
			Usage: "`PATH` to direct output to (if not given, writes to stdout)",
		},
		cli.IntFlag{
			Name:  "s, skip",
			Value: 0,
			Usage: "`ROW` to slice from",
		},
		cli.IntFlag{
			Name:  "n, nrows",
			Value: -1,
			Usage: "number of rows to take",
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
	}

	app.Action = func(c *cli.Context) error {

		var err error
		cin := make(chan *Row)
		cout := make(chan *Row)

		// Create input channel
		var readers []io.Reader
		if c.NArg() == 0 {
			readers = []io.Reader{os.Stdin}
		} else {
			for i := 0; i != c.NArg(); i++ {
				reader, err := os.Open(c.Args().Get(i))
				if err != nil {
					return MissingFileError
				}
				readers = append(readers, reader)
			}

		}

		go readInputRows(readers, cin)

		// Create processor channels
		pipeline := Pipeline{}

		if c.Int("skip") != 0 {
			pipeline.Add(RowSkipper(c.Int("skip")))
		}

		if c.Int("nrows") != -1 {
			pipeline.Add(RowLimiter(c.Int("nrows")))
		}

		if c.String("predicate") != "" {
			pipeline.Add(Predicator(c.String("predicate")))
		}

		if c.String("columns") != "" {
			cols := strings.Split(c.String("columns"), ",")
			var indices []int
			intConversionFailure := false
			for _, col := range cols {
				idx, err := strconv.Atoi(col)
				if err != nil {
					intConversionFailure = true
					break
				}
				indices = append(indices, idx)
			}
			var tf Transformer
			if !intConversionFailure {
				tf = ColumnIntSelector(indices)
			} else {
				tf = ColumnStringSelector(cols)
			}
			pipeline.Add(tf)
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

		switch c.String("format") {
		case "csv":
			return writeCSVRows(writer, cout)
		case "json":
			return writeJSONRows(writer, cout)
		case "libsvm":
			return writeLibSVMRows(writer, cout, c.Int("libsvm-label"))
		default:
			return errors.New("unhandled output format")
		}
	}

	app.Run(os.Args)
}
