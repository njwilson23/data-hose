package main

import (
	"bufio"
	"os"
	"path/filepath"

	"github.com/urfave/cli"
)

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

				return merge(readers, writer, &ReadOptions{}, &WriteOptions{})
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
