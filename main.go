package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"example/pkg/input"
	"example/pkg/sql"
	"fmt"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

func main() {
	app := &cli.App{
		Name:  "sql",
		Usage: "Queries piped data",
		Action: func(ctx *cli.Context) error {
			queryString := ctx.Args().Get(0)

			if queryString == "" {
				return errors.New("Need valid sql query as first argument")
			}

			query := sql.Parse(queryString)

			if query == nil {
				return errors.New("Unable to parse query")
			}

			dataRows, err := input.NewStdinReader().Parse(bufio.NewReader(os.Stdin))
			if err != nil {
				return err
			}

			result, err := sql.QueryData(dataRows, *query)
			if err != nil {
				return err
			}

			for _, row := range result {
				output, err := json.Marshal(row)
				if err != nil {
					return err
				}

				fmt.Println(string(output))
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
