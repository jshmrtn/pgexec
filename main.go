package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/urfave/cli/v2"
)

type connArgs struct {
	host     string
	port     string
	user     string
	password string
	database string
	url      string
}

func main() {
	args := connArgs{}

	app := &cli.App{
		Name:      "pgexec",
		UsageText: "pgexec --url \"postgres://...\" \"SELECT * FROM users;\"",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "url",
				Destination: &args.url,
				Usage:       "Connection string, e.g. postgres://<user>:<pw>@<host>:<port>/<db>",
			},
			&cli.StringFlag{
				Name:        "host",
				Destination: &args.host,
				Usage:       "Host addres",
			},
			&cli.StringFlag{
				Name:        "port",
				Aliases:     []string{"p"},
				Destination: &args.port,
				Usage:       "Port",
			},
			&cli.StringFlag{
				Name:        "user",
				Aliases:     []string{"u"},
				Destination: &args.user,
				Usage:       "User name",
			},
			&cli.StringFlag{
				Name:        "password",
				Aliases:     []string{"pw"},
				Destination: &args.password,
				Usage:       "Password",
			},
			&cli.StringFlag{
				Name:        "db",
				Destination: &args.database,
				Usage:       "Database name",
			},
		},
		Action: func(cCtx *cli.Context) error {
			err := execCommand(cCtx.Context, args, cCtx.Args().Get(0))
			return err
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func trim(str string) string {
	return strings.Trim(str, " \t\n\r")
}

func getConnPool(ctx context.Context, connArgs connArgs) (*pgxpool.Pool, error) {
	if trim(connArgs.url) != "" {
		return pgxpool.New(context.Background(), connArgs.url)
	}
	port, err := strconv.Atoi(trim(connArgs.port))
	if err != nil {
		return nil, err
	}
	return pgxpool.NewWithConfig(ctx, &pgxpool.Config{
		MaxConns: 10,
		ConnConfig: &pgx.ConnConfig{
			Config: pgconn.Config{
				Host:     connArgs.host,
				Port:     uint16(port),
				Database: connArgs.database,
				User:     connArgs.user,
				Password: connArgs.password,
			},
		},
	})
}

func execCommand(ctx context.Context, connArgs connArgs, sql string) error {
	pool, err := getConnPool(ctx, connArgs)
	if err != nil {
		return err
	}
	defer pool.Close()

	tx, err := pool.Begin(ctx)
	defer tx.Rollback(ctx)
	if err != nil {
		return err
	}
	res, err := tx.Query(ctx, sql)
	if err != nil {
		return err
	}
	vals := scanRowsToMaps(res)

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleLight)
	t.Style().Format.Header = text.FormatDefault

	header := table.Row{}
	for _, v := range res.FieldDescriptions() {
		header = append(header, v.Name)
	}
	t.AppendHeader(header)
	for _, val := range vals {
		row := table.Row{}
		for _, field := range res.FieldDescriptions() {
			row = append(row, val[field.Name])
		}
		t.AppendRow(row)
	}
	t.Render()

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}

	return nil
}

func scanRowsToMaps(rows pgx.Rows) []map[string]interface{} {
	var rowMaps []map[string]interface{}
	fields := rows.FieldDescriptions()

	for rows.Next() {
		scans := make([]interface{}, len(fields))
		row := make(map[string]interface{})

		for i := range scans {
			scans[i] = &scans[i]
		}
		rows.Scan(scans...)
		for i, v := range scans {
			var value = ""
			if v != nil {
				switch fields[i].DataTypeOID {
				case pgtype.UUIDOID:
					arr := v.([16]uint8)
					uuidVal, err := uuid.FromBytes(arr[:])
					if err != nil {
						value = fmt.Sprintf("%x", v)
					} else {
						value = uuidVal.String()
					}
				case pgtype.BoolOID:
					value = fmt.Sprintf("%t", v)
				default:
					value = fmt.Sprintf("%s", v)
				}
			} else {
				value = "null"
			}
			row[fields[i].Name] = value
		}
		rowMaps = append(rowMaps, row)
	}
	return rowMaps
}
