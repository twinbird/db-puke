package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
)

const (
	DBPukeVersion                 = "0.0.4"
	DBTypeMSSql                   = "mssql"
	UnsupportedColumnTypeOutput   = "[UNSUPPORTED COLUMN TYPE]"
	DBPukeEnvironmentNamePassword = "DB_PUKE_PASSWORD"
)

type DBPukeOperator interface {
	DBOpen() error
	DBClose() error
	GetTableNames() ([]string, error)
	QueryAllRecords(table string) (*sql.Rows, error)
	FormatData(val any, ty *sql.ColumnType) (string, error)
}

func main() {
	option, err := parseArgs()
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(1)
	}
	commandOption = option

	exec()

	os.Exit(0)
}

func makeOperator() (DBPukeOperator, error) {
	switch commandOption.DBType {
	case DBTypeMSSql:
		return NewMSSqlOperator(), nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", commandOption.DBType)
	}
}

func exportTableToCSV(operator DBPukeOperator, table string) error {
	rows, err := operator.QueryAllRecords(table)
	if err != nil {
		return err
	}
	defer rows.Close()

	file, err := createOutputFile(table)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writeOutputHeader(rows, writer)
	if err != nil {
		return err
	}

	return writeOutputBody(operator, rows, writer)
}

func exec() {
	operator, err := makeOperator()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create operator. '%s'\n", err)
		os.Exit(1)
	}

	err = operator.DBOpen()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open database. '%s'\n", err)
		os.Exit(1)
	}
	defer operator.DBClose()

	tables := commandOption.ParsedTableNames
	if len(commandOption.ParsedTableNames) == 0 {
		all_tables, err := operator.GetTableNames()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to retrieve the list of tables. '%s'\n", err)
			os.Exit(1)
		}
		tables = all_tables
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(tables))
	for _, table := range tables {
		go func(t string) {
			defer wg.Done()
			err := exportTableToCSV(operator, t)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Export failed: '%s' %s\n", t, err)
			}
		}(table)
	}
	wg.Wait()
}
