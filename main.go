package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

const (
	DBPukeVersion = "0.0.3"
	DBTypeMSSql   = "mssql"
)

var (
	commandOption *Option
)

type Option struct {
	DBType        string
	Host          string
	Port          int
	Database      string
	Schema        string
	User          string
	Password      string
	OutDir        string
	NullRepresent string
}

func parseArgs() *Option {
	option := &Option{}

	flag.StringVar(&option.DBType, "type", "", "database server type [mssql]")
	flag.StringVar(&option.Host, "h", "localhost", "database server host")
	flag.IntVar(&option.Port, "p", 1433, "database server port")
	flag.StringVar(&option.Database, "d", "", "database")
	flag.StringVar(&option.Schema, "s", "", "database schema")
	flag.StringVar(&option.User, "u", "", "database user name")
	flag.StringVar(&option.Password, "P", "", "database user password")
	flag.StringVar(&option.OutDir, "o", "db-puke-exported", "export directory")
	flag.StringVar(&option.NullRepresent, "N", "NULL", "string to represent NULL")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `%s - database data exporter [version %s]

Usage:
  db-puke -type <database type> -h <hostname> -p <access port> -d <database name> -s <database schema> -u <username> -P <password> -o <output dir>

Options:
`, os.Args[0], DBPukeVersion)
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "  --help\n\tshow this help message and exit")
	}

	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(0)
	}

	flag.Parse()

	if option.DBType == "" {
		fmt.Println("Error: Please specify the database type (-type)")
		os.Exit(1)
	}

	if option.DBType != DBTypeMSSql {
		fmt.Println("Error: Specify database type is not supported")
		os.Exit(1)
	}

	if option.Database == "" {
		fmt.Println("Error: Please specify the database name (-d)")
		os.Exit(1)
	}
	if option.Schema == "" {
		fmt.Println("Error: Please specify the schema name (-s)")
		os.Exit(1)
	}
	if option.User == "" {
		fmt.Println("Error: Please specify the username (-u)")
		os.Exit(1)
	}
	if option.Password == "" {
		fmt.Println("Error: Please specify the database password (-P)")
		os.Exit(1)
	}

	return option
}

func main() {
	commandOption = parseArgs()

	exec()

	os.Exit(0)
}

func exec() {
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		commandOption.User, commandOption.Password, commandOption.Host, commandOption.Port, commandOption.Database)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to the database. '%s'\n", err)
		os.Exit(1)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to the database. '%s'\n", err)
		os.Exit(1)
	}

	tables, err := getTables(db, commandOption.Schema)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to retrieve the list of tables. '%s'\n", err)
		os.Exit(1)
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(tables))
	for _, table := range tables {
		go func(t string) {
			defer wg.Done()
			err := exportTableToCSV(db, commandOption.Schema, t, commandOption.OutDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Export failed: '%s' %s\n", t, err)
			}
		}(table)
	}
	wg.Wait()
}

func getTables(db *sql.DB, schema string) ([]string, error) {
	query := `
        SELECT
			TABLE_SCHEMA,
			TABLE_NAME 
		FROM
			INFORMATION_SCHEMA.TABLES 
		WHERE
			TABLE_TYPE = 'BASE TABLE'
		AND
			TABLE_SCHEMA = @schema
	`
	rows, err := db.QueryContext(context.Background(), query, sql.Named("schema", schema))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schema, tname string

		err := rows.Scan(&schema, &tname)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tname)
	}
	return tables, nil
}

func getOutputFilePath(outdir, tableName string) (string, error) {
	absPath, err := filepath.Abs(outdir)
	if err != nil {
		return "", fmt.Errorf("Error retrieving output directory path: %w", err)
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		if err := os.MkdirAll(absPath, 0755); err != nil {
			return "", fmt.Errorf("Error creating output directory: %w", err)
		}
	}

	filePath := filepath.Join(absPath, fmt.Sprintf("%s.csv", tableName))

	return filePath, nil
}

func formatData(val any, ty *sql.ColumnType) (string, error) {
	if val == nil {
		return commandOption.NullRepresent, nil
	}
	tyname := ty.DatabaseTypeName()

	switch tyname {
	case "INT":
		return fmt.Sprintf("%d", val), nil
	case "BIGINT":
		return fmt.Sprintf("%d", val), nil
	case "SMALLINT":
		return fmt.Sprintf("%d", val), nil
	case "TINYINT":
		return fmt.Sprintf("%d", val), nil
	case "BIT":
		if val == true {
			return "1", nil
		} else {
			return "0", nil
		}
	case "FLOAT":
		return fmt.Sprintf("%g", val), nil
	case "REAL":
		return fmt.Sprintf("%g", val), nil
	case "VARCHAR":
		return fmt.Sprintf("%s", val), nil
	case "NVARCHAR":
		return fmt.Sprintf("%s", val), nil
	case "CHAR":
		return fmt.Sprintf("%s", val), nil
	case "TEXT":
		return fmt.Sprintf("%s", val), nil
	case "NTEXT":
		return fmt.Sprintf("%s", val), nil
	case "DATE":
		t := (val).(time.Time)
		return t.Format("2006-01-02"), nil
	case "DATETIME":
		t := (val).(time.Time)
		return t.Format("2006-01-02 15:04:05.000"), nil
	case "DATETIME2":
		t := (val).(time.Time)
		return t.Format("2006-01-02 15:04:05.0000000"), nil
	case "NUMERIC":
		fallthrough
	case "DECIMAL":
		v := val.([]uint8)
		return fmt.Sprintf("%s", string(v)), nil
	case "UNIQUEIDENTIFIER":
		byte_val := val.([]byte)

		var guid mssql.UniqueIdentifier
		if err := guid.Scan(byte_val); err != nil {
			return "", err
		}

		return fmt.Sprintf("%s", guid.String()), nil
	}

	return "[NOT SUPPORTED COLUMN TYPE]", nil
}

func createOutputFile(outdir, table string) (*os.File, error) {
	fileName, err := getOutputFilePath(outdir, table)
	if err != nil {
		return nil, err
	}

	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}

	return file, err
}

func writeOutputHeader(rows *sql.Rows, writer *csv.Writer) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	if err := writer.Write(columns); err != nil {
		return err
	}

	return nil
}

func writeOutputBody(rows *sql.Rows, writer *csv.Writer) error {
	column_types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	values := make([]interface{}, len(column_types))
	valuePtrs := make([]interface{}, len(column_types))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		var record []string
		for i, val := range values {
			ty := column_types[i]
			val_str, err := formatData(val, ty)
			if err != nil {
				return err
			}
			record = append(record, val_str)
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

func exportTableToCSV(db *sql.DB, schema, table string, outdir string) error {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM [%s].[%s]", schema, table))
	if err != nil {
		return err
	}
	defer rows.Close()

	file, err := createOutputFile(outdir, table)
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

	return writeOutputBody(rows, writer)
}
