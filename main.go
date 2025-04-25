package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

const (
	DBPukeVersion                 = "0.0.4"
	DBTypeMSSql                   = "mssql"
	UnsupportedColumnTypeOutput   = "[UNSUPPORTED COLUMN TYPE]"
	DBPukeEnvironmentNamePassword = "DB_PUKE_PASSWORD"
)

var (
	commandOption *Option
)

type Option struct {
	DBType           string
	Host             string
	PortString       string
	Port             int
	Database         string
	Schema           string
	User             string
	Password         string
	OutDir           string
	NullRepresent    string
	TableNames       string
	ParsedTableNames []string
}

func parseArgs() *Option {
	option := &Option{}

	flag.StringVar(&option.DBType, "type", "", "database server type [mssql]")
	flag.StringVar(&option.Host, "h", "localhost", "database server host")
	flag.StringVar(&option.PortString, "p", "", "database server port")
	flag.StringVar(&option.Database, "d", "", "database")
	flag.StringVar(&option.Schema, "s", "", "database schema")
	flag.StringVar(&option.User, "u", "", "database user name")
	flag.StringVar(&option.Password, "P", "", "database user password(or use DB_PUKE_PASSWORD env var)")
	flag.StringVar(&option.OutDir, "o", "db-puke-exported", "export directory")
	flag.StringVar(&option.NullRepresent, "N", "NULL", "string to represent NULL")
	flag.StringVar(&option.TableNames, "t", "", "table names to export (comma-separated). exports all tables if omitted.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `%s - database data exporter [version %s]

Usage:
  db-puke -type <database type> -h <hostname> -p <access port> -d <database name> -s <database schema> -u <username> -P <password> -o <output dir>

Example:
  mssql:
    DB_PUKE_PASSWORD=saPassword1234 ./db-puke -type mssql -h localhost -p 1433 -d dummy_database -s dummy_schema -u sa -o outdir

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

	if pass, ok := os.LookupEnv(DBPukeEnvironmentNamePassword); ok {
		option.Password = pass
	}

	switch option.DBType {
	case DBTypeMSSql:
		parseMssqlOption(option)
	default:
		fmt.Fprintf(os.Stderr, "Error: Specify database type(%s) is not supported\n", option.DBType)
		os.Exit(1)
	}

	option.ParsedTableNames = parseTableOption(option.TableNames)

	return option
}

func parseMssqlOption(option *Option) {
	if option.Database == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the database name (-d)")
		os.Exit(1)
	}
	if option.Schema == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the schema name (-s)")
		os.Exit(1)
	}
	if option.User == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the username (-u)")
		os.Exit(1)
	}
	if option.Password == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the database password (-P)")
		os.Exit(1)
	}
	if option.PortString == "" {
		option.Port = 1433
	} else {
		port, err := strconv.Atoi(option.PortString)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error: Invalid port number (-p)")
			os.Exit(1)
		}
		option.Port = port
	}
}

func parseTableOption(opstr string) []string {
	s := strings.Trim(opstr, " ")
	splitted := strings.Split(s, ",")
	ret := make([]string, 0)
	for _, tname := range splitted {
		tname = strings.Trim(tname, " ")
		if tname != "" {
			ret = append(ret, tname)
		}
	}
	return ret
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

	tables := commandOption.ParsedTableNames
	if len(commandOption.ParsedTableNames) == 0 {
		all_tables, err := getTables(db, commandOption.Schema)
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
		return "", fmt.Errorf("error retrieving output directory path: %w", err)
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		if err := os.MkdirAll(absPath, 0755); err != nil {
			return "", fmt.Errorf("error creating output directory: %w", err)
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
		fallthrough
	case "BIGINT":
		fallthrough
	case "SMALLINT":
		fallthrough
	case "TINYINT":
		return fmt.Sprintf("%d", val), nil
	case "BIT":
		if val == true {
			return "1", nil
		} else {
			return "0", nil
		}
	case "FLOAT":
		fallthrough
	case "REAL":
		return fmt.Sprintf("%g", val), nil
	case "VARCHAR":
		fallthrough
	case "NVARCHAR":
		fallthrough
	case "CHAR":
		fallthrough
	case "NCHAR":
		fallthrough
	case "TEXT":
		fallthrough
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
	case "SMALLDATETIME":
		t := (val).(time.Time)
		return t.Format("2006-01-02 15:04:05"), nil
	case "MONEY":
		fallthrough
	case "SMALLMONEY":
		fallthrough
	case "NUMERIC":
		fallthrough
	case "DECIMAL":
		v := val.([]uint8)
		return string(v), nil
	case "UNIQUEIDENTIFIER":
		byte_val := val.([]byte)

		var guid mssql.UniqueIdentifier
		if err := guid.Scan(byte_val); err != nil {
			return "", err
		}

		return guid.String(), nil
	}

	return UnsupportedColumnTypeOutput, nil
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
