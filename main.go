package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

const (
	DBTypeMSSql = "mssql"
)

type Option struct {
	DBType   string
	Host     string
	Port     int
	Database string
	Schema   string
	User     string
	Password string
	OutDir   string
}

func parseArgs() *Option {
	option := &Option{}

	flag.StringVar(&option.DBType, "type", "mssql", "database type (mssql)")
	flag.StringVar(&option.Host, "h", "localhost", "hostname")
	flag.IntVar(&option.Port, "p", 1433, "port")
	flag.StringVar(&option.Database, "d", "", "database")
	flag.StringVar(&option.Schema, "s", "", "schema")
	flag.StringVar(&option.User, "u", "", "username")
	flag.StringVar(&option.Password, "P", "", "password")
	flag.StringVar(&option.OutDir, "o", "db-puke-exported", "export dir")

	flag.Parse()

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
	option := parseArgs()

	exec(option)

	os.Exit(0)
}

func exec(option *Option) {
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		option.User, option.Password, option.Host, option.Port, option.Database)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error: Failed to connect to the database", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Error: Failed to connect to the database", err)
	}

	tables, err := getTables(db, option.Schema)
	if err != nil {
		log.Fatal("Failed to retrieve the list of tables", err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(len(tables))
	for _, table := range tables {
		go func(t string) {
			defer wg.Done()
			err := exportTableToCSV(db, option.Schema, t, option.OutDir)
			if err != nil {
				log.Printf("Failed %s %v\n", t, err)
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

func getColumnType(db *sql.DB, schema_name, table_name string) (map[string]string, error) {
	query := `
		SELECT
			 COLUMN_NAME
			,DATA_TYPE
		FROM
			INFORMATION_SCHEMA.COLUMNS
		WHERE
			TABLE_NAME = @table_name
		AND
			TABLE_SCHEMA = @schema_name;
	`
	rows, err := db.QueryContext(
		context.Background(),
		query,
		sql.Named("table_name", table_name),
		sql.Named("schema_name", schema_name),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]string, 0)
	for rows.Next() {
		var column_name, data_type string

		err := rows.Scan(&column_name, &data_type)
		if err != nil {
			return nil, err
		}
		columns[column_name] = data_type
	}
	return columns, nil
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

func formatData(val any, ty string) string {
	if val == nil {
		return "NULL"
	}

	switch ty {
	case "INT":
		return fmt.Sprintf("%d", val)
	case "SMALLINT":
		return fmt.Sprintf("%d", val)
	case "TINYINT":
		return fmt.Sprintf("%d", val)
	case "BIT":
		if val == true {
			return "1"
		} else {
			return "0"
		}
	case "FLOAT":
		return fmt.Sprintf("%g", val)
	case "REAL":
		return fmt.Sprintf("%g", val)
	case "VARCHAR":
		return fmt.Sprintf("%s", val)
	case "CHAR":
		return fmt.Sprintf("%s", val)
	case "DATETIME":
		t := (val).(time.Time)
		return t.Format(time.DateTime)
	}

	return "[NOT SUPPORTED COLUMN TYPE]"
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
			record = append(record, formatData(val, ty.DatabaseTypeName()))
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
