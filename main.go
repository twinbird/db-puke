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

	for _, table := range tables {
		err := exportTableToCSV(db, option.Schema, table, option.OutDir)
		if err != nil {
			log.Printf("Failed %s %v\n", table, err)
		}
	}
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

func exportTableToCSV(db *sql.DB, schema, table string, outdir string) error {
	column_types, err := getColumnType(db, schema, table)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("SELECT * FROM [%s].[%s]", schema, table)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fileName, err := getOutputFilePath(outdir, table)
	if err != nil {
		return err
	}
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(columns); err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		var record []string
		for _, val := range values {
			if val == nil {
				record = append(record, "NULL")
			} else {
				record = append(record, fmt.Sprintf("%v", val))
			}
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}
