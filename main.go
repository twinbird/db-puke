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

type Config struct {
	DBType   string
	Host     string
	Port     int
	Database string
	Schema   string
	User     string
	Password string
	OutDir   string
}

func parseArgs() *Config {
	config := &Config{}

	flag.StringVar(&config.DBType, "type", "mssql", "database type (mssql)")
	flag.StringVar(&config.Host, "h", "localhost", "hostname")
	flag.IntVar(&config.Port, "p", 1433, "port")
	flag.StringVar(&config.Database, "d", "", "database")
	flag.StringVar(&config.Schema, "s", "", "schema")
	flag.StringVar(&config.User, "u", "", "username")
	flag.StringVar(&config.Password, "P", "", "password")
	flag.StringVar(&config.OutDir, "o", "db-puke-exported", "export dir")

	flag.Parse()

	if config.Database == "" {
		fmt.Println("Error: Please specify the database name (-d)")
		os.Exit(1)
	}
	if config.Schema == "" {
		fmt.Println("Error: Please specify the schema name (-s)")
		os.Exit(1)
	}
	if config.User == "" {
		fmt.Println("Error: Please specify the username (-u)")
		os.Exit(1)
	}
	if config.Password == "" {
		fmt.Println("Error: Please specify the database password (-P)")
		os.Exit(1)
	}

	return config
}

func main() {
	config := parseArgs()

	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error: Failed to connect to the database", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Error: Failed to connect to the database", err)
	}

	tables, err := getTables(db, config.Schema)
	if err != nil {
		log.Fatal("Failed to retrieve the list of tables", err)
	}
	log.Println(tables)

	for _, table := range tables {
		err := exportTableToCSV(db, config.Schema, table, config.OutDir)
		if err != nil {
			log.Printf("Failed %s %v\n", table, err)
		} else {
			fmt.Printf("Success %s\n", table)
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
