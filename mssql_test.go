package main

import (
	"bytes"
	"database/sql"
	"fmt"
	_ "github.com/microsoft/go-mssqldb"
	"log"
	"os"
	"testing"
)

var msSqlOption = &Option{
	DBType:   DBTypeMSSql,
	Host:     "127.0.0.1",
	Port:     1433,
	Database: "dummy_database",
	Schema:   "dummy_schema",
	User:     "sa",
	Password: "saPassword1234",
	OutDir:   "testoutdir/test_int_column",
}

func execSQL(query string) {
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		msSqlOption.User, msSqlOption.Password, msSqlOption.Host, msSqlOption.Port, "")

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error: Failed to connect to the database", err)
	}
	defer db.Close()

	_, err = db.Exec(query)
	if err != nil {
		log.Fatal("Error: Failed to Exec", err)
	}
}

func CompareFiles(file1, file2 string) (bool, error) {
	data1, err := os.ReadFile(file1)
	if err != nil {
		return false, err
	}

	data2, err := os.ReadFile(file2)
	if err != nil {
		return false, err
	}

	return bytes.Equal(data1, data2), nil
}

func AssertCompareFiles(t *testing.T, file1, file2 string) {
	ret, err := CompareFiles(file1, file2)
	if err != nil {
		t.Errorf("file compare failed: %v", err)
	}

	if ret == false {
		t.Errorf("output file is not equal")
	}
}

func createDatabaseAndSchema() {
	execSQL(`
		DROP DATABASE IF EXISTS dummy_database;
		CREATE DATABASE dummy_database;
	`)

	execSQL(`
		USE dummy_database;
		IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dummy_schema')
		BEGIN
			EXEC('CREATE SCHEMA dummy_schema AUTHORIZATION dbo;');
		END;
	`)
}

func TestMain(m *testing.M) {
	createDatabaseAndSchema()

	m.Run()
}

func TestIntColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_int_column_table;
		CREATE TABLE dummy_schema.test_int_column_table (
			int_col INT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (1);
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (2);
	`)

	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/test_int_column/test_int_column_table.csv", "testdata/mssql/test_int_column_table.csv")
}
