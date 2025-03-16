package main

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "github.com/microsoft/go-mssqldb"
)

var msSqlOption = &Option{
	DBType:   DBTypeMSSql,
	Host:     "127.0.0.1",
	Port:     1433,
	Database: "dummy_database",
	Schema:   "dummy_schema",
	User:     "sa",
	Password: "saPassword1234",
	OutDir:   "",
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
	RemoveTestOutputFile("testoutdir/mssql")
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
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (-2147483648);
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (2147483647);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_int_column_table.csv", "testdata/mssql/test_int_column_table.csv")
}

func TestSmallintColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_smallint_column_table;
		CREATE TABLE dummy_schema.test_smallint_column_table (
			smallint_col SMALLINT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_smallint_column_table (smallint_col) VALUES (-32768);
		INSERT INTO dummy_schema.test_smallint_column_table (smallint_col) VALUES (32767);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_smallint_column_table.csv", "testdata/mssql/test_smallint_column_table.csv")
}
