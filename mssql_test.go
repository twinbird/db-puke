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
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (0);
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
		INSERT INTO dummy_schema.test_smallint_column_table (smallint_col) VALUES (0);
		INSERT INTO dummy_schema.test_smallint_column_table (smallint_col) VALUES (32767);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_smallint_column_table.csv", "testdata/mssql/test_smallint_column_table.csv")
}

func TestTinyintColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_tinyint_column_table;
		CREATE TABLE dummy_schema.test_tinyint_column_table (
			tinyint_col TINYINT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_tinyint_column_table (tinyint_col) VALUES (0);
		INSERT INTO dummy_schema.test_tinyint_column_table (tinyint_col) VALUES (255);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_tinyint_column_table.csv", "testdata/mssql/test_tinyint_column_table.csv")
}

func TestFloatColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_float_column_table;
		CREATE TABLE dummy_schema.test_float_column_table (
			float_col FLOAT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (-1.79E+308);
		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (-2.23E-308);

		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (0);

		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (2.23E-308);
		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (1.79E+308);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_float_column_table.csv", "testdata/mssql/test_float_column_table.csv")
}

func TestRealColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_real_column_table;
		CREATE TABLE dummy_schema.test_real_column_table (
			real_col REAL NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (-3.40E+38);
		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (-1.18E-38);

		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (0);

		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (1.18E-38);
		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (3.40E+38);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_real_column_table.csv", "testdata/mssql/test_real_column_table.csv")
}

func TestCharColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_char_column_table;
		CREATE TABLE dummy_schema.test_char_column_table (
			char_col CHAR(30) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('a');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('                             a');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('012345678901234567890123456789');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('nonescapestring:nonescapestrin');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('shouldbeescape
shouldbeescape');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('shouldbeescape"shouldbeescape"');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('shouldbeescape,shouldbeescape,');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('TEST string');
		INSERT INTO dummy_schema.test_char_column_table (char_col) VALUES ('日本語の文字列');
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_char_column_table.csv", "testdata/mssql/test_char_column_table.csv")
}

func TestVarcharColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_varchar_column_table;
		CREATE TABLE dummy_schema.test_varchar_column_table (
			varchar_col varchar(30) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('a');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('                             a');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('012345678901234567890123456789');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('nonescapestring:nonescapestrin');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('shouldbeescape
shouldbeescape');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('shouldbeescape"shouldbeescape"');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('shouldbeescape,shouldbeescape,');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('TEST string');
		INSERT INTO dummy_schema.test_varchar_column_table (varchar_col) VALUES ('日本語の文字列');
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_varchar_column_table.csv", "testdata/mssql/test_varchar_column_table.csv")
}

func TestDatetimeColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_datetime_column_table;
		CREATE TABLE dummy_schema.test_datetime_column_table (
			datetime_col datetime NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_datetime_column_table (datetime_col) VALUES ('2025-03-22 21:54:24');
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_datetime_column_table.csv", "testdata/mssql/test_datetime_column_table.csv")
}

func TestBitColumn(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_bit_column_table;
		CREATE TABLE dummy_schema.test_bit_column_table (
			bit_col bit NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_bit_column_table (bit_col) VALUES (0);
		INSERT INTO dummy_schema.test_bit_column_table (bit_col) VALUES (1);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_bit_column_table.csv", "testdata/mssql/test_bit_column_table.csv")
}

func TestMultipleTableOutput(t *testing.T) {
	// Create table for test
	execSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_multiple_table_output1;
		CREATE TABLE dummy_schema.test_multiple_table_output1 (
			col1 int NOT NULL PRIMARY KEY
		);
		DROP TABLE IF EXISTS dummy_schema.test_multiple_table_output2;
		CREATE TABLE dummy_schema.test_multiple_table_output2 (
			col2 int NOT NULL PRIMARY KEY
		);
		DROP TABLE IF EXISTS dummy_schema.test_multiple_table_output3;
		CREATE TABLE dummy_schema.test_multiple_table_output3 (
			col3 int NOT NULL PRIMARY KEY
		);
		DROP TABLE IF EXISTS dummy_schema.test_multiple_table_output4;
		CREATE TABLE dummy_schema.test_multiple_table_output4 (
			col4 int NOT NULL PRIMARY KEY
		);
		DROP TABLE IF EXISTS dummy_schema.test_multiple_table_output5;
		CREATE TABLE dummy_schema.test_multiple_table_output5 (
			col5 int NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_multiple_table_output1 (col1) VALUES (1);
		INSERT INTO dummy_schema.test_multiple_table_output1 (col1) VALUES (10);

		INSERT INTO dummy_schema.test_multiple_table_output2 (col2) VALUES (2);
		INSERT INTO dummy_schema.test_multiple_table_output2 (col2) VALUES (20);

		INSERT INTO dummy_schema.test_multiple_table_output3 (col3) VALUES (3);
		INSERT INTO dummy_schema.test_multiple_table_output3 (col3) VALUES (30);

		INSERT INTO dummy_schema.test_multiple_table_output4 (col4) VALUES (4);
		INSERT INTO dummy_schema.test_multiple_table_output4 (col4) VALUES (40);

		INSERT INTO dummy_schema.test_multiple_table_output5 (col5) VALUES (5);
		INSERT INTO dummy_schema.test_multiple_table_output5 (col5) VALUES (50);
	`)

	msSqlOption.OutDir = "testoutdir/mssql"
	exec(msSqlOption)

	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output1.csv", "testdata/mssql/test_multiple_table_output1.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output2.csv", "testdata/mssql/test_multiple_table_output2.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output3.csv", "testdata/mssql/test_multiple_table_output3.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output4.csv", "testdata/mssql/test_multiple_table_output4.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output5.csv", "testdata/mssql/test_multiple_table_output5.csv")
}
