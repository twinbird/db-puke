package main

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "github.com/microsoft/go-mssqldb"
)

var msSqlTestOption = &Option{
	DBType:        DBTypeMSSql,
	Host:          "127.0.0.1",
	Port:          1433,
	Database:      "dummy_database",
	Schema:        "dummy_schema",
	User:          "sa",
	Password:      "saPassword1234",
	OutDir:        "",
	NullRepresent: "NULL",
}

func execMssqlTestSQL(query string) {
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		msSqlTestOption.User, msSqlTestOption.Password, msSqlTestOption.Host, msSqlTestOption.Port, "")

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

func createTestDatabaseAndSchemaMssql() {
	execMssqlTestSQL(`
		DROP DATABASE IF EXISTS dummy_database;
		CREATE DATABASE dummy_database;
	`)

	execMssqlTestSQL(`
		USE dummy_database;
		IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dummy_schema')
		BEGIN
			EXEC('CREATE SCHEMA dummy_schema AUTHORIZATION dbo;');
		END;
	`)
}

func TestMain(m *testing.M) {
	RemoveTestOutputFile("testoutdir/mssql")
	createTestDatabaseAndSchemaMssql()

	m.Run()
}

func TestMssqlIntColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_int_column_table;
		CREATE TABLE dummy_schema.test_int_column_table (
			int_col INT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (-2147483648);
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (0);
		INSERT INTO dummy_schema.test_int_column_table (int_col) VALUES (2147483647);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_int_column_table.csv", "testdata/mssql/test_int_column_table.csv")
}

func TestMssqlBigIntColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_bigint_column_table;
		CREATE TABLE dummy_schema.test_bigint_column_table (
			bigint_col BIGINT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_bigint_column_table (bigint_col) VALUES (-9223372036854775808);
		INSERT INTO dummy_schema.test_bigint_column_table (bigint_col) VALUES (0);
		INSERT INTO dummy_schema.test_bigint_column_table (bigint_col) VALUES (9223372036854775807);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_bigint_column_table.csv", "testdata/mssql/test_bigint_column_table.csv")
}

func TestMssqlSmallintColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_smallint_column_table;
		CREATE TABLE dummy_schema.test_smallint_column_table (
			smallint_col SMALLINT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_smallint_column_table (smallint_col) VALUES (-32768);
		INSERT INTO dummy_schema.test_smallint_column_table (smallint_col) VALUES (0);
		INSERT INTO dummy_schema.test_smallint_column_table (smallint_col) VALUES (32767);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_smallint_column_table.csv", "testdata/mssql/test_smallint_column_table.csv")
}

func TestMssqlTinyintColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_tinyint_column_table;
		CREATE TABLE dummy_schema.test_tinyint_column_table (
			tinyint_col TINYINT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_tinyint_column_table (tinyint_col) VALUES (0);
		INSERT INTO dummy_schema.test_tinyint_column_table (tinyint_col) VALUES (255);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_tinyint_column_table.csv", "testdata/mssql/test_tinyint_column_table.csv")
}

func TestMssqlFloatColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_float_column_table;
		CREATE TABLE dummy_schema.test_float_column_table (
			float_col FLOAT NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (-1.79E+308);
		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (-2.23E-308);

		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (0);

		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (2.23E-308);
		INSERT INTO dummy_schema.test_float_column_table (float_col) VALUES (1.79E+308);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_float_column_table.csv", "testdata/mssql/test_float_column_table.csv")
}

func TestMssqlRealColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_real_column_table;
		CREATE TABLE dummy_schema.test_real_column_table (
			real_col REAL NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (-3.40E+38);
		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (-1.18E-38);

		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (0);

		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (1.18E-38);
		INSERT INTO dummy_schema.test_real_column_table (real_col) VALUES (3.40E+38);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_real_column_table.csv", "testdata/mssql/test_real_column_table.csv")
}

func TestMssqlDecimalColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_decimal_column_table;
		CREATE TABLE dummy_schema.test_decimal_column_table (
			decimal_col DECIMAL(15, 3) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_decimal_column_table (decimal_col) VALUES (-999999999999.999);
		INSERT INTO dummy_schema.test_decimal_column_table (decimal_col) VALUES (0);
		INSERT INTO dummy_schema.test_decimal_column_table (decimal_col) VALUES (999999999999.999);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_decimal_column_table.csv", "testdata/mssql/test_decimal_column_table.csv")
}

func TestMssqlNumericColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_numeric_column_table;
		CREATE TABLE dummy_schema.test_numeric_column_table (
			numeric_col NUMERIC(15, 3) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_numeric_column_table (numeric_col) VALUES (-999999999999.999);
		INSERT INTO dummy_schema.test_numeric_column_table (numeric_col) VALUES (0);
		INSERT INTO dummy_schema.test_numeric_column_table (numeric_col) VALUES (999999999999.999);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_numeric_column_table.csv", "testdata/mssql/test_numeric_column_table.csv")
}

func TestMssqlCharColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_char_column_table;
		CREATE TABLE dummy_schema.test_char_column_table (
			char_col CHAR(30) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
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

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_char_column_table.csv", "testdata/mssql/test_char_column_table.csv")
}

func TestMssqlNcharColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_nchar_column_table;
		CREATE TABLE dummy_schema.test_nchar_column_table (
			nchar_col NCHAR(30) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('a');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('                             a');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('012345678901234567890123456789');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('nonescapestring:nonescapestrin');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('shouldbeescape
shouldbeescape');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('shouldbeescape"shouldbeescape"');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('shouldbeescape,shouldbeescape,');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('TEST string');
		INSERT INTO dummy_schema.test_nchar_column_table (nchar_col) VALUES ('日本語の文字列');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_nchar_column_table.csv", "testdata/mssql/test_nchar_column_table.csv")
}

func TestMssqlTextColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_text_column_table;
		CREATE TABLE dummy_schema.test_text_column_table (
			text_col TEXT
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_text_column_table (text_col) VALUES ('');
		INSERT INTO dummy_schema.test_text_column_table (text_col) VALUES ('a');
		INSERT INTO dummy_schema.test_text_column_table (text_col) VALUES ('shouldbeescape
shouldbeescape');
		INSERT INTO dummy_schema.test_text_column_table (text_col) VALUES ('TEST string');
		INSERT INTO dummy_schema.test_text_column_table (text_col) VALUES ('日本語の文字列');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_text_column_table.csv", "testdata/mssql/test_text_column_table.csv")
}

func TestMssqlNtextColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_ntext_column_table;
		CREATE TABLE dummy_schema.test_ntext_column_table (
			ntext_col NTEXT
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_ntext_column_table (ntext_col) VALUES ('');
		INSERT INTO dummy_schema.test_ntext_column_table (ntext_col) VALUES ('a');
		INSERT INTO dummy_schema.test_ntext_column_table (ntext_col) VALUES ('shouldbeescape
shouldbeescape');
		INSERT INTO dummy_schema.test_ntext_column_table (ntext_col) VALUES ('TEST string');
		INSERT INTO dummy_schema.test_ntext_column_table (ntext_col) VALUES ('日本語の文字列');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_ntext_column_table.csv", "testdata/mssql/test_ntext_column_table.csv")
}

func TestMssqlVarcharColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_varchar_column_table;
		CREATE TABLE dummy_schema.test_varchar_column_table (
			varchar_col varchar(30) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
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

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_varchar_column_table.csv", "testdata/mssql/test_varchar_column_table.csv")
}

func TestMssqlNvarcharColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_nvarchar_column_table;
		CREATE TABLE dummy_schema.test_nvarchar_column_table (
			nvarchar_col nvarchar(30) NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('a');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('                             a');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('012345678901234567890123456789');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('nonescapestring:nonescapestrin');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('shouldbeescape
shouldbeescape');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('shouldbeescape"shouldbeescape"');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('shouldbeescape,shouldbeescape,');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('TEST string');
		INSERT INTO dummy_schema.test_nvarchar_column_table (nvarchar_col) VALUES ('日本語の文字列');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_nvarchar_column_table.csv", "testdata/mssql/test_nvarchar_column_table.csv")
}

func TestMssqlDateColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_date_column_table;
		CREATE TABLE dummy_schema.test_date_column_table (
			date_col date NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_date_column_table (date_col) VALUES ('2025-01-01');
		INSERT INTO dummy_schema.test_date_column_table (date_col) VALUES ('2025-03-03');
		INSERT INTO dummy_schema.test_date_column_table (date_col) VALUES ('2025-12-31');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_date_column_table.csv", "testdata/mssql/test_date_column_table.csv")
}

func TestMssqlDatetimeColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_datetime_column_table;
		CREATE TABLE dummy_schema.test_datetime_column_table (
			datetime_col datetime NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_datetime_column_table (datetime_col) VALUES ('2025-03-22 21:54:24');
		INSERT INTO dummy_schema.test_datetime_column_table (datetime_col) VALUES ('2025-03-22 21:54:24.123');
		INSERT INTO dummy_schema.test_datetime_column_table (datetime_col) VALUES ('2025-03-22 21:54:24.997');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_datetime_column_table.csv", "testdata/mssql/test_datetime_column_table.csv")
}

func TestMssqlSmalldatetimeColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_smalldatetime_column_table;
		CREATE TABLE dummy_schema.test_smalldatetime_column_table (
			smalldatetime_col smalldatetime NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_smalldatetime_column_table (smalldatetime_col) VALUES ('2025-03-22 21:54:24');
		INSERT INTO dummy_schema.test_smalldatetime_column_table (smalldatetime_col) VALUES ('2025-03-22 21:55:24');
		INSERT INTO dummy_schema.test_smalldatetime_column_table (smalldatetime_col) VALUES ('2025-03-22 21:55:34');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_smalldatetime_column_table.csv", "testdata/mssql/test_smalldatetime_column_table.csv")
}

func TestMssqlDatetime2Column(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_datetime2_column_table;
		CREATE TABLE dummy_schema.test_datetime2_column_table (
			datetime2_col datetime2 NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_datetime2_column_table (datetime2_col) VALUES ('2025-03-22 21:54:24.0000000');
		INSERT INTO dummy_schema.test_datetime2_column_table (datetime2_col) VALUES ('2025-03-22 21:54:24.1234567');
		INSERT INTO dummy_schema.test_datetime2_column_table (datetime2_col) VALUES ('2025-03-22 21:54:24.9999999');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_datetime2_column_table.csv", "testdata/mssql/test_datetime2_column_table.csv")
}

func TestMssqlMoneyColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_money_column_table;
		CREATE TABLE dummy_schema.test_money_column_table (
			money_col money NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_money_column_table (money_col) VALUES (-922337203685477);
		INSERT INTO dummy_schema.test_money_column_table (money_col) VALUES (-922337203685477.5808);

		INSERT INTO dummy_schema.test_money_column_table (money_col) VALUES (0);

		INSERT INTO dummy_schema.test_money_column_table (money_col) VALUES (922337203685477);
		INSERT INTO dummy_schema.test_money_column_table (money_col) VALUES (922337203685477.5807);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_money_column_table.csv", "testdata/mssql/test_money_column_table.csv")
}

func TestMssqlSmallmoneyColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_smallmoney_column_table;
		CREATE TABLE dummy_schema.test_smallmoney_column_table (
			smallmoney_col smallmoney NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_smallmoney_column_table (smallmoney_col) VALUES (-214748.3648);
		INSERT INTO dummy_schema.test_smallmoney_column_table (smallmoney_col) VALUES (-214748);

		INSERT INTO dummy_schema.test_smallmoney_column_table (smallmoney_col) VALUES (0);

		INSERT INTO dummy_schema.test_smallmoney_column_table (smallmoney_col) VALUES (214748);
		INSERT INTO dummy_schema.test_smallmoney_column_table (smallmoney_col) VALUES (214748.3647);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_smallmoney_column_table.csv", "testdata/mssql/test_smallmoney_column_table.csv")
}

func TestMssqlBitColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_bit_column_table;
		CREATE TABLE dummy_schema.test_bit_column_table (
			bit_col bit NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_bit_column_table (bit_col) VALUES (0);
		INSERT INTO dummy_schema.test_bit_column_table (bit_col) VALUES (1);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_bit_column_table.csv", "testdata/mssql/test_bit_column_table.csv")
}

func TestMssqlUniqueidentifierColumn(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_uniqueidentifier_column_table;
		CREATE TABLE dummy_schema.test_uniqueidentifier_column_table (
			uniqueidentifier_col uniqueidentifier NOT NULL PRIMARY KEY
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;
		INSERT INTO dummy_schema.test_uniqueidentifier_column_table (uniqueidentifier_col) VALUES ('0E984725-C51C-4BF4-9960-E1C80E27ABA0');
		INSERT INTO dummy_schema.test_uniqueidentifier_column_table (uniqueidentifier_col) VALUES ('4487A153-A228-4287-900C-FA2EF942B4EB');
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_uniqueidentifier_column_table.csv", "testdata/mssql/test_uniqueidentifier_column_table.csv")
}

func TestMssqlMultipleTableOutput(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
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
	execMssqlTestSQL(`
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

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output1.csv", "testdata/mssql/test_multiple_table_output1.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output2.csv", "testdata/mssql/test_multiple_table_output2.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output3.csv", "testdata/mssql/test_multiple_table_output3.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output4.csv", "testdata/mssql/test_multiple_table_output4.csv")
	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_table_output5.csv", "testdata/mssql/test_multiple_table_output5.csv")
}

func TestMssqlMultipleColumnOutput(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_multiple_column_output;
		CREATE TABLE dummy_schema.test_multiple_column_output (
			col1 int NOT NULL PRIMARY KEY,
			col2 varchar(32) NOT NULL,
			col3 float,
			col4 bit NOT NULL
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;

		INSERT INTO dummy_schema.test_multiple_column_output (col1, col2, col3, col4)
		VALUES (1, 'test row 1', 3.14, 0);

		INSERT INTO dummy_schema.test_multiple_column_output (col1, col2, col3, col4)
		VALUES (2, 'test row 2', NULL, 1);

		INSERT INTO dummy_schema.test_multiple_column_output (col1, col2, col3, col4)
		VALUES (3, '', NULL, 0);

		INSERT INTO dummy_schema.test_multiple_column_output (col1, col2, col3, col4)
		VALUES (4, 'TEST,STRING', 3.3, 1);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_multiple_column_output.csv", "testdata/mssql/test_multiple_column_output.csv")
}

func TestMssqlUnsupportedColumnOutput(t *testing.T) {
	// Create table for test
	execMssqlTestSQL(`
		USE dummy_database;
		DROP TABLE IF EXISTS dummy_schema.test_unsupported_column_output;
		CREATE TABLE dummy_schema.test_unsupported_column_output (
			col1 int NOT NULL PRIMARY KEY,
			unsupported_col sql_variant,
			col2 varchar(32) NOT NULL,
			col3 float
		);
	`)
	// Insert test data
	execMssqlTestSQL(`
		USE dummy_database;

		INSERT INTO dummy_schema.test_unsupported_column_output (col1, unsupported_col, col2, col3)
		VALUES (1, CAST(1 AS INT), 'test row 1', 3.14);

		INSERT INTO dummy_schema.test_unsupported_column_output (col1, unsupported_col, col2, col3)
		VALUES (2, CAST(1 AS INT), 'test row 2', NULL);

		INSERT INTO dummy_schema.test_unsupported_column_output (col1, unsupported_col, col2, col3)
		VALUES (3, CAST(1 AS INT), '', NULL);

		INSERT INTO dummy_schema.test_unsupported_column_output (col1, unsupported_col, col2, col3)
		VALUES (4, CAST(1 AS INT), 'TEST,STRING', 3.3);
	`)

	msSqlTestOption.OutDir = "testoutdir/mssql"
	commandOption = msSqlTestOption
	exec()

	AssertCompareFiles(t, "testoutdir/mssql/test_unsupported_column_output.csv", "testdata/mssql/test_unsupported_column_output.csv")
}
