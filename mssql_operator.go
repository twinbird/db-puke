package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
)

type MSSqlOperator struct {
	connString string
	db         *sql.DB
}

func NewMSSqlOperator() *MSSqlOperator {
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		commandOption.User, commandOption.Password, commandOption.Host, commandOption.Port, commandOption.Database)
	return &MSSqlOperator{connString: connString}
}

func (o *MSSqlOperator) DBOpen() error {
	db, err := sql.Open("sqlserver", o.connString)
	if err != nil {
		return err
	}
	o.db = db

	err = db.Ping()
	if err != nil {
		return err
	}

	return nil
}

func (o *MSSqlOperator) DBClose() error {
	return o.db.Close()
}

func (o *MSSqlOperator) GetTableNames() ([]string, error) {
	db := o.db
	schema := commandOption.Schema

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

func (o *MSSqlOperator) QueryAllRecords(table string) (*sql.Rows, error) {
	db := o.db
	schema := commandOption.Schema

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM [%s].[%s]", schema, table))
	if err != nil {
		return nil, err
	}

	return rows, nil
}

func (o *MSSqlOperator) FormatData(val any, ty *sql.ColumnType) (string, error) {
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
