package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
)

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

func createOutputFile(table string) (*os.File, error) {
	fileName, err := getOutputFilePath(commandOption.OutDir, table)
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

func writeOutputBody(operator DBPukeOperator, rows *sql.Rows, writer *csv.Writer) error {
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
			val_str, err := operator.FormatData(val, ty)
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
