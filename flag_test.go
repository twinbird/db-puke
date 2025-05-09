package main

import (
	"io"
	"testing"
)

func TestEmptyTableOption(t *testing.T) {
	option, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"-d",
		"dummy_database",
		"-s",
		"dummy_schema",
		"-u",
		"sa",
		"-P",
		"saPassword1234",
		"-t",
		",",
	}, io.Discard)

	if err != nil {
		t.Fatalf("want error: 'nil', but got '%s'", err)
	}

	if len(option.ParsedTableNames) != 0 {
		t.Errorf("want: '', but got '%v'", option.ParsedTableNames)
	}
}
