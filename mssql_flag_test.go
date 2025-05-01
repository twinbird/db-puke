package main

import (
	"io"
	"testing"
)

func TestValidMinimumArgs(t *testing.T) {
	option, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"-h",
		"localhost",
		"-d",
		"dummy_database",
		"-s",
		"dummy_schema",
		"-u",
		"sa",
		"-P",
		"saPassword",
	}, io.Discard)
	if err != nil {
		t.Fatalf("call by valid args. want: nil, but got %s", err.Error())
	}

	if option.DBType != DBTypeMSSql {
		t.Errorf("option.DBType want: %s, but got %s", DBTypeMSSql, option.DBType)
	}

	if option.Host != "localhost" {
		t.Errorf("option.Host want: %s, but got %s", "localhost", option.Host)
	}

	if option.Database != "dummy_database" {
		t.Errorf("option.Database want: %s, but got %s", "dummy_database", option.Database)
	}

	if option.User != "sa" {
		t.Errorf("option.User want: %s, but got %s", "sa", option.User)
	}

	if option.Schema != "dummy_schema" {
		t.Errorf("option.Schema want: %s, but got %s", "dummy_schema", option.Schema)
	}

	if option.Password != "saPassword" {
		t.Errorf("option.Password want: %s, but got %s", "saPassword", option.Password)
	}
}

func TestRootHelpArgs(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"--help",
	}, io.Discard)
	if err == nil {
		t.Errorf("call by --help args. want return error, but got nil")
	}
}

func TestSubcommandHelpArgs(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"--help",
	}, io.Discard)
	if err == nil {
		t.Errorf("call by --help args. want return error, but got nil")
	}
}

func TestPasswordFromEnv(t *testing.T) {
	t.Setenv(DBPukeEnvironmentNamePassword, "saPassword")
	option, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"-h",
		"localhost",
		"-d",
		"dummy_database",
		"-s",
		"dummy_schema",
		"-u",
		"sa",
	}, io.Discard)

	if err != nil {
		t.Fatalf("call by valid args. want: nil, but got %s", err.Error())
	}

	if option.DBType != DBTypeMSSql {
		t.Errorf("option.DBType want: %s, but got %s", DBTypeMSSql, option.DBType)
	}

	if option.Host != "localhost" {
		t.Errorf("option.Host want: %s, but got %s", "localhost", option.Host)
	}

	if option.Database != "dummy_database" {
		t.Errorf("option.Database want: %s, but got %s", "dummy_database", option.Database)
	}

	if option.User != "sa" {
		t.Errorf("option.User want: %s, but got %s", "sa", option.User)
	}

	if option.Schema != "dummy_schema" {
		t.Errorf("option.Schema want: %s, but got %s", "dummy_schema", option.Schema)
	}

	if option.Password != "saPassword" {
		t.Errorf("option.Password want: %s, but got %s", "saPassword", option.Password)
	}
}

func TestNoSpecifiedHost(t *testing.T) {
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
		"saPassword",
	}, io.Discard)

	if err != nil {
		t.Fatalf("call by valid args. want: nil, but got '%s'", err.Error())
	}

	if option.Host != "localhost" {
		t.Fatalf("want: localhost, but got %s", option.Host)
	}
}

func TestNoSpecifiedDatabase(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"-s",
		"dummy_schema",
		"-u",
		"sa",
		"-P",
		"saPassword",
	}, io.Discard)

	if err == nil {
		t.Fatalf("call by invalid args. want error: '%s', but got nil", MssqlNoSpecifiedDatabaseMessage)
	}

	if err.Error() != MssqlNoSpecifiedDatabaseMessage {
		t.Fatalf("call by invalid args. want error: '%s', but got '%s'", MssqlNoSpecifiedDatabaseMessage, err.Error())
	}
}
