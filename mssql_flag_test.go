package main

import (
	"io"
	"testing"
)

func TestMssqlValidMinimumArgs(t *testing.T) {
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

func TestMssqlRootHelpArgs(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"--help",
	}, io.Discard)
	if err == nil {
		t.Errorf("call by --help args. want return error, but got nil")
	}
}

func TestMssqlSubcommandHelpArgs(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"--help",
	}, io.Discard)
	if err == nil {
		t.Errorf("call by --help args. want return error, but got nil")
	}
}

func TestMssqlPasswordFromEnv(t *testing.T) {
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

func TestMssqlNoSpecifiedHost(t *testing.T) {
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

func TestMssqlNoSpecifiedDatabase(t *testing.T) {
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

func TestMssqlNoSpecifiedSchema(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"-d",
		"dummy_database",
		"-u",
		"sa",
		"-P",
		"saPassword",
	}, io.Discard)

	if err == nil {
		t.Fatalf("call by invalid args. want error: '%s', but got nil", MssqlNoSpecifiedSchemaMessage)
	}

	if err.Error() != MssqlNoSpecifiedSchemaMessage {
		t.Fatalf("call by invalid args. want error: '%s', but got '%s'", MssqlNoSpecifiedSchemaMessage, err.Error())
	}
}

func TestMssqlNoSpecifiedUser(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"-d",
		"dummy_database",
		"-s",
		"dummy_schema",
		"-P",
		"saPassword",
	}, io.Discard)

	if err == nil {
		t.Fatalf("call by invalid args. want error: '%s', but got nil", MssqlNoSpecifiedUserMessage)
	}

	if err.Error() != MssqlNoSpecifiedUserMessage {
		t.Fatalf("call by invalid args. want error: '%s', but got '%s'", MssqlNoSpecifiedUserMessage, err.Error())
	}
}

func TestMssqlNoSpecifiedPassword(t *testing.T) {
	_, err := parseArgs([]string{
		"db-puke",
		"mssql",
		"-d",
		"dummy_database",
		"-s",
		"dummy_schema",
		"-u",
		"sa",
	}, io.Discard)

	if err == nil {
		t.Fatalf("call by invalid args. want error: '%s', but got nil", MssqlNoSpecifiedPasswordMessage)
	}

	if err.Error() != MssqlNoSpecifiedPasswordMessage {
		t.Fatalf("call by invalid args. want error: '%s', but got '%s'", MssqlNoSpecifiedPasswordMessage, err.Error())
	}
}

func TestMssqlInvalidPortSpecified(t *testing.T) {
	_, err := parseArgs([]string{
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
		"-p",
		"foo",
	}, io.Discard)

	if err == nil {
		t.Fatalf("call by invalid args. want error: '%s', but got nil", MssqlInvalidPortSpecifiedMessage)
	}

	if err.Error() != MssqlInvalidPortSpecifiedMessage {
		t.Fatalf("call by invalid args. want error: '%s', but got '%s'", MssqlInvalidPortSpecifiedMessage, err.Error())
	}
}

func TestMssqlDefaultPort(t *testing.T) {
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
	}, io.Discard)

	if err != nil {
		t.Fatalf("want error: 'nil', but got '%s'", err)
	}

	if option.Port != MssqlDefaultPort {
		t.Errorf("mssql default port want: '%s', but got", MssqlNoSpecifiedPasswordMessage)
	}
}
