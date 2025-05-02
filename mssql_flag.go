package main

import (
	"flag"
	"fmt"
	"strconv"
)

const (
	MssqlNoSpecifiedDatabaseMessage = "error: please specify the database name (-d)\n"
	MssqlNoSpecifiedSchemaMessage   = "error: please specify the schema name (-s)\n"
	MssqlNoSpecifiedUserMessage     = "error: please specify the username (-u)\n"
)

func mssqlUsageMessage(prg_name string) error {
	return fmt.Errorf(`%s - database data exporter [version %s]

Usage:
  %s <database type> -h <hostname> -d <database name> -s <database schema> -u <username> -P <password>

Example:
  mssql(SQLServer):
    DB_PUKE_PASSWORD=saPassword1234 %s mssql -h localhost -d dummy_database -s dummy_schema -u sa

See more:
  '%s <database type> --help'
`, prg_name, DBPukeVersion, prg_name, prg_name, prg_name)
}

func setMssqlFlag(option *Option, fs *flag.FlagSet) {
	fs.StringVar(&option.Host, "h", "localhost", "database server host")
	fs.StringVar(&option.PortString, "p", "", "database server port")
	fs.StringVar(&option.Database, "d", "", "database")
	fs.StringVar(&option.Schema, "s", "", "database schema")
	fs.StringVar(&option.User, "u", "", "database user name")
	fs.StringVar(&option.Password, "P", "", "database user password(or use DB_PUKE_PASSWORD env var)")
}

func validateMssqlOption(option *Option) error {
	if option.Database == "" {
		return fmt.Errorf(MssqlNoSpecifiedDatabaseMessage)
	}
	if option.Schema == "" {
		return fmt.Errorf(MssqlNoSpecifiedSchemaMessage)
	}
	if option.User == "" {
		return fmt.Errorf(MssqlNoSpecifiedUserMessage)
	}
	if option.Password == "" {
		return fmt.Errorf("error: please specify the database password (-P)\n")
	}
	if option.PortString == "" {
		option.Port = 1433
	} else {
		port, err := strconv.Atoi(option.PortString)
		if err != nil {
			return fmt.Errorf("error: invalid port number (-p)\n")
		}
		option.Port = port
	}

	return nil
}
