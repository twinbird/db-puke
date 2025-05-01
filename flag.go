package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

var (
	commandOption *Option
)

type Option struct {
	DBType           string
	Host             string
	PortString       string
	Port             int
	Database         string
	Schema           string
	User             string
	Password         string
	OutDir           string
	NullRepresent    string
	TableNames       string
	ParsedTableNames []string
}

func rootUsageMessage() error {
	return fmt.Errorf(`db-puke - database data exporter [version %s]

Usage:
  db-puke <database type> -h <hostname> -d <database name> -s <database schema> -u <username> -P <password>

Example:
  mssql(SQLServer):
    DB_PUKE_PASSWORD=saPassword1234 db-puke mssql -h localhost -d dummy_database -s dummy_schema -u sa

See more:
  'db-puke <database type> --help'
`, DBPukeVersion)
}

func parseArgs(args []string, errWriter io.Writer) (*Option, error) {
	option := &Option{}

	if len(args) < 3 {
		return option, rootUsageMessage()
	}

	option.DBType = args[1]

	fs := flag.NewFlagSet(option.DBType, flag.ContinueOnError)
	fs.SetOutput(errWriter)
	setCommonFlag(option, fs)

	switch option.DBType {
	case DBTypeMSSql:
		flag.ErrHelp = mssqlUsageMessage(args[0])
		setMssqlFlag(option, fs)
	default:
		return nil, fmt.Errorf("error: specify database type(%s) is not supported\n", option.DBType)
	}

	if err := fs.Parse(args[2:]); err != nil {
		if err == flag.ErrHelp {
			return nil, fmt.Errorf("")
		}
		return nil, err
	}

	setFromEnv(option)

	switch option.DBType {
	case DBTypeMSSql:
		if err := validateMssqlOption(option); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("error: specify database type(%s) is not supported\n", option.DBType)
	}

	option.ParsedTableNames = parseTableOption(option.TableNames)

	return option, nil
}

func setCommonFlag(option *Option, fs *flag.FlagSet) {
	fs.StringVar(&option.OutDir, "o", "db-puke-exported", "export directory")
	fs.StringVar(&option.NullRepresent, "N", "NULL", "string to represent NULL")
	fs.StringVar(&option.TableNames, "t", "", "table names to export (comma-separated). exports all tables if omitted.")
}

func setFromEnv(option *Option) {
	if pass, ok := os.LookupEnv(DBPukeEnvironmentNamePassword); ok {
		option.Password = pass
	}
}

func parseTableOption(opstr string) []string {
	s := strings.Trim(opstr, " ")
	splitted := strings.Split(s, ",")
	ret := make([]string, 0)
	for _, tname := range splitted {
		tname = strings.Trim(tname, " ")
		if tname != "" {
			ret = append(ret, tname)
		}
	}
	return ret
}
