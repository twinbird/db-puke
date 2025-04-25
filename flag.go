package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
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

func parseArgs() *Option {
	option := &Option{}

	flag.StringVar(&option.DBType, "type", "", "database server type [mssql]")
	flag.StringVar(&option.Host, "h", "localhost", "database server host")
	flag.StringVar(&option.PortString, "p", "", "database server port")
	flag.StringVar(&option.Database, "d", "", "database")
	flag.StringVar(&option.Schema, "s", "", "database schema")
	flag.StringVar(&option.User, "u", "", "database user name")
	flag.StringVar(&option.Password, "P", "", "database user password(or use DB_PUKE_PASSWORD env var)")
	flag.StringVar(&option.OutDir, "o", "db-puke-exported", "export directory")
	flag.StringVar(&option.NullRepresent, "N", "NULL", "string to represent NULL")
	flag.StringVar(&option.TableNames, "t", "", "table names to export (comma-separated). exports all tables if omitted.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `%s - database data exporter [version %s]

Usage:
  db-puke -type <database type> -h <hostname> -p <access port> -d <database name> -s <database schema> -u <username> -P <password> -o <output dir>

Example:
  mssql:
    DB_PUKE_PASSWORD=saPassword1234 ./db-puke -type mssql -h localhost -p 1433 -d dummy_database -s dummy_schema -u sa -o outdir

Options:
`, os.Args[0], DBPukeVersion)
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "  --help\n\tshow this help message and exit")
	}

	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(0)
	}

	flag.Parse()

	if pass, ok := os.LookupEnv(DBPukeEnvironmentNamePassword); ok {
		option.Password = pass
	}

	switch option.DBType {
	case DBTypeMSSql:
		parseMssqlOption(option)
	default:
		fmt.Fprintf(os.Stderr, "Error: Specify database type(%s) is not supported\n", option.DBType)
		os.Exit(1)
	}

	option.ParsedTableNames = parseTableOption(option.TableNames)

	return option
}

func parseMssqlOption(option *Option) {
	if option.Database == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the database name (-d)")
		os.Exit(1)
	}
	if option.Schema == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the schema name (-s)")
		os.Exit(1)
	}
	if option.User == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the username (-u)")
		os.Exit(1)
	}
	if option.Password == "" {
		fmt.Fprintln(os.Stderr, "Error: Please specify the database password (-P)")
		os.Exit(1)
	}
	if option.PortString == "" {
		option.Port = 1433
	} else {
		port, err := strconv.Atoi(option.PortString)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error: Invalid port number (-p)")
			os.Exit(1)
		}
		option.Port = port
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
