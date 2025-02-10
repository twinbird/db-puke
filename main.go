package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/microsoft/go-mssqldb"
)

type Config struct {
	DBType   string
	Host     string
	Port     int
	Database string
	Schema   string
	User     string
	Password string
	OutDir   string
}

func parseArgs() *Config {
	config := &Config{}

	// フラグの定義
	flag.StringVar(&config.DBType, "type", "mssql", "database type (mssql)")
	flag.StringVar(&config.Host, "h", "localhost", "hostname")
	flag.IntVar(&config.Port, "p", 1433, "port")
	flag.StringVar(&config.Database, "d", "", "database")
	flag.StringVar(&config.Schema, "s", "dbo", "schema")
	flag.StringVar(&config.User, "u", "", "username")
	flag.StringVar(&config.Password, "P", "", "password")
	flag.StringVar(&config.OutDir, "o", "./", "export dir")

	// パース
	flag.Parse()

	// 必須項目チェック
	if config.Database == "" {
		fmt.Println("エラー: データベース名 (-d) を指定してください")
		os.Exit(1)
	}
	if config.User == "" {
		fmt.Println("エラー: ユーザー名 (-u) を指定してください")
		os.Exit(1)
	}
	if config.Password == "" {
		fmt.Println("エラー: パスワード (-P) を指定してください")
		os.Exit(1)
	}

	return config
}

func main() {
	// オプション解析
	config := parseArgs()

	// 接続文字列の作成
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		config.User, config.Password, config.Host, config.Port, config.Database)

	// データベース接続
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("接続エラー:", err)
	}
	defer db.Close()

	// 接続確認
	err = db.Ping()
	if err != nil {
		log.Fatal("Ping 失敗:", err)
	}
	fmt.Println("SQL Server に接続しました。")

	tables, err := getTables(db, config.Schema)
	if err != nil {
		log.Fatal("getTables Failed", err)
	}

	for _, table := range tables {
		err := exportTableToCSV(db, config.Schema, table, config.OutDir)
		if err != nil {
			log.Printf("テーブル %s のエクスポートに失敗: %v\n", table, err)
		} else {
			fmt.Printf("テーブル %s をエクスポートしました\n", table)
		}
	}
}

func getTables(db *sql.DB, schema string) ([]string, error) {
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
		log.Fatal("クエリ実行エラー:", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var schema, tname string

		err := rows.Scan(&schema, &tname)
		if err != nil {
			log.Fatal("結果取得エラー:", err)
		}
		tables = append(tables, tname)
	}
	return tables, nil
}

func getOutputFilePath(outdir, tableName string) (string, error) {
	// 出力ディレクトリを絶対パスに変換
	absPath, err := filepath.Abs(outdir)
	if err != nil {
		return "", fmt.Errorf("出力ディレクトリのパス取得エラー: %w", err)
	}

	// ディレクトリが存在しない場合は作成
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		if err := os.MkdirAll(absPath, 0755); err != nil {
			return "", fmt.Errorf("出力ディレクトリの作成エラー: %w", err)
		}
	}

	// ファイルのフルパスを組み立て
	filePath := filepath.Join(absPath, fmt.Sprintf("%s.csv", tableName))

	return filePath, nil
}

// テーブルの内容をCSVファイルに書き出す
func exportTableToCSV(db *sql.DB, schema, table string, outdir string) error {
	// クエリを作成
	query := fmt.Sprintf("SELECT * FROM [%s].[%s]", schema, table)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	// カラム情報を取得
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// CSVファイルを作成
	fileName, err := getOutputFilePath(outdir, table)
	if err != nil {
		return err
	}
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// カラム名を書き込む
	if err := writer.Write(columns); err != nil {
		return err
	}

	// 各行のデータを書き込む
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		var record []string
		for _, val := range values {
			if val == nil {
				record = append(record, "NULL")
			} else {
				record = append(record, fmt.Sprintf("%v", val))
			}
		}

		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}
