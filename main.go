package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	_ "github.com/microsoft/go-mssqldb"
)

func main() {
	// SQL Server の接続情報
	server := "localhost"
	port := 1433
	user := "SA"
	password := "saPassword1234"
	database := "dummy_database"
	schema := "dummy_schema"

	// 接続文字列の作成
	connString := fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s&encrypt=disable",
		user, password, server, port, database)

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

	tables, err := getTables(db, schema)
	if err != nil {
		log.Fatal("getTables Failed", err)
	}

	for _, table := range tables {
		err := exportTableToCSV(db, schema, table)
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

// テーブルの内容をCSVファイルに書き出す
func exportTableToCSV(db *sql.DB, schema, table string) error {
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
	fileName := fmt.Sprintf("%s.csv", table)
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
