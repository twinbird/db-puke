package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

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

	// クエリ実行
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

	// 結果の処理
	for rows.Next() {
		var schema, tname string

		err := rows.Scan(&schema, &tname)
		if err != nil {
			log.Fatal("結果取得エラー:", err)
		}
		fmt.Printf("schema: %s, table: %s\n", schema, tname)
	}

	// エラーチェック
	if err = rows.Err(); err != nil {
		log.Fatal("rows エラー:", err)
	}
}
