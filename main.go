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
	query := "SELECT id, varchar_col FROM dummy_table"
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		log.Fatal("クエリ実行エラー:", err)
	}
	defer rows.Close()

	// 結果の処理
	for rows.Next() {
		var id int
		var varcharCol string

		err := rows.Scan(&id, &varcharCol)
		if err != nil {
			log.Fatal("結果取得エラー:", err)
		}
		fmt.Printf("ID: %d, varchar_col: %s\n", id, varcharCol)
	}

	// エラーチェック
	if err = rows.Err(); err != nil {
		log.Fatal("rows エラー:", err)
	}
}
