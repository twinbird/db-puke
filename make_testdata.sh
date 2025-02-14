#!/bin/bash

# dummy_table
docker compose exec -i mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'saPassword1234' -s "," -W -o "/tmp/expect_dummy_table.csv" -C -Q "
USE DUMMY_DATABASE;
SET NOCOUNT ON;
SELECT * FROM dummy_schema.dummy_table;
"
docker compose cp mssql:/tmp/expect_dummy_table.csv testdata/expect_dummy_table.csv
sed -i '1d' testdata/expect_dummy_table.csv
sed -i '2d' testdata/expect_dummy_table.csv


# dummy_table2
docker compose exec -i mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'saPassword1234' -s "," -W -o "/tmp/expect_dummy_table2.csv" -C -Q "
USE DUMMY_DATABASE;
SET NOCOUNT ON;
SELECT * FROM dummy_schema.dummy_table2;
"
docker compose cp mssql:/tmp/expect_dummy_table2.csv testdata/expect_dummy_table2.csv
sed -i '1d' testdata/expect_dummy_table2.csv
sed -i '2d' testdata/expect_dummy_table2.csv

# 日本語のテーブル
docker compose exec -i mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'saPassword1234' -s "," -W -o "/tmp/expect_日本語のテーブル.csv" -C -Q "
USE DUMMY_DATABASE;
SET NOCOUNT ON;
SELECT * FROM dummy_schema.日本語のテーブル;
"
docker compose cp mssql:/tmp/expect_日本語のテーブル.csv testdata/expect_日本語のテーブル.csv
sed -i '1d' testdata/expect_日本語のテーブル.csv
sed -i '2d' testdata/expect_日本語のテーブル.csv
