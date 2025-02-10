#!/bin/bash

# create database
docker compose exec -i mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'saPassword1234' -C -Q "
DROP DATABASE IF EXISTS dummy_database;
CREATE DATABASE dummy_database;
"

# create table
docker compose exec -i mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'saPassword1234' -C -Q "
USE dummy_database;
-- Many column type table
DROP TABLE IF EXISTS dummy_table;
CREATE TABLE dummy_table (
    id INT PRIMARY KEY IDENTITY(1,1),

    tinyint_col TINYINT,
    smallint_col SMALLINT,
    int_col INT,
    bigint_col BIGINT,
    decimal_col DECIMAL(10,2),
    numeric_col NUMERIC(10,2),
    float_col FLOAT,
    real_col REAL,

    char_col CHAR(10),
    varchar_col VARCHAR(50),
    text_col TEXT,

    nchar_col NCHAR(10),
    nvarchar_col NVARCHAR(50),
    ntext_col NTEXT,

    date_col DATE,
    datetime_col DATETIME,
    datetime2_col DATETIME2,
    smalldatetime_col SMALLDATETIME,
    time_col TIME,
    datetimeoffset_col DATETIMEOFFSET,

    binary_col BINARY(16),
    varbinary_col VARBINARY(16),
    image_col IMAGE,

    bit_col BIT,
    uniqueidentifier_col UNIQUEIDENTIFIER
);

-- simple table
DROP TABLE IF EXISTS dummy_table2;
CREATE TABLE dummy_table2 (
    char_col CHAR(10) PRIMARY KEY
);
-- simple table
DROP TABLE IF EXISTS dummy_table3;
CREATE TABLE dummy_table3 (
    char_col CHAR(10) PRIMARY KEY
);
"

# insert dummy data
docker compose exec -i mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U SA -P 'saPassword1234' -C -Q "
USE dummy_database;
INSERT INTO dummy_table (
    tinyint_col, smallint_col, int_col, bigint_col, decimal_col, numeric_col, float_col, real_col,
    char_col, varchar_col, text_col, 
    nchar_col, nvarchar_col, ntext_col, 
    date_col, datetime_col, datetime2_col, smalldatetime_col, time_col, datetimeoffset_col,
    binary_col, varbinary_col, image_col, 
    bit_col, uniqueidentifier_col
) VALUES (
    255, -32768, 2147483647, 9223372036854775807, 1234.56, 7890.12, 3.141592, 2.71828,
    'ABC', 'Hello, World!', 'This is a text column',
    N'あいう', N'こんにちは', N'これはNTextです',
    '2025-02-07', '2025-02-07 12:34:56', '2025-02-07 12:34:56.789', '2025-02-07 12:34:00', '12:34:56', '2025-02-07 12:34:56 +09:00',
    0x0123456789ABCDEF0123456789ABCDEF, 0x0123456789ABCDEF, 0x0123456789ABCDEF0123456789ABCDEF,
    1, NEWID()
)

INSERT INTO dummy_table2 ( char_col ) VALUES ('ROW1');
INSERT INTO dummy_table2 ( char_col ) VALUES ('ROW2');
INSERT INTO dummy_table2 ( char_col ) VALUES ('ROW3');

INSERT INTO dummy_table3 ( char_col ) VALUES ('ROW1');
INSERT INTO dummy_table3 ( char_col ) VALUES ('ROW2');
INSERT INTO dummy_table3 ( char_col ) VALUES ('ROW3');
"
