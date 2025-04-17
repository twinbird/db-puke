# DB-puke

🤮 Puke your database data🤮

## Usage

### MSSQL(SQLServer)

Currently, only SQL Server authentication is supported.

```
DB_PUKE_PASSWORD=[Your DB Password] db-puke -type mssql -h [Your DB Host] -p [Your DB Port] -d [Your DB Name] -s [Your DB Schema] -u [Your DB Username] -o [Export Directory Name]
```

or use -P option

```
db-puke -type mssql -h [Your DB Host] -p [Your DB Port] -d [Your DB Name] -s [Your DB Schema] -u [Your DB Username] -P [Your DB Password] -o [Export Directory Name]
```

#### Command example

```
DB_PUKE_PASSWORD=saPassword1234 ./db-puke -type mssql -h localhost -p 1433 -d dummy_database -s dummy_schema -u sa -o outdir
```

## Data Types and Output Format

The unsupported column types will be output as `[UNSUPPORTED COLUMN TYPE]`.

### MS SQL Server (type: mssql)

| Data Type    | Output Format           |
|--------------|-------------------------|
| `int`        | Number                  |
| `bigint`     | Number                  |
| `smallint`   | Number                  |
| `tinyint`    | Number                  |
| `bit`        | `0` / `1`               |
| `float`      | Number (may be in scientific notation)  |
| `real`       | Number (may be in scientific notation)  |
| `varchar`    | String                  |
| `nvarchar`   | String                  |
| `char`       | String                  |
| `nchar`      | String                  |
| `text`       | String                  |
| `ntext`      | String                  |
| `date`       | `YYYY-MM-DD`            |
| `datetime`   | `YYYY-MM-DD HH:MM:SS.mmm`  |
| `datetime2`  | `YYYY-MM-DD HH:MM:SS.mmmmmmm`  |
| `smalldatetime`  | `YYYY-MM-DD HH:MM:SS`  |
| `decimal`    | Number                  |
| `numeric`    | Number                  |
| `money`      | Number                  |
| `smallmoney`      | Number                  |
| `uniqueidentifier | String (XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX) | 

