# DB-puke

🤮 Puke your database data🤮

## Usage

```
db-puke -type mssql -h localhost -p 1433 -d sample_db -s sample_schema -u sample_user -p user_password -o outdir
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
| `uniqueidentifier | String (XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX) | 

