# DB-puke

🤮 Puke your database data🤮

## Usage

```
db-puke -type mssql -h localhost -p 1433 -d sample_db -s sample_schema -u sample_user -p user_password -o outdir
```

## Data Types and Output Format

### MS SQL Server (type: mssql)

| Data Type    | Output Format           |
|--------------|-------------------------|
| `int`        | Number                  |
| `smallint`   | Number                  |
| `tinyint`    | Number                  |
| `bit`        | `0` / `1`               |
| `float`      | `X.XXXXXXXXXX`          |
| `real`       | `X.XXXXXXXXXX`          |
| `varchar`    | String                  |
| `char`       | String                  |
| `datetime`   | `YYYY-MM-DD HH:MM:SS.sss` |

