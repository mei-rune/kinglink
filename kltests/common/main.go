package common

import (
	"flag"
	"os"

	_ "github.com/lib/pq"
)

var (

	PostgreSQLUrl = "host=127.0.0.1 user=golang password=123456 dbname=golang sslmode=disable"
	MySQLUrl      = "golang:123456@tcp(localhost:3306)/golang?autocommit=true&parseTime=true&multiStatements=true"
	MsSqlUrl      = "sqlserver://golang:123456@127.0.0.1?database=golang&connection+timeout=30"
	DMSqlUrl      = "dm://" + os.Getenv("dm_username") + ":" + os.Getenv("dm_password") + "@" + os.Getenv("dm_host")+"?noConvertToHex=true"


	DBUrl = flag.String("kl_db_url", "", "the db url")
	DBDrv = flag.String("kl_db_drv", "postgres", "the db driver")
)


func GetTestConnURL() string {
	if *DBUrl == "" {
		switch *DBDrv {
		case "postgres", "":
			return PostgreSQLUrl
		case "mysql":
			return MySQLUrl
		case "sqlserver", "mssql":
			return MsSqlUrl
		case "dm":
			return DMSqlUrl
		}
	}

	return *DBUrl
}

