package common

import (
	"flag"

	_ "github.com/lib/pq"
)

var (
	DBUrl = flag.String("kl_db_url", "host=127.0.0.1 dbname=delayed_test user=delayedtest password=123456 sslmode=disable", "the db url")
	DBDrv = flag.String("kl_db_drv", "postgres", "the db driver")
)
