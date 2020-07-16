package tests

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/runner-mei/kinglink"
	klclient "github.com/runner-mei/kinglink/services"
	"github.com/runner-mei/kinglink/tests/common"
	"github.com/runner-mei/resty"
)

var (
	DBUrl = common.DBUrl
	DBDrv = common.DBDrv
)

func MakeOpts() *kinglink.DbOptions {
	return &kinglink.DbOptions{
		DbDrv: *DBDrv,
		DbURL: *DBUrl,
	}
}

type TServer struct {
	Ctx           context.Context
	Conn          *sql.DB
	DbOpts        *kinglink.DbOptions
	Wopts         *kinglink.WorkOptions
	Srv           *klclient.Server
	Hsrv          *httptest.Server
	RemoteClient  klclient.Client
	RemoteBackend kinglink.WorkBackend
}

func ServerTest(t *testing.T, dbopts *kinglink.DbOptions, wopts *kinglink.WorkOptions, cb func(srv *TServer)) {
	if dbopts == nil {
		dbopts = MakeOpts()
	}
	if wopts == nil {
		wopts = &kinglink.WorkOptions{}
	}

	if dbopts.Conn == nil {
		conn, err := sql.Open(dbopts.DbDrv, dbopts.DbURL)
		if err != nil {
			t.Error(err)
			return
		}
		dbopts.Conn = conn
	}
	srv, err := klclient.NewServer(dbopts, wopts)
	if err != nil {
		t.Error(err)
		return
	}
	hsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.ServeHTTP(r.Context(), w, r)
	}))
	defer hsrv.Close()

	tsrv := &TServer{
		Ctx:    context.Background(),
		DbOpts: dbopts,
		Wopts:  wopts,
		Srv:    srv,
		Hsrv:   hsrv,
		Conn:   dbopts.Conn,
	}

	tsrv.RemoteClient = &klclient.ClientClient{
		Proxy: resty.Must(resty.New(hsrv.URL + "/tasks")),
	}
	tsrv.RemoteBackend = &kinglink.WorkBackendClient{
		Proxy: resty.Must(resty.New(hsrv.URL + "/backend")),
	}

	for _, tablename := range []string{
		dbopts.RunningTablename,
		dbopts.ResultTablename,
	} {
		_, err = tsrv.Conn.Exec("DELETE FROM " + tablename)
		if err != nil {
			t.Error(err)
			return
		}
	}
	cb(tsrv)
}

func AssetTime(t *testing.T, field string, actual, excepted time.Time) {
	t.Helper()

	interval := actual.Sub(excepted)
	if interval < 0 {
		interval = -interval
	}

	if interval > time.Second {
		t.Error(field+": want ", excepted, "got", actual, "interval is", interval)
	}
}
