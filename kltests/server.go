package kltests

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	_ "gitee.com/opengauss/openGauss-connector-go-pq" // openGauss
	_ "gitee.com/runner.mei/dm"                       // 达梦
	_ "gitee.com/runner.mei/gokb"                     // 人大金仓
	_ "github.com/lib/pq"

	"github.com/runner-mei/kinglink"
	"github.com/runner-mei/kinglink/kltests/common"
)

// var (
// 	DBUrl = common.DBUrl
// 	DBDrv = common.DBDrv
// )

func MakeOpts() *kinglink.DbOptions {
	return &kinglink.DbOptions{
		DbDrv: *common.DBDrv,
		DbURL: common.GetTestConnURL(),
	}
}

type TServer struct {
	Ctx           context.Context
	Conn          *sql.DB
	DbOpts        *kinglink.DbOptions
	Wopts         *kinglink.WorkOptions
	Srv           *kinglink.Server
	Hsrv          *httptest.Server
	RemoteClient  kinglink.Client
	RemoteBackend kinglink.WorkBackend
}

func ServerTest(t testing.TB, dbopts *kinglink.DbOptions, wopts *kinglink.WorkOptions, interceptor kinglink.InterceptorFunc, cb func(srv *TServer)) {
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

	common.SetAssertInterval(t, dbopts.Conn)

	srv, err := kinglink.NewServer(dbopts, wopts, interceptor)
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

	tsrv.RemoteClient, err = kinglink.NewRemoteClient(hsrv.URL + "/tasks")
	if err != nil {
		t.Error(err)
		return
	}
	tsrv.RemoteBackend, err = kinglink.NewRemoteWorkBackend(hsrv.URL + "/backend")
	if err != nil {
		t.Error(err)
		return
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

	common.AssetTime(t, field, actual, excepted)
}
