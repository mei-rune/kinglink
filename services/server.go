package services

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/runner-mei/kinglink"
)

type Server struct {
	backendProxy jobBackendProxy
	clientProxy  jobClientService
}

func (srv *Server) Close() error {
	return srv.clientProxy.backend.Close()
}

func (srv *Server) ServeHTTP(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ss := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(ss) == 0 {
		notFound(ctx, w, r)
		return
	}
	if ss[0] == "tasks" {
		srv.serveTasks(ctx, w, r, ss[1:])
	} else if ss[0] == "backend" {
		srv.serveBackend(ctx, w, r, ss[1:])
	} else {
		notFound(ctx, w, r)
	}
}

func (srv *Server) ServeBackend(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ss := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	srv.serveBackend(ctx, w, r, ss)
}

func (srv *Server) serveBackend(ctx context.Context, w http.ResponseWriter, r *http.Request, ss []string) {
	switch r.Method {
	case http.MethodGet:
		if len(ss) != 0 {
			notFound(ctx, w, r)
			return
		}

		queryParams := r.URL.Query()
		queues := queryParams["queues"]
		name := queryParams.Get("name")

		job, err := srv.backendProxy.Fetch(ctx, name, queues)
		if err != nil {
			returnError(ctx, w, r, http.StatusInternalServerError, err.Error())
			return
		}
		if job == nil {
			returnNoContent(ctx, w, r, http.StatusNoContent)
			return
		}
		returnOK(ctx, w, r, http.StatusOK, job)
		return
	case http.MethodPut:
		fallthrough
	case http.MethodPost:
		if len(ss) == 0 {
			notFound(ctx, w, r)
			return
		}

		if len(ss) != 2 {
			notFound(ctx, w, r)
			return
		}

		switch ss[1] {
		case "retry":
			var bindArgs struct {
				Attempts int              `json:"attempts,omitempty"`
				NextTime time.Time        `json:"next_time,omitempty"`
				Payload  kinglink.Payload `json:"payload,omitempty"`
				Err      string           `json:"err_message,omitempty"`
			}
			if err := bind(r, &bindArgs); err != nil {
				returnError(ctx, w, r, http.StatusBadRequest, "读参数失败： "+err.Error())
				return
			}

			err := srv.backendProxy.Retry(ctx, ss[0], bindArgs.Attempts, bindArgs.NextTime, &bindArgs.Payload, bindArgs.Err)
			if err != nil {
				returnError(ctx, w, r, http.StatusInternalServerError, "读参数失败： "+err.Error())
				return
			}
			returnOK(ctx, w, r, http.StatusOK, map[string]interface{}{"id": ss[0]})
			return
		case "fail":
			var bindArgs struct {
				Err string `json:"err_message,omitempty"`
			}
			if err := bind(r, &bindArgs); err != nil {
				returnError(ctx, w, r, http.StatusBadRequest, "读参数失败： "+err.Error())
				return
			}

			err := srv.backendProxy.Fail(ctx, ss[0], bindArgs.Err)
			if err != nil {
				returnError(ctx, w, r, http.StatusInternalServerError, "读参数失败： "+err.Error())
				return
			}
			returnOK(ctx, w, r, http.StatusOK, map[string]interface{}{"id": ss[0]})
			return
		case "success":
			err := srv.backendProxy.Success(ctx, ss[0])
			if err != nil {
				returnError(ctx, w, r, http.StatusInternalServerError, err.Error())
				return
			}
			returnOK(ctx, w, r, http.StatusOK, map[string]interface{}{"id": ss[0]})
			return
		default:
			notFound(ctx, w, r)
			return
		}

	// case http.MethodDelete:
	// 	if len(ss) != 1 {
	// 		notFound(ctx, w, r)
	// 		return
	// 	}
	// 	err := srv.backendProxy.Destroy(ctx, ss[0])
	// 	if err != nil {
	// 		returnError(ctx, w, r, http.StatusInternalServerError, err.Error())
	// 		return
	// 	}
	// 	returnOK(ctx, w, r, http.StatusOK, map[string]interface{}{"id": ss[0]})
	// 	return
	default:
		returnError(ctx, w, r, http.StatusMethodNotAllowed, "Method "+r.Method+" Not Allowed")
		return
	}
}

func (srv *Server) ServeTasks(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	ss := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	srv.serveTasks(ctx, w, r, ss)
}

func (srv *Server) serveTasks(ctx context.Context, w http.ResponseWriter, r *http.Request, ss []string) {
	switch r.Method {
	case http.MethodGet:
		if len(ss) == 1 {
			results, err := srv.clientProxy.Get(ctx, ss[0])
			if err != nil {
				returnError(ctx, w, r, http.StatusInternalServerError, err.Error())
				return
			}
			returnOK(ctx, w, r, http.StatusOK, results)
			return
		}

		if len(ss) == 0 {
			queryParams := r.URL.Query()
			queues := queryParams["queues"]

			var limit, offset int
			if s := queryParams.Get("limit"); s != "" {
				i, err := strconv.Atoi(s)
				if err != nil {
					returnError(ctx, w, r, http.StatusBadRequest, "参数 'limit' 不是一个整型数字: "+s)
					return
				}
				limit = i
			}
			if s := queryParams.Get("offset"); s != "" {
				i, err := strconv.Atoi(s)
				if err != nil {
					returnError(ctx, w, r, http.StatusBadRequest, "参数 'offset' 不是一个整型数字: "+s)
					return
				}
				offset = i
			}

			results, err := srv.clientProxy.List(ctx, queues, limit, offset)
			if err != nil {
				returnError(ctx, w, r, http.StatusInternalServerError, err.Error())
				return
			}
			returnOK(ctx, w, r, http.StatusOK, results)
			return
		}
		notFound(ctx, w, r)
	case http.MethodPut:
		fallthrough
	case http.MethodPost:
		if len(ss) == 0 {
			var bindArgs struct {
				Type     string                 `json:"type,omitempty"`
				TypeName string                 `json:"type_name,omitempty"`
				Args     map[string]interface{} `json:"args,omitempty"`
				Options  *Options               `json:"options,omitempty"`
			}
			if bindArgs.TypeName == "" {
				bindArgs.TypeName = bindArgs.Type
			}
			if err := bind(r, &bindArgs); err != nil {
				returnError(ctx, w, r, http.StatusBadRequest, "读参数失败： "+err.Error())
				return
			}

			id, err := srv.clientProxy.Create(ctx, bindArgs.TypeName, bindArgs.Args, bindArgs.Options)
			if err != nil {
				returnError(ctx, w, r, http.StatusInternalServerError, "读参数失败： "+err.Error())
				return
			}
			returnText(ctx, w, r, http.StatusCreated, id)
			return
		}

		if len(ss) == 1 && ss[0] == "batch" {
			var requests []BatchRequest
			if err := bind(r, &requests); err != nil {
				returnError(ctx, w, r, http.StatusBadRequest, "读参数失败： "+err.Error())
				return
			}
			result, err := srv.clientProxy.BatchCreate(ctx, requests)
			if err != nil {
				returnError(ctx, w, r, http.StatusInternalServerError, "读参数失败： "+err.Error())
				return
			}
			returnOK(ctx, w, r, http.StatusCreated, result)
			return
		}
	case http.MethodDelete:
		if len(ss) != 1 {
			notFound(ctx, w, r)
			return
		}
		err := srv.clientProxy.Delete(ctx, ss[0])
		if err != nil {
			returnError(ctx, w, r, http.StatusInternalServerError, err.Error())
			return
		}
		returnOK(ctx, w, r, http.StatusOK, map[string]interface{}{"id": ss[0]})
	default:
		returnError(ctx, w, r, http.StatusMethodNotAllowed, "Method "+r.Method+" Not Allowed")
	}
}

func bind(r *http.Request, value interface{}) error {
	return json.NewDecoder(r.Body).Decode(value)
}

func returnError(ctx context.Context, w http.ResponseWriter, r *http.Request, statusCode int, message string) {
	w.WriteHeader(statusCode)
	w.Write([]byte(message))
}

func returnOK(ctx context.Context, w http.ResponseWriter, r *http.Request, statusCode int, value interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(value)
}

func returnText(ctx context.Context, w http.ResponseWriter, r *http.Request, statusCode int, value string) {
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.WriteHeader(statusCode)
	io.WriteString(w, value)
}

func returnNoContent(ctx context.Context, w http.ResponseWriter, r *http.Request, statusCode int) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(statusCode)
}

func notFound(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	http.NotFound(w, r)
}

func NewServer(dbopts *kinglink.DbOptions, opts *kinglink.WorkOptions) (*Server, error) {
	backend, err := kinglink.NewBackend(dbopts, opts)
	if err != nil {
		return nil, err
	}
	return &Server{
		backendProxy: jobBackendProxy{backend: backend},
		clientProxy:  jobClientService{backend: backend},
	}, nil
}
