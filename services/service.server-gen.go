// Please don't edit this file!
package tasks

import (
	"net/http"

	"github.com/runner-mei/loong"
)

// BatchResult is skipped
// BatchRequest is skipped

func InitClientService(mux loong.Party, svc ClientService) {
	mux.POST("", func(ctx *loong.Context) error {
		var bindArgs struct {
			TypeName string                 `json:"type_name,omitempty"`
			Args     map[string]interface{} `json:"args,omitempty"`
			Options  *Options               `json:"options,omitempty"`
		}
		if err := ctx.Bind(&bindArgs); err != nil {
			return ctx.ReturnError(loong.ErrBadArgument("bindArgs", "body", err), http.StatusBadRequest)
		}

		result, err := svc.Create(ctx.StdContext, bindArgs.TypeName, bindArgs.Args, bindArgs.Options)
		if err != nil {
			return ctx.ReturnError(err)
		}
		return ctx.ReturnCreatedResult(result)
	})
	mux.POST("/batch", func(ctx *loong.Context) error {
		var requests []BatchRequest
		if err := ctx.Bind(&requests); err != nil {
			return ctx.ReturnError(loong.ErrBadArgument("requests", "body", err), http.StatusBadRequest)
		}

		result, err := svc.BatchCreate(ctx.StdContext, requests)
		if err != nil {
			return ctx.ReturnError(err)
		}
		return ctx.ReturnCreatedResult(result)
	})
	mux.GET("", func(ctx *loong.Context) error {
		result, err := svc.List(ctx.StdContext)
		if err != nil {
			return ctx.ReturnError(err)
		}
		return ctx.ReturnQueryResult(result)
	})
	mux.GET("/:id", func(ctx *loong.Context) error {
		var id = ctx.Param("id")

		result, err := svc.Get(ctx.StdContext, id)
		if err != nil {
			return ctx.ReturnError(err)
		}
		return ctx.ReturnQueryResult(result)
	})
	mux.DELETE("/:id", func(ctx *loong.Context) error {
		var id = ctx.Param("id")

		err := svc.Delete(ctx.StdContext, id)
		if err != nil {
			return ctx.ReturnError(err)
		}
		return ctx.ReturnDeletedResult("OK")
	})
}
