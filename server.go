package main

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"time"
)

type V1InsertParams struct {
	Key string `json:"key"`
}

type V1InsertResponse struct {
	Key    string `json:"key"`
	Status string `json:"status"`
}

type V1ExistsResponse struct {
	Key     string        `json:"key"`
	Exists  bool          `json:"exists"`
	Elapsed time.Duration `json:"elapsed"`
}

func StartServer(config *Config) {
	port := fmt.Sprintf(":%d", config.Server.Port)
	host := config.Server.Host
	log.Println(fmt.Sprintf("Starting server on at: http://%s%s", host, port))

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/":
			homeHandler(ctx)
		case "/v1/insert":
			v1InsertHandler(ctx)
		case "/v1/exists":
			v1ExistsHandler(ctx)
		default:
			notFoundHandler(ctx)
		}
	}

	if err := fasthttp.ListenAndServe(port, requestHandler); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func homeHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("Quotient is up and running"))
}

func notFoundHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusNotFound)
	ctx.SetBody([]byte("Not found"))
}

func v1InsertHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	body := ctx.PostBody()
	bodyString := []byte(string(body))
	var jsonBody V1InsertParams

	err := json.Unmarshal(bodyString, &jsonBody)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody([]byte(err.Error()))
		return
	}

	if jsonBody.Key == "" {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody([]byte("Key is required"))
		return
	}

	insertError := QF.Insert([]byte(jsonBody.Key))
	if insertError != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBody([]byte(insertError.Error()))
	}

	response := V1InsertResponse{Key: jsonBody.Key, Status: "inserted"}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBody(responseJSON)
}

func v1ExistsHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsGet() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	key := string(ctx.QueryArgs().Peek("key"))
	if key == "" {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody([]byte("Key is required"))
		return
	}

	exists, elapsed := QF.Exists([]byte(key))
	response := V1ExistsResponse{Key: key, Exists: exists, Elapsed: elapsed}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBody(responseJSON)
}
