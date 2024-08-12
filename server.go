package main

import (
	"encoding/json"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
)

type V1InsertParams struct {
	Key string `json:"key"`
}

func StartServer(config *Config) {
	port := fmt.Sprintf(":%d", config.Server.Port)
	host := config.Raft.TCPAddress
	log.Println(fmt.Sprintf("Starting server on at: %s", host))

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

	response := fmt.Sprintf(`{"key":"%s", "status": "inserted"}`, jsonBody.Key)

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte(response))
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
	response := fmt.Sprintf(`{"key":"%s", "exists": %t, "elapsed": %d}`, key, exists, elapsed)

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte(response))
}
