package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/valyala/fasthttp"
	"log"
	"strings"
	"time"
)

type V1InsertParams struct {
	Key string `json:"key"`
}

type V1RemoveParams struct {
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

type V1RemoveResponse struct {
	Key     string `json:"key"`
	Removed bool   `json:"removed"`
}

type V1CountResponse struct {
	Count int `json:"count"`
}

func StartServer(config *Config) {
	addr := fmt.Sprintf("%s:%d", config.Server.Host, config.Server.Port)
	log.Printf("Starting server on: http://%s", addr)

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/":
			homeHandler(ctx)
		case "/v1/insert":
			v1InsertHandler(ctx)
		case "/v1/exists":
			v1ExistsHandler(ctx)
		case "/v1/remove":
			v1RemoveHandler(ctx)
		case "/v1/count":
			v1CountHandler(ctx)
		case "/v1/add_peer":
			v1AddPeerHandler(ctx)
		case "/v1/remove_peer":
			v1RemovePeerHandler(ctx)
		default:
			notFoundHandler(ctx)
		}
	}

	if err := fasthttp.ListenAndServe(addr, requestHandler); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func (rn *RaftNode) BootstrapCluster(peerAddresses []string) error {
	log.Printf("Attempting to bootstrap cluster with peers: %v", peerAddresses)
	cfg := rn.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %v", err)
	}

	log.Printf("Current configuration: %v", cfg.Configuration().Servers)

	// Only bootstrap if this node is not already part of a configuration
	if len(cfg.Configuration().Servers) == 0 {
		servers := make([]raft.Server, 0, len(peerAddresses))
		for _, addr := range peerAddresses {
			serverID := raft.ServerID(strings.Split(addr, ":")[0])
			servers = append(servers, raft.Server{
				ID:       serverID,
				Address:  raft.ServerAddress(addr),
				Suffrage: raft.Voter,
			})
		}

		config := raft.Configuration{Servers: servers}
		log.Printf("Attempting to bootstrap cluster with configuration: %v", config)

		future := rn.raft.BootstrapCluster(config)
		if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
			return fmt.Errorf("failed to bootstrap cluster: %v", err)
		}
	} else {
		log.Printf("Node is already part of a cluster, skipping bootstrap")
	}

	return nil
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
	var jsonBody V1InsertParams

	err := json.Unmarshal(body, &jsonBody)
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

	if !ServerRaftNode.IsLeader() {
		leaderAddr := ServerRaftNode.LeaderAddress()
		ctx.SetStatusCode(fasthttp.StatusTemporaryRedirect)
		ctx.Response.Header.Set("Location", "http://"+leaderAddr+"/v1/insert")
		return
	}

	insertError := ServerRaftNode.Insert(jsonBody.Key)
	if insertError != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBody([]byte(insertError.Error()))
		return
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

	if !ServerRaftNode.IsLeader() {
		leaderAddr := ServerRaftNode.LeaderAddress()
		ctx.SetStatusCode(fasthttp.StatusTemporaryRedirect)
		ctx.Response.Header.Set("Location", "http://"+leaderAddr+"/v1/exists?key="+key)
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

func v1RemoveHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	body := ctx.PostBody()
	var jsonBody V1RemoveParams

	err := json.Unmarshal(body, &jsonBody)
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

	if !ServerRaftNode.IsLeader() {
		leaderAddr := ServerRaftNode.LeaderAddress()
		ctx.SetStatusCode(fasthttp.StatusTemporaryRedirect)
		ctx.Response.Header.Set("Location", "http://"+leaderAddr+"/v1/remove")
		return
	}

	removeError := ServerRaftNode.Remove(jsonBody.Key)
	if removeError != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBody([]byte(removeError.Error()))
		return
	}

	response := V1RemoveResponse{Key: jsonBody.Key, Removed: true}
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

func v1CountHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsGet() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	if !ServerRaftNode.IsLeader() {
		leaderAddr := ServerRaftNode.LeaderAddress()
		ctx.SetStatusCode(fasthttp.StatusTemporaryRedirect)
		ctx.Response.Header.Set("Location", "http://"+leaderAddr+"/v1/count")
		return
	}

	count := QF.Count()
	response := V1CountResponse{Count: count}
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

func v1AddPeerHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	var params struct {
		NodeID string `json:"node_id"`
		Addr   string `json:"addr"`
	}

	if err := json.Unmarshal(ctx.PostBody(), &params); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody([]byte("Invalid JSON"))
		return
	}

	if err := ServerRaftNode.AddPeer(params.NodeID, params.Addr); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("Peer added successfully"))
}

func v1RemovePeerHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	var params struct {
		NodeID string `json:"node_id"`
	}

	if err := json.Unmarshal(ctx.PostBody(), &params); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetBody([]byte("Invalid JSON"))
		return
	}

	if err := ServerRaftNode.RemovePeer(params.NodeID); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("Peer removed successfully"))
}
