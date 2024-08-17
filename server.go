package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/valyala/fasthttp"
	"io/ioutil"
	"log"
	"net/http"
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
	Key     string `json:"key"`
	Exists  bool   `json:"exists"`
	Elapsed string `json:"elapsed"`
}

type V1RemoveResponse struct {
	Key     string `json:"key"`
	Removed bool   `json:"removed"`
}

type V1CountResponse struct {
	Count int `json:"count"`
}

type V1InfoResponse struct {
	IsLeader bool   `json:"is_leader"`
	NodeID   string `json:"node_id"`
	QFSize   int    `json:"qf_size"`
}

const (
	maxRetries     = 3
	retryDelay     = 500 * time.Millisecond
	dialTimeout    = 10 * time.Second
	requestTimeout = 15 * time.Second
)

func StartServer(config *Config) {
	addr := fmt.Sprintf("%s:%d", "0.0.0.0", config.Server.Port)
	log.Printf("Starting server on: http://%s", addr)

	requestHandler := func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/":
			homeHandler(ctx)
		case "/health":
			healthHandler(ctx)
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
		case "/v1/info":
			v1InfoHandler(ctx)
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
	start := time.Now()
	log.Printf("Received insert request")

	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	body := ctx.PostBody()
	var jsonBody V1InsertParams

	err := json.Unmarshal(body, &jsonBody)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte(err.Error()))
		return
	}

	if jsonBody.Key == "" {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Key is required"))
		return
	}

	log.Printf("Attempting to insert key: %s", jsonBody.Key)

	// Check if this node is the leader
	if !ServerRaftNode.IsLeader() {
		leaderAddr := ServerRaftNode.LeaderAddress()
		log.Printf("This node is not the leader. Leader address: %s", leaderAddr)
		if leaderAddr == "" {
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			ctx.SetContentType("text/plain; charset=utf-8")
			ctx.SetBody([]byte("No leader available"))
			return
		}

		log.Printf("Forwarding insert request to leader at %s", leaderAddr)

		// Attempt to forward the request to the leader with retries
		var respBody []byte
		var respStatus int
		var err error
		for i := 0; i < maxRetries; i++ {
			respBody, respStatus, err = forwardV1InsertRequestToLeader(leaderAddr, body)
			if err == nil {
				break
			}
			log.Printf("Attempt %d: Error forwarding request to leader: %v", i+1, err)
			time.Sleep(retryDelay)
		}

		if err != nil {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			ctx.SetContentType("text/plain; charset=utf-8")
			errorMsg := fmt.Sprintf("Failed to forward request to leader after %d attempts. Last error: %v", maxRetries, err)
			if len(respBody) > 0 {
				errorMsg += fmt.Sprintf("\nPartial response received: %s", string(respBody))
			}
			ctx.SetBody([]byte(errorMsg))
			return
		}

		log.Printf("Successfully forwarded request to leader. Status: %d, Body: %s", respStatus, string(respBody))

		// Return the leader's response
		ctx.SetStatusCode(respStatus)
		ctx.SetBody(respBody)
		return
	}

	log.Printf("This node is the leader. Proceeding with insert operation.")

	insertError := ServerRaftNode.Insert(jsonBody.Key)
	if insertError != nil {
		log.Printf("Error inserting key: %v", insertError)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte(insertError.Error()))
		return
	}

	response := V1InsertResponse{Key: jsonBody.Key, Status: "inserted"}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBody(responseJSON)

	log.Printf("Insert operation completed successfully. Key: %s, Elapsed time: %v", jsonBody.Key, time.Since(start))
}

func v1ExistsHandler(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	log.Printf("Received exists request")

	if !ctx.IsGet() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	key := string(ctx.QueryArgs().Peek("key"))
	if key == "" {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Key is required"))
		return
	}

	log.Printf("Checking existence of key: %s", key)

	exists, elapsed := QF.Exists([]byte(key))
	elapsedMillis := float64(elapsed) / float64(time.Microsecond)
	elapsedFormatted := fmt.Sprintf("%.2fµs", elapsedMillis)

	log.Printf("Key %s exists: %v, Lookup time: %s", key, exists, elapsedFormatted)

	response := V1ExistsResponse{Key: key, Exists: exists, Elapsed: elapsedFormatted}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBody(responseJSON)

	log.Printf("Exists operation completed. Key: %s, Exists: %v, Total elapsed time: %v", key, exists, time.Since(start))
}

func v1RemoveHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	body := ctx.PostBody()
	var jsonBody V1RemoveParams

	err := json.Unmarshal(body, &jsonBody)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte(err.Error()))
		return
	}

	if jsonBody.Key == "" {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Key is required"))
		return
	}

	removeError := ServerRaftNode.Remove(jsonBody.Key)
	if removeError != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("text/plain; charset=utf-8")
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
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	count := QF.Count()
	response := V1CountResponse{Count: count}
	responseJSON, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("text/plain; charset=utf-8")
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
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	var params struct {
		NodeID string `json:"node_id"`
		Addr   string `json:"addr"`
	}

	if err := json.Unmarshal(ctx.PostBody(), &params); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Invalid JSON"))
		return
	}

	if err := ServerRaftNode.AddPeer(params.NodeID, params.Addr); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("Peer added successfully"))
}

func v1RemovePeerHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	var params struct {
		NodeID string `json:"node_id"`
	}

	if err := json.Unmarshal(ctx.PostBody(), &params); err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Invalid JSON"))
		return
	}

	if err := ServerRaftNode.RemovePeer(params.NodeID); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte(err.Error()))
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBody([]byte("Peer removed successfully"))
}

func v1InfoHandler(ctx *fasthttp.RequestCtx) {
	if !ctx.IsGet() {
		ctx.SetStatusCode(fasthttp.StatusMethodNotAllowed)
		ctx.SetContentType("text/plain; charset=utf-8")
		ctx.SetBody([]byte("Method not allowed"))
		return
	}

	isLeader := ServerRaftNode.IsLeader()
	qfSize := 2 ^ Configuration.Quotient.LogSize

	response := V1InfoResponse{
		IsLeader: isLeader,
		QFSize:   int(qfSize),
		NodeID:   Configuration.Raft.NodeID,
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling response: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	ctx.SetBody(jsonResponse)

}

func healthHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/plain; charset=utf-8")
	ctx.SetBodyString("OK")
}

func forwardV1InsertRequestToLeader(leaderAddr string, body []byte) ([]byte, int, error) {
	client := &http.Client{
		Timeout: requestTimeout,
	}

	leaderURL := fmt.Sprintf("http://%s/v1/insert", leaderAddr)
	log.Printf("Forwarding to: %s", leaderURL)

	req, err := http.NewRequest("POST", leaderURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, 0, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("error sending request to leader: %v", err)
	}
	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("error reading response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return respBody, resp.StatusCode, fmt.Errorf("non-OK HTTP status: %v", resp.StatusCode)
	}

	return respBody, resp.StatusCode, nil
}
