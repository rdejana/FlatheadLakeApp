package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestEndpoints(t *testing.T) {
	client := NewClient("")
	updates := make(chan Reading, 10)
	queries := make(chan chan<- Reading, 10)

	// mock reading for /data
	go func() {
		for reply := range queries {
			reply <- Reading{
				Station:    "USGS-12371550",
				Value:      2891.5,
				Unit:       "ft",
				FullPool:   2893.0,
				Delta:      -1.5,
				MeasuredAt: time.Now(),
				FetchedAt:  time.Now(),
			}
		}
	}()

	mux := newMux(client, queries)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Test Home page
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("Failed to GET /: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / returned status %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Test History page
	resp, err = http.Get(srv.URL + "/history")
	if err != nil {
		t.Fatalf("Failed to GET /history: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /history returned status %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Test /data
	resp, err = http.Get(srv.URL + "/data")
	if err != nil {
		t.Fatalf("Failed to GET /data: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /data returned status %d", resp.StatusCode)
	}
	var r Reading
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		t.Errorf("Failed to decode /data response: %v", err)
	}
	if r.FullPool != 2893.0 {
		t.Errorf("Expected full pool 2893.0, got %f", r.FullPool)
	}
	resp.Body.Close()

	// Test /data/history parameter validation
	resp, err = http.Get(srv.URL + "/data/history")
	if err != nil {
		t.Fatalf("Failed to GET /data/history without params: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing params, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	_ = updates
}
