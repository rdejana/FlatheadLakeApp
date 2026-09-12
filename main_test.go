package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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

	boatStore, err := NewSQLiteBoatStore(":memory:")
	if err != nil {
		t.Fatalf("Failed to create in-memory SQLite store: %v", err)
	}
	defer boatStore.Close()

	mux := newMux(client, queries, boatStore)
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

	// Test Boat page
	resp, err = http.Get(srv.URL + "/boat")
	if err != nil {
		t.Fatalf("Failed to GET /boat: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /boat returned status %d", resp.StatusCode)
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

	// Test /data/point parameter validation
	resp, err = http.Get(srv.URL + "/data/point")
	if err != nil {
		t.Fatalf("Failed to GET /data/point without params: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing time param, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Test /data/point with invalid time format
	resp, err = http.Get(srv.URL + "/data/point?time=invalid")
	if err != nil {
		t.Fatalf("Failed to GET /data/point with invalid time: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 for invalid time param, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Test /api/boat-logs GET empty
	resp, err = http.Get(srv.URL + "/api/boat-logs")
	if err != nil {
		t.Fatalf("Failed to GET /api/boat-logs: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for GET /api/boat-logs, got %d", resp.StatusCode)
	}
	var logs []BoatLog
	if err := json.NewDecoder(resp.Body).Decode(&logs); err != nil {
		t.Errorf("Failed to decode boat logs: %v", err)
	}
	if len(logs) != 0 {
		t.Errorf("Expected 0 logs initially, got %d", len(logs))
	}
	resp.Body.Close()

	// Test /api/boat-logs POST
	logPayload := `{"action":"in","rating":"green","logged_at":"2024-06-01T14:00:00Z","lake_level":2891.75,"notes":"Smooth launch"}`
	resp, err = http.Post(srv.URL+"/api/boat-logs", "application/json", strings.NewReader(logPayload))
	if err != nil {
		t.Fatalf("Failed to POST /api/boat-logs: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("Expected 201 for POST /api/boat-logs, got %d", resp.StatusCode)
	}
	var created BoatLog
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Errorf("Failed to decode created boat log: %v", err)
	}
	if created.Action != "in" || created.Rating != "green" || created.LakeLevel != 2891.75 {
		t.Errorf("Unexpected created boat log content: %+v", created)
	}
	resp.Body.Close()

	// Test /api/boat-logs/export
	resp, err = http.Get(srv.URL + "/api/boat-logs/export")
	if err != nil {
		t.Fatalf("Failed to GET /api/boat-logs/export: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for GET /api/boat-logs/export, got %d", resp.StatusCode)
	}
	if !strings.Contains(resp.Header.Get("Content-Type"), "text/csv") {
		t.Errorf("Expected text/csv Content-Type, got %s", resp.Header.Get("Content-Type"))
	}
	resp.Body.Close()

	// Test /api/boat-logs/import with raw CSV
	csvData := `action,rating,logged_at,notes
out,yellow,2024-09-15T10:00:00Z,Tight clearance
in,red,2024-05-15T12:00:00Z,Scraped bunks`
	resp, err = http.Post(srv.URL+"/api/boat-logs/import", "text/csv", strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("Failed to POST /api/boat-logs/import: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for POST /api/boat-logs/import, got %d", resp.StatusCode)
	}
	var importRes map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&importRes); err != nil {
		t.Errorf("Failed to decode import response: %v", err)
	}
	if imported, ok := importRes["imported"].(float64); !ok || imported != 2 {
		t.Errorf("Expected 2 imported records, got %v", importRes["imported"])
	}
	resp.Body.Close()

	// Test /api/boat-logs DELETE
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/api/boat-logs?id="+created.ID, nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to DELETE /api/boat-logs: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("Expected 204 for DELETE /api/boat-logs, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	_ = updates
}
