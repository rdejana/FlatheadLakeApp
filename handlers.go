package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

func serveEmbeddedHTML(w http.ResponseWriter, filename string) {
	data, err := htmlFS.ReadFile("html/" + filename)
	if err != nil {
		http.Error(w, "file not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

func newMux(client *Client, queries chan<- chan<- Reading, boatStore BoatStore) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		serveEmbeddedHTML(w, "index.html")
	})

	mux.HandleFunc("/history", func(w http.ResponseWriter, r *http.Request) {
		serveEmbeddedHTML(w, "history.html")
	})

	mux.HandleFunc("/boat", func(w http.ResponseWriter, r *http.Request) {
		serveEmbeddedHTML(w, "boat.html")
	})

	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		reply := make(chan Reading, 1)
		queries <- reply
		reading := <-reply
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reading)
	})

	mux.HandleFunc("/data/history", func(w http.ResponseWriter, r *http.Request) {
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")

		if start == "" || end == "" {
			http.Error(w, "start and end query parameters are required", http.StatusBadRequest)
			return
		}

		startTime, err := time.Parse(time.RFC3339, start)
		if err != nil {
			http.Error(w, "invalid start time format, expected RFC3339", http.StatusBadRequest)
			return
		}

		endTime, err := time.Parse(time.RFC3339, end)
		if err != nil {
			http.Error(w, "invalid end time format, expected RFC3339", http.StatusBadRequest)
			return
		}

		fc, err := client.GetContinuousHistory(r.Context(), defaultStationID, defaultParameter, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
		if err != nil {
			log.Printf("[history] error fetching USGS data: %v", err)
			http.Error(w, fmt.Sprintf("failed to fetch historical data: %v", err), http.StatusInternalServerError)
			return
		}

		points := make([]HistoricalPoint, 0, len(fc.Features))
		unit := "ft"
		station := defaultStationID

		for _, feat := range fc.Features {
			if feat.Properties.UnitOfMeasure != "" {
				unit = feat.Properties.UnitOfMeasure
			}
			if feat.Properties.MonitoringLocationID != "" {
				station = feat.Properties.MonitoringLocationID
			}
			points = append(points, HistoricalPoint{
				Time:  feat.Properties.Time,
				Value: float64(feat.Properties.Value),
			})
		}

		resp := HistoryResponse{
			Station:  station,
			Unit:     unit,
			FullPool: summerFullPool,
			Points:   points,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/data/point", func(w http.ResponseWriter, r *http.Request) {
		timeParam := r.URL.Query().Get("time")
		if timeParam == "" {
			http.Error(w, "time query parameter is required", http.StatusBadRequest)
			return
		}

		pointTime, err := time.Parse(time.RFC3339, timeParam)
		if err != nil {
			http.Error(w, "invalid time format, expected RFC3339", http.StatusBadRequest)
			return
		}

		fc, err := client.GetPointContinuous(r.Context(), defaultStationID, defaultParameter, pointTime)
		if err != nil {
			log.Printf("[point] error fetching USGS data: %v", err)
			http.Error(w, fmt.Sprintf("failed to fetch data: %v", err), http.StatusInternalServerError)
			return
		}

		if len(fc.Features) == 0 {
			http.Error(w, "no reading found for the specified point in time", http.StatusNotFound)
			return
		}

		p := fc.Features[0].Properties
		value := float64(p.Value)
		resp := Reading{
			Station:    p.MonitoringLocationID,
			Value:      value,
			Unit:       p.UnitOfMeasure,
			FullPool:   summerFullPool,
			Delta:      value - summerFullPool,
			MeasuredAt: p.Time,
			FetchedAt:  time.Now(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/api/boat-logs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			logs := boatStore.GetAll()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(logs)

		case http.MethodPost:
			var req struct {
				Action    string    `json:"action"`
				Rating    string    `json:"rating"`
				LoggedAt  time.Time `json:"logged_at"`
				LakeLevel *float64  `json:"lake_level,omitempty"`
				Notes     string    `json:"notes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request body", http.StatusBadRequest)
				return
			}
			if req.Action != "in" && req.Action != "out" {
				http.Error(w, "action must be 'in' or 'out'", http.StatusBadRequest)
				return
			}
			if req.Rating != "green" && req.Rating != "yellow" && req.Rating != "red" {
				http.Error(w, "rating must be 'green', 'yellow', or 'red'", http.StatusBadRequest)
				return
			}
			if req.LoggedAt.IsZero() {
				req.LoggedAt = time.Now()
			}

			lakeLevel := 0.0
			unit := "ft"

			if req.LakeLevel != nil {
				lakeLevel = *req.LakeLevel
			} else {
				// Query USGS API for the lake level at this timestamp
				fc, err := client.GetPointContinuous(r.Context(), defaultStationID, defaultParameter, req.LoggedAt)
				if err == nil && len(fc.Features) > 0 {
					p := fc.Features[0].Properties
					lakeLevel = float64(p.Value)
					if p.UnitOfMeasure != "" {
						unit = p.UnitOfMeasure
					}
				} else {
					// Fallback to latest reading
					reply := make(chan Reading, 1)
					queries <- reply
					cur := <-reply
					if cur.Value > 0 {
						lakeLevel = cur.Value
						unit = cur.Unit
					}
				}
			}

			entry := BoatLog{
				Action:    req.Action,
				Rating:    req.Rating,
				LakeLevel: lakeLevel,
				Unit:      unit,
				FullPool:  summerFullPool,
				Delta:     lakeLevel - summerFullPool,
				LoggedAt:  req.LoggedAt,
				Notes:     req.Notes,
			}

			created := boatStore.Add(entry)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(created)

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, "id parameter is required", http.StatusBadRequest)
				return
			}
			if !boatStore.Delete(id) {
				http.Error(w, "log not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/boat-logs/export", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		logs := boatStore.GetAll()
		var buf bytes.Buffer
		cw := csv.NewWriter(&buf)

		// Header
		_ = cw.Write([]string{"id", "action", "rating", "lake_level", "unit", "full_pool", "delta", "logged_at", "notes"})

		for _, l := range logs {
			_ = cw.Write([]string{
				l.ID,
				l.Action,
				l.Rating,
				fmt.Sprintf("%.2f", l.LakeLevel),
				l.Unit,
				fmt.Sprintf("%.2f", l.FullPool),
				fmt.Sprintf("%.2f", l.Delta),
				l.LoggedAt.Format(time.RFC3339),
				l.Notes,
			})
		}
		cw.Flush()

		filename := fmt.Sprintf("boat-lift-logs-%s.csv", time.Now().Format("20060102-150405"))
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		w.Write(buf.Bytes())
	})

	mux.HandleFunc("/api/boat-logs/import", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Support both multipart file upload and raw CSV body
		var reader io.Reader = r.Body
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
			if err := r.ParseMultipartForm(10 << 20); err == nil {
				file, _, err := r.FormFile("file")
				if err == nil {
					defer file.Close()
					reader = file
				}
			}
		}

		cr := csv.NewReader(reader)
		cr.FieldsPerRecord = -1
		records, err := cr.ReadAll()
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to parse CSV: %v", err), http.StatusBadRequest)
			return
		}

		if len(records) == 0 {
			http.Error(w, "CSV file is empty", http.StatusBadRequest)
			return
		}

		// Detect header indices for input fields: action, rating, logged_at, notes, id (optional)
		colIdx := map[string]int{
			"id":        -1,
			"action":    -1,
			"rating":    -1,
			"logged_at": -1,
			"date":      -1,
			"time":      -1,
			"datetime":  -1,
			"notes":     -1,
			"note":      -1,
		}

		hasHeader := false
		headerRow := records[0]
		for i, h := range headerRow {
			hClean := strings.ToLower(strings.TrimSpace(h))
			if _, exists := colIdx[hClean]; exists {
				colIdx[hClean] = i
				hasHeader = true
			}
		}

		startRow := 0
		if hasHeader {
			startRow = 1
		} else {
			// default column positions if no named header:
			// action, rating, logged_at, notes
			colIdx["action"] = 0
			colIdx["rating"] = 1
			colIdx["logged_at"] = 2
			colIdx["notes"] = 3
		}

		imported := 0
		for rowIdx := startRow; rowIdx < len(records); rowIdx++ {
			row := records[rowIdx]
			if len(row) == 0 || (len(row) == 1 && strings.TrimSpace(row[0]) == "") {
				continue
			}

			getVal := func(names ...string) string {
				for _, name := range names {
					idx := colIdx[name]
					if idx >= 0 && idx < len(row) {
						v := strings.TrimSpace(row[idx])
						if v != "" {
							return v
						}
					}
				}
				return ""
			}

			action := strings.ToLower(getVal("action"))
			if action != "in" && action != "out" {
				continue // skip invalid records
			}

			rating := strings.ToLower(getVal("rating"))
			if rating != "green" && rating != "yellow" && rating != "red" {
				rating = "green"
			}

			loggedAtStr := getVal("logged_at", "datetime", "date", "time")
			loggedAt := time.Now()
			if loggedAtStr != "" {
				// Try RFC3339, standard date formats
				for _, layout := range []string{
					time.RFC3339,
					"2006-01-02T15:04",
					"2006-01-02 15:04:05",
					"2006-01-02 15:04",
					"2006-01-02",
					"01/02/2006 15:04",
					"01/02/2006",
				} {
					if t, err := time.Parse(layout, loggedAtStr); err == nil {
						loggedAt = t
						break
					}
				}
			}

			// Pool level always retrieved from the USGS API for this point in time
			lakeLevel := 0.0
			unit := "ft"
			fc, err := client.GetPointContinuous(r.Context(), defaultStationID, defaultParameter, loggedAt)
			if err == nil && len(fc.Features) > 0 {
				p := fc.Features[0].Properties
				lakeLevel = float64(p.Value)
				if p.UnitOfMeasure != "" {
					unit = p.UnitOfMeasure
				}
			} else {
				// Fallback to latest reading if point not available
				reply := make(chan Reading, 1)
				queries <- reply
				cur := <-reply
				if cur.Value > 0 {
					lakeLevel = cur.Value
					unit = cur.Unit
				}
			}

			fullPool := summerFullPool
			delta := lakeLevel - fullPool

			notes := getVal("notes", "note")
			id := getVal("id")

			boatStore.Add(BoatLog{
				ID:        id,
				Action:    action,
				Rating:    rating,
				LakeLevel: lakeLevel,
				Unit:      unit,
				FullPool:  fullPool,
				Delta:     delta,
				LoggedAt:  loggedAt,
				Notes:     notes,
			})
			imported++
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"imported": imported,
			"total":    len(boatStore.GetAll()),
		})
	})

	return mux
}
