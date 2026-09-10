package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const baseURL = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/latest-continuous/items"

// ---- USGS API types --------------------------------------------------------

type Float64Value float64

func (f *Float64Value) UnmarshalJSON(data []byte) error {
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		*f = Float64Value(n)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}

	*f = Float64Value(v)
	return nil
}

type FeatureCollection struct {
	Type           string    `json:"type"`
	Features       []Feature `json:"features"`
	NumberReturned int       `json:"numberReturned"`
	Links          []Link    `json:"links"`
	TimeStamp      time.Time `json:"timeStamp"`
}

type Feature struct {
	Type       string                `json:"type"`
	ID         string                `json:"id"`
	Properties MeasurementProperties `json:"properties"`
	Geometry   Geometry              `json:"geometry"`
}

type MeasurementProperties struct {
	TimeSeriesID         string       `json:"time_series_id"`
	MonitoringLocationID string       `json:"monitoring_location_id"`
	ParameterCode        string       `json:"parameter_code"`
	StatisticID          string       `json:"statistic_id"`
	Time                 time.Time    `json:"time"`
	Value                Float64Value `json:"value"`
	UnitOfMeasure        string       `json:"unit_of_measure"`
	ApprovalStatus       string       `json:"approval_status"`
	Qualifier            *string      `json:"qualifier"`
	LastModified         time.Time    `json:"last_modified"`
}

type Geometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type Link struct {
	Type  string `json:"type"`
	Rel   string `json:"rel"`
	Title string `json:"title"`
	Href  string `json:"href"`
}

// ---- Client ----------------------------------------------------------------

type Client struct {
	apiKey string
	client *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) GetLatestContinuous(ctx context.Context, locationID, parameterCode string) (*FeatureCollection, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}

	var result FeatureCollection
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ---- Domain ----------------------------------------------------------------

// Reading holds the latest pool level measurement.
type Reading struct {
	Station    string    `json:"station"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	MeasuredAt time.Time `json:"measured_at"`
	FetchedAt  time.Time `json:"fetched_at"`
}

// ---- State goroutine -------------------------------------------------------

// stateLoop owns the latest Reading. Other goroutines communicate with it
// via channels only — no shared state, no mutexes.
func stateLoop(updates <-chan Reading, queries <-chan chan<- Reading) {
	var latest Reading
	for {
		select {
		case r := <-updates:
			latest = r
			log.Printf("[state] updated: %.2f %s at %s", r.Value, r.Unit, r.MeasuredAt.Format(time.RFC3339))
		case reply := <-queries:
			reply <- latest
		}
	}
}

// ---- Fetch goroutine -------------------------------------------------------

func fetchLoop(client *Client, updates chan<- Reading, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	fetch := make(chan struct{}, 1)
	fetch <- struct{}{} // immediate fetch on startup

	doFetch := func() {
		fc, err := client.GetLatestContinuous(context.Background(), "USGS-12371550", "00062")
		if err != nil {
			log.Printf("[fetch] error: %v", err)
			return
		}
		if len(fc.Features) == 0 {
			log.Printf("[fetch] no features returned")
			return
		}
		p := fc.Features[0].Properties
		updates <- Reading{
			Station:    p.MonitoringLocationID,
			Value:      float64(p.Value),
			Unit:       p.UnitOfMeasure,
			MeasuredAt: p.Time,
			FetchedAt:  time.Now(),
		}
	}

	for {
		select {
		case <-done:
			return
		case <-fetch:
			doFetch()
		case <-ticker.C:
			doFetch()
		}
	}
}

// ---- HTTP handlers ---------------------------------------------------------

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Flathead Lake Level</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: Georgia, "Times New Roman", serif;
      min-height: 100vh;
      background: #0a1f2e;
      color: #e8f4f8;
      display: flex;
      flex-direction: column;
    }

    /* ---------- sky + mountains + water scene ---------- */
    .scene {
      position: relative;
      width: 100%;
      height: 260px;
      overflow: hidden;
      flex-shrink: 0;
    }

    /* Sky gradient — deep Montana blue at top, warm horizon near dusk */
    .sky {
      position: absolute; inset: 0;
      background: linear-gradient(to bottom,
        #0d2a4a 0%,
        #1a4a7a 40%,
        #2e6da4 70%,
        #5fa8c8 100%);
    }

    /* Sun glow on the horizon */
    .sun-glow {
      position: absolute;
      bottom: 90px; left: 50%;
      transform: translateX(-50%);
      width: 340px; height: 100px;
      background: radial-gradient(ellipse at center,
        rgba(255,210,120,0.35) 0%,
        rgba(255,160,60,0.12) 55%,
        transparent 80%);
    }

    /* Mission Mountain silhouette — SVG inline */
    .mountains {
      position: absolute;
      bottom: 70px; left: 0; right: 0;
    }

    /* Water reflection */
    .water {
      position: absolute;
      bottom: 0; left: 0; right: 0;
      height: 80px;
      background: linear-gradient(to bottom,
        #1e6fa8 0%,
        #155a8a 50%,
        #0e3d5e 100%);
    }

    /* ripple lines */
    .water::after {
      content: "";
      position: absolute;
      inset: 0;
      background: repeating-linear-gradient(
        to bottom,
        transparent 0px,
        transparent 10px,
        rgba(255,255,255,0.04) 10px,
        rgba(255,255,255,0.04) 11px
      );
    }

    /* ---------- page content ---------- */
    .page {
      flex: 1;
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 2rem 1rem 3rem;
      background: linear-gradient(to bottom, #0e3d5e, #0a2233);
    }

    .headline {
      text-align: center;
      margin-bottom: 2rem;
    }
    .headline h1 {
      font-size: 2rem;
      font-weight: normal;
      letter-spacing: 0.04em;
      color: #c8e8f5;
      text-shadow: 0 1px 8px rgba(0,0,0,0.6);
    }
    .headline .tagline {
      margin-top: 0.35rem;
      font-size: 0.82rem;
      font-family: -apple-system, "Segoe UI", system-ui, sans-serif;
      color: #7aaccc;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }

    /* ---------- gauge card ---------- */
    .card {
      background: rgba(255,255,255,0.06);
      border: 1px solid rgba(255,255,255,0.12);
      border-radius: 12px;
      padding: 2rem 2.5rem;
      max-width: 440px;
      width: 100%;
      text-align: center;
      backdrop-filter: blur(4px);
    }

    .gauge-label {
      font-family: -apple-system, "Segoe UI", system-ui, sans-serif;
      font-size: 0.72rem;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: #7aaccc;
      margin-bottom: 0.5rem;
    }

    .level-value {
      font-size: 4.5rem;
      font-weight: bold;
      font-family: -apple-system, "Segoe UI", system-ui, sans-serif;
      color: #7dd4f8;
      line-height: 1;
      text-shadow: 0 0 24px rgba(100,200,240,0.4);
    }

    .level-unit {
      font-size: 1.1rem;
      color: #7aaccc;
      margin-left: 6px;
      vertical-align: middle;
    }

    /* water level bar */
    .bar-wrap {
      margin: 1.4rem auto 0;
      width: 80%;
      height: 10px;
      background: rgba(255,255,255,0.1);
      border-radius: 6px;
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      border-radius: 6px;
      background: linear-gradient(to right, #1a6ea8, #7dd4f8);
      transition: width 0.8s ease;
    }

    .divider {
      margin: 1.5rem auto;
      width: 60%;
      border: none;
      border-top: 1px solid rgba(255,255,255,0.1);
    }

    .meta {
      font-family: -apple-system, "Segoe UI", system-ui, sans-serif;
      font-size: 0.82rem;
      color: #7aaccc;
      line-height: 2;
      text-align: left;
    }
    .meta .label { color: #4a8aaa; }
    .meta .value { color: #c8e8f5; font-weight: 600; }

    .loading {
      font-family: -apple-system, "Segoe UI", system-ui, sans-serif;
      font-size: 0.9rem;
      color: #7aaccc;
      font-style: italic;
    }

    /* ---------- footer ---------- */
    footer {
      text-align: center;
      font-family: -apple-system, "Segoe UI", system-ui, sans-serif;
      font-size: 0.72rem;
      color: #3a6a88;
      padding: 1.5rem 1rem;
      border-top: 1px solid rgba(255,255,255,0.06);
      letter-spacing: 0.04em;
    }
  </style>
</head>
<body>

  <!-- Scenic header -->
  <div class="scene">
    <div class="sky"></div>
    <div class="sun-glow"></div>
    <!-- Mission Mountains silhouette -->
    <svg class="mountains" viewBox="0 0 1200 120" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
      <polygon points="0,120 0,80 60,50 120,70 200,20 280,65 340,30 400,60 460,10 520,55 580,25 640,60 700,15 760,50 820,30 880,55 940,20 1000,60 1060,35 1120,55 1200,40 1200,120" fill="#0d1e2e"/>
      <!-- snow caps -->
      <polygon points="200,20 220,35 180,38" fill="rgba(230,240,250,0.7)"/>
      <polygon points="460,10 480,28 440,30" fill="rgba(230,240,250,0.7)"/>
      <polygon points="580,25 598,42 562,43" fill="rgba(230,240,250,0.7)"/>
      <polygon points="700,15 720,33 680,35" fill="rgba(230,240,250,0.7)"/>
      <polygon points="940,20 958,37 922,39" fill="rgba(230,240,250,0.7)"/>
    </svg>
    <div class="water"></div>
  </div>

  <!-- Main content -->
  <div class="page">
    <div class="headline">
      <h1>Flathead Lake</h1>
      <div class="tagline">Pool Level · USGS Station 12371550 · Refreshes every 60 s</div>
    </div>

    <div class="card" id="card">
      <div class="loading">Fetching latest reading…</div>
    </div>
  </div>

  <footer>Data sourced from USGS Water Resources · Polson, Montana</footer>

  <script>
    // Flathead Lake elevation range (ft above sea level) for the progress bar
    const LOW = 2877, HIGH = 2893;

    function load() {
      fetch('/data')
        .then(r => r.json())
        .then(d => {
          const card = document.getElementById('card');
          if (!d.station) {
            card.innerHTML = '<div class="loading">Waiting for first reading…</div>';
            return;
          }
          const pct = Math.min(100, Math.max(0, ((d.value - LOW) / (HIGH - LOW)) * 100));
          const measured = new Date(d.measured_at).toLocaleString('en-US', {month:'short',day:'numeric',year:'numeric',hour:'numeric',minute:'2-digit',timeZoneName:'short'});
          const fetched  = new Date(d.fetched_at).toLocaleString('en-US', {hour:'numeric',minute:'2-digit',second:'2-digit',timeZoneName:'short'});
          card.innerHTML =
            '<div class="gauge-label">Current Pool Elevation</div>' +
            '<div class="level-value">' + d.value.toFixed(2) + '<span class="level-unit">' + d.unit + '</span></div>' +
            '<div class="bar-wrap"><div class="bar-fill" style="width:' + pct.toFixed(1) + '%"></div></div>' +
            '<hr class="divider">' +
            '<div class="meta">' +
              '<div><span class="label">Station &nbsp;&nbsp;&nbsp;</span><span class="value">' + d.station + '</span></div>' +
              '<div><span class="label">Measured &nbsp;</span><span class="value">' + measured + '</span></div>' +
              '<div><span class="label">Fetched &nbsp;&nbsp;</span><span class="value">' + fetched + '</span></div>' +
            '</div>';
        })
        .catch(() => {
          document.getElementById('card').innerHTML = '<div class="loading">Unable to load data.</div>';
        });
    }
    load();
    setInterval(load, 60000);
  </script>
</body>
</html>`

func newMux(queries chan<- chan<- Reading) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, indexHTML)
	})

	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		reply := make(chan Reading, 1)
		queries <- reply
		reading := <-reply
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reading)
	})

	return mux
}

// ---- Main ------------------------------------------------------------------

func main() {
	client := NewClient(os.Getenv("USGS_API_KEY"))

	updates := make(chan Reading, 1)
	queries := make(chan chan<- Reading, 1)
	done := make(chan struct{})

	// State goroutine: single owner of the latest reading.
	go stateLoop(updates, queries)

	// Fetch goroutine: polls USGS and pushes readings to state.
	go fetchLoop(client, updates, done)

	// HTTP server.
	srv := &http.Server{
		Addr:    ":8080",
		Handler: newMux(queries),
	}
	go func() {
		log.Printf("[http] listening on http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[http] server error: %v", err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	fmt.Println("Application running... press Ctrl+C or send SIGTERM to stop.")
	<-sigs
	fmt.Println()

	close(done)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	fmt.Println("Application stopped")
}
