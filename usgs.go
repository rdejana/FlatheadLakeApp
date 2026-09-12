package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	latestContinuousURL = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/latest-continuous/items"
	continuousURL       = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items"
	defaultStationID    = "USGS-12371550"
	defaultParameter    = "00062"
	summerFullPool      = 2893.00
)

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
	u, err := url.Parse(latestContinuousURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	u.RawQuery = q.Encode()

	return c.fetchFeatureCollection(ctx, u.String())
}

func (c *Client) GetContinuousHistory(ctx context.Context, locationID, parameterCode, start, end string) (*FeatureCollection, error) {
	u, err := url.Parse(continuousURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	q.Set("datetime", fmt.Sprintf("%s/%s", start, end))
	q.Set("limit", "10000")
	u.RawQuery = q.Encode()

	return c.fetchFeatureCollection(ctx, u.String())
}

func (c *Client) GetPointContinuous(ctx context.Context, locationID, parameterCode string, point time.Time) (*FeatureCollection, error) {
	u, err := url.Parse(continuousURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	// Query starting at the requested timestamp with a 2-hour forward window to find the closest reading
	endWindow := point.Add(2 * time.Hour)
	q.Set("datetime", fmt.Sprintf("%s/%s", point.UTC().Format(time.RFC3339), endWindow.UTC().Format(time.RFC3339)))
	q.Set("limit", "1")
	u.RawQuery = q.Encode()

	return c.fetchFeatureCollection(ctx, u.String())
}

func (c *Client) fetchFeatureCollection(ctx context.Context, reqURL string) (*FeatureCollection, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
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
