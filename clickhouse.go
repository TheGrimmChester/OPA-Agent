package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ClickHouseWriter handles batch writes to ClickHouse
type ClickHouseWriter struct {
	url       string
	bufMu     sync.Mutex
	buf       bytes.Buffer
	fullBuf   bytes.Buffer
	count     int
	batchSize int
	client    *http.Client
}

// NewClickHouseWriter creates a new ClickHouse writer
func NewClickHouseWriter(url string, batchSize int) *ClickHouseWriter {
	return &ClickHouseWriter{
		url:       url,
		batchSize: batchSize,
		client:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Add adds a row to the batch buffer
func (w *ClickHouseWriter) Add(minRow map[string]interface{}) {
	w.bufMu.Lock()
	defer w.bufMu.Unlock()
	j, _ := json.Marshal(minRow)
	w.buf.Write(j)
	w.buf.WriteString("\n")
	w.count++
	if w.count >= w.batchSize {
		go w.flushLocked()
	}
}

// AddFull adds a full row (with detailed data) to the buffer
func (w *ClickHouseWriter) AddFull(fullRow map[string]interface{}) {
	w.bufMu.Lock()
	j, _ := json.Marshal(fullRow)
	w.fullBuf.Write(j)
	w.fullBuf.WriteString("\n")
	fullData := w.fullBuf.Bytes()
	var fullDataCopy []byte
	shouldFlush := len(fullData) > 0
	if shouldFlush {
		fullDataCopy = make([]byte, len(fullData))
		copy(fullDataCopy, fullData)
		w.fullBuf.Reset()
	}
	w.bufMu.Unlock()
	
	if shouldFlush {
		go func() {
			url := strings.TrimRight(w.url, "/") + "/?query=INSERT%20INTO%20opa.spans_full%20FORMAT%20JSONEachRow"
			req, _ := http.NewRequest("POST", url, bytes.NewReader(fullDataCopy))
			req.Header.Set("Content-Type", "application/json")
			log.Printf("[DEBUG] Inserting into spans_full: %d bytes", len(fullDataCopy))
			resp, err := w.client.Do(req)
			if err != nil {
				LogError(err, "Failed to flush spans_full to ClickHouse", nil)
			} else {
				bodyBytes, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					LogError(nil, "ClickHouse INSERT failed", map[string]interface{}{
						"status_code": resp.StatusCode,
						"body":        string(bodyBytes),
					})
				} else {
					log.Printf("[DEBUG] Successfully inserted into spans_full: %d bytes", len(fullDataCopy))
				}
			}
		}()
	}
}

func (w *ClickHouseWriter) flushLocked() {
	data := w.buf.Bytes()
	if len(data) > 0 {
		url := strings.TrimRight(w.url, "/") + "/?query=INSERT%20INTO%20opa.spans_min%20FORMAT%20JSONEachRow"
		req, _ := http.NewRequest("POST", url, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		w.client.Do(req)
		w.buf.Reset()
		w.count = 0
	}
	
	fullData := w.fullBuf.Bytes()
	if len(fullData) > 0 {
		url := strings.TrimRight(w.url, "/") + "/?query=INSERT%20INTO%20opa.spans_full%20FORMAT%20JSONEachRow"
		req, _ := http.NewRequest("POST", url, bytes.NewReader(fullData))
		req.Header.Set("Content-Type", "application/json")
		w.client.Do(req)
		w.fullBuf.Reset()
	}
}

// AddRUM adds RUM event to ClickHouse
func (w *ClickHouseWriter) AddRUM(rumEvent map[string]interface{}) {
	rumJSON, _ := json.Marshal(rumEvent)
	rumData := append(rumJSON, '\n')
	
	go func() {
		url := strings.TrimRight(w.url, "/") + "/?query=INSERT%20INTO%20opa.rum_events%20FORMAT%20JSONEachRow"
		req, _ := http.NewRequest("POST", url, bytes.NewReader(rumData))
		req.Header.Set("Content-Type", "application/json")
		resp, err := w.client.Do(req)
		if err != nil {
			LogError(err, "Failed to write RUM event to ClickHouse", nil)
		} else {
			resp.Body.Close()
		}
	}()
}

// AddError adds error instance and group to ClickHouse
func (w *ClickHouseWriter) AddError(errorInstance, errorGroup map[string]interface{}) {
	// Write error instance
	instanceJSON, _ := json.Marshal(errorInstance)
	instanceData := append(instanceJSON, '\n')
	
	// Write error group
	groupJSON, _ := json.Marshal(errorGroup)
	groupData := append(groupJSON, '\n')
	
	// Write both asynchronously
	go func() {
		// Write error instance
		url := strings.TrimRight(w.url, "/") + "/?query=INSERT%20INTO%20opa.error_instances%20FORMAT%20JSONEachRow"
		req, _ := http.NewRequest("POST", url, bytes.NewReader(instanceData))
		req.Header.Set("Content-Type", "application/json")
		resp, err := w.client.Do(req)
		if err != nil {
			LogError(err, "Failed to write error instance to ClickHouse", nil)
		} else {
			resp.Body.Close()
		}
		
		// Write error group
		url = strings.TrimRight(w.url, "/") + "/?query=INSERT%20INTO%20opa.error_groups%20FORMAT%20JSONEachRow"
		req, _ = http.NewRequest("POST", url, bytes.NewReader(groupData))
		req.Header.Set("Content-Type", "application/json")
		resp, err = w.client.Do(req)
		if err != nil {
			LogError(err, "Failed to write error group to ClickHouse", nil)
		} else {
			resp.Body.Close()
		}
	}()
}

// AddLog adds a log entry to ClickHouse
func (w *ClickHouseWriter) AddLog(logEntry map[string]interface{}) {
	// Convert timestamp to DateTime64(3) format if it's a string
	if timestampStr, ok := logEntry["timestamp"].(string); ok {
		// Parse the timestamp string and reformat for ClickHouse DateTime64(3)
		if t, err := time.Parse("2006-01-02 15:04:05.000", timestampStr); err == nil {
			logEntry["timestamp"] = t.Format("2006-01-02 15:04:05.000")
		} else if t, err := time.Parse("2006-01-02 15:04:05", timestampStr); err == nil {
			logEntry["timestamp"] = t.Format("2006-01-02 15:04:05.000")
		} else {
			// If parsing fails, use current time
			logEntry["timestamp"] = time.Now().Format("2006-01-02 15:04:05.000")
		}
	} else {
		// If timestamp is missing, use current time
		logEntry["timestamp"] = time.Now().Format("2006-01-02 15:04:05.000")
	}
	
	// Ensure span_id is properly handled (can be empty string for NULL)
	if spanID, ok := logEntry["span_id"].(string); ok && spanID == "" {
		logEntry["span_id"] = nil
	}
	
	logJSON, _ := json.Marshal(logEntry)
	logData := append(logJSON, '\n')
	
	// Write asynchronously
	go func() {
		url := strings.TrimRight(w.url, "/") + "/?query=INSERT%20INTO%20opa.logs%20FORMAT%20JSONEachRow"
		req, _ := http.NewRequest("POST", url, bytes.NewReader(logData))
		req.Header.Set("Content-Type", "application/json")
		resp, err := w.client.Do(req)
		if err != nil {
			LogError(err, "Failed to write log to ClickHouse", nil)
		} else {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				LogError(nil, "ClickHouse log INSERT failed", map[string]interface{}{
					"status_code": resp.StatusCode,
					"body":        string(bodyBytes),
					"log_data":    string(logData),
				})
			}
		}
	}()
}

// Flush flushes all buffered data to ClickHouse
func (w *ClickHouseWriter) Flush() {
	w.bufMu.Lock()
	defer w.bufMu.Unlock()
	w.flushLocked()
}

// ClickHouseQuery handles queries to ClickHouse
type ClickHouseQuery struct {
	url    string
	client *http.Client
}

// NewClickHouseQuery creates a new ClickHouse query client
func NewClickHouseQuery(url string) *ClickHouseQuery {
	return &ClickHouseQuery{
		url:    url,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Query executes a query and returns results
func (q *ClickHouseQuery) Query(query string) ([]map[string]interface{}, error) {
	queryUpper := strings.ToUpper(query)
	if !strings.Contains(queryUpper, "FORMAT") {
		query = strings.TrimSpace(query) + " FORMAT JSONEachRow"
	}
	
	baseURL := strings.TrimRight(q.url, "/")
	reqURL, err := url.Parse(baseURL)
	if err != nil {
		LogError(err, "Invalid ClickHouse URL", map[string]interface{}{
			"url": baseURL,
		})
		return nil, fmt.Errorf("invalid ClickHouse URL: %v", err)
	}
	
	reqURL.RawQuery = url.Values{
		"query": []string{query},
	}.Encode()
	
	req, err := http.NewRequest("GET", reqURL.String(), nil)
	if err != nil {
		return nil, err
	}
	
	resp, err := q.client.Do(req)
	if err != nil {
		LogError(err, "ClickHouse request failed", map[string]interface{}{
			"url": reqURL.String(),
		})
		return nil, fmt.Errorf("ClickHouse request failed: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		body, _ := bufio.NewReader(resp.Body).ReadString('\n')
		LogError(nil, "ClickHouse query error", map[string]interface{}{
			"status_code": resp.StatusCode,
			"url":         reqURL.String(),
			"body":        body,
		})
		return nil, fmt.Errorf("ClickHouse error (status %d): %s", resp.StatusCode, body)
	}
	
	var results []map[string]interface{}
	scanner := bufio.NewScanner(resp.Body)
	maxCapacity := 10 * 1024 * 1024 // 10MB
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)
	parseErrors := 0
	for scanner.Scan() {
		var row map[string]interface{}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			parseErrors++
			// Log first few parse errors to avoid log spam
			if parseErrors <= 3 {
				rowBytes := scanner.Bytes()
				rowLen := len(rowBytes)
				truncLen := rowLen
				if rowLen > 200 {
					truncLen = 200
				}
				LogError(err, "Failed to parse JSON row from ClickHouse response", map[string]interface{}{
					"row_bytes": string(rowBytes[:truncLen]), // First 200 chars
				})
			}
			continue
		}
		results = append(results, row)
	}
	
	if parseErrors > 3 {
		log.Printf("[WARN] ClickHouse query: %d additional rows failed to parse (only first 3 errors logged)", parseErrors-3)
	}
	
	return results, scanner.Err()
}

// Execute executes a query without returning results
func (q *ClickHouseQuery) Execute(query string) error {
	baseURL := strings.TrimRight(q.url, "/")
	reqURL, err := url.Parse(baseURL)
	if err != nil {
		LogError(err, "Invalid ClickHouse URL in Execute", map[string]interface{}{
			"url": baseURL,
		})
		return fmt.Errorf("invalid ClickHouse URL: %v", err)
	}
	
	reqURL.RawQuery = url.Values{
		"query": []string{query},
	}.Encode()
	
	req, err := http.NewRequest("POST", reqURL.String(), nil)
	if err != nil {
		LogError(err, "Failed to create ClickHouse request", nil)
		return err
	}
	
	resp, err := q.client.Do(req)
	if err != nil {
		LogError(err, "ClickHouse Execute request failed", map[string]interface{}{
			"url": reqURL.String(),
		})
		return fmt.Errorf("ClickHouse request failed: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		body, _ := bufio.NewReader(resp.Body).ReadString('\n')
		LogError(nil, "ClickHouse Execute error", map[string]interface{}{
			"status_code": resp.StatusCode,
			"url":         reqURL.String(),
			"body":        body,
		})
		return fmt.Errorf("ClickHouse error (status %d): %s", resp.StatusCode, body)
	}
	
	return nil
}

