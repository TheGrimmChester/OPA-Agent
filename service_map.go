package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

type ServiceDependency struct {
	FromService string
	ToService   string
	CallCount   int64
	TotalDuration float64
	ErrorCount  int64
	BytesSent   int64
	BytesReceived int64
}

type ServiceMapProcessor struct {
	dependencies map[string]*ServiceDependency // key: "from->to"
	mu           sync.RWMutex
	queryClient  *ClickHouseQuery
	writer       *ClickHouseWriter
	orgID        string
	projectID    string
}

func NewServiceMapProcessor(queryClient *ClickHouseQuery, writer *ClickHouseWriter, orgID, projectID string) *ServiceMapProcessor {
	return &ServiceMapProcessor{
		dependencies: make(map[string]*ServiceDependency),
		queryClient:  queryClient,
		writer:       writer,
		orgID:        orgID,
		projectID:    projectID,
	}
}

// Process span to extract service dependencies
func (smp *ServiceMapProcessor) ProcessSpan(span *Span, parentSpan *Span) {
	if parentSpan == nil {
		return // Root span, no dependency
	}

	fromService := parentSpan.Service
	toService := span.Service

	if fromService == toService {
		return // Same service, not a dependency
	}

	key := fmt.Sprintf("%s->%s", fromService, toService)

	smp.mu.Lock()
	defer smp.mu.Unlock()

	dep, exists := smp.dependencies[key]
	if !exists {
		dep = &ServiceDependency{
			FromService: fromService,
			ToService:   toService,
		}
		smp.dependencies[key] = dep
	}

	dep.CallCount++
	dep.TotalDuration += span.Duration
	if span.Status == "error" || span.Status == "0" {
		dep.ErrorCount++
	}

	// Extract network bytes if available
	if span.Net != nil {
		if sent, ok := span.Net["bytes_sent"].(float64); ok {
			dep.BytesSent += int64(sent)
		}
		if recv, ok := span.Net["bytes_received"].(float64); ok {
			dep.BytesReceived += int64(recv)
		}
	}
}

// Flush dependencies to ClickHouse
func (smp *ServiceMapProcessor) Flush() {
	smp.mu.Lock()
	defer smp.mu.Unlock()

	now := time.Now()
	date := now.Format("2006-01-02")
	hour := now.Format("2006-01-02 15:04:00")

	for _, dep := range smp.dependencies {
		avgDuration := dep.TotalDuration / float64(dep.CallCount)
		errorRate := float64(dep.ErrorCount) / float64(dep.CallCount) * 100.0

		row := map[string]interface{}{
			"organization_id": smp.orgID,
			"project_id":      smp.projectID,
			"from_service":    dep.FromService,
			"to_service":      dep.ToService,
			"date":            date,
			"hour":            hour,
			"call_count":      dep.CallCount,
			"total_duration_ms": dep.TotalDuration,
			"avg_duration_ms":   avgDuration,
			"max_duration_ms":   dep.TotalDuration, // Will be aggregated by SummingMergeTree
			"error_count":       dep.ErrorCount,
			"error_rate":         errorRate,
			"bytes_sent":         dep.BytesSent,
			"bytes_received":     dep.BytesReceived,
		}

		// Write to service_dependencies table via HTTP
		jsonData, _ := json.Marshal(row)
		url := fmt.Sprintf("%s/?query=INSERT%%20INTO%%20opa.service_dependencies%%20FORMAT%%20JSONEachRow", 
			strings.TrimRight(smp.writer.url, "/"))
		req, _ := http.NewRequest("POST", url, bytes.NewReader(jsonData))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			LogError(err, "Failed to insert service dependency", map[string]interface{}{
				"from_service": dep.FromService,
				"to_service":   dep.ToService,
			})
		} else {
			resp.Body.Close()
		}
	}

	// Clear after flush
	smp.dependencies = make(map[string]*ServiceDependency)
}

// Update service map metadata
func (smp *ServiceMapProcessor) UpdateMetadata() {
	smp.mu.RLock()
	defer smp.mu.RUnlock()

	for _, dep := range smp.dependencies {
		avgDuration := dep.TotalDuration / float64(dep.CallCount)
		errorRate := float64(dep.ErrorCount) / float64(dep.CallCount) * 100.0

		healthStatus := "healthy"
		if errorRate > 10.0 || avgDuration > 1000.0 {
			healthStatus = "degraded"
		}
		if errorRate > 50.0 {
			healthStatus = "down"
		}

		query := fmt.Sprintf(`INSERT INTO opa.service_map_metadata 
			(organization_id, project_id, from_service, to_service, last_seen, avg_latency_ms, error_rate, call_count, health_status)
			VALUES ('%s', '%s', '%s', '%s', now(), %.2f, %.2f, %d, '%s')`,
			escapeSQL(smp.orgID),
			escapeSQL(smp.projectID),
			escapeSQL(dep.FromService),
			escapeSQL(dep.ToService),
			avgDuration,
			errorRate,
			dep.CallCount,
			healthStatus)

		smp.queryClient.Execute(query)
	}
}

