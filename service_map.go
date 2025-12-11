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

type ExternalDependency struct {
	FromService     string
	DependencyType  string // 'database', 'http', 'redis', 'cache'
	DependencyTarget string // e.g., "mysql://db.example.com", "https://api.example.com"
	CallCount       int64
	TotalDuration   float64
	ErrorCount      int64
	BytesSent       int64
	BytesReceived   int64
}

type ServiceMapProcessor struct {
	dependencies map[string]*ServiceDependency // key: "from->to"
	externalDeps map[string]*ExternalDependency // key: "from->type->target"
	mu           sync.RWMutex
	queryClient  *ClickHouseQuery
	writer       *ClickHouseWriter
	clickhouseURL string
	orgID        string
	projectID    string
}

func NewServiceMapProcessor(queryClient *ClickHouseQuery, writer *ClickHouseWriter, orgID, projectID string) *ServiceMapProcessor {
	// Extract URL from queryClient - we'll need to pass it or get it from writer
	// For now, we'll store it when we have access to it
	return &ServiceMapProcessor{
		dependencies: make(map[string]*ServiceDependency),
		externalDeps: make(map[string]*ExternalDependency),
		queryClient:  queryClient,
		writer:       writer,
		clickhouseURL: "", // Will be set via SetURL if needed
		orgID:        orgID,
		projectID:    projectID,
	}
}

// SetURL sets the ClickHouse URL (called during initialization)
func (smp *ServiceMapProcessor) SetURL(url string) {
	smp.clickhouseURL = url
}

// Process span to extract service dependencies
func (smp *ServiceMapProcessor) ProcessSpan(span *Span, parentSpan *Span) {
	// Process external dependencies (HTTP, Redis, SQL, Cache) for all spans, including root spans
	// This allows tracking external dependencies even when there's no parent service
	smp.ProcessExternalDependencies(span)
	
	// Service-to-service dependencies only make sense when there's a parent span
	if parentSpan == nil {
		return // Root span, no service-to-service dependency
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

// ProcessExternalDependencies extracts external dependencies (DB, HTTP, Redis, Cache) from a span
func (smp *ServiceMapProcessor) ProcessExternalDependencies(span *Span) {
	if span.Service == "" {
		return
	}

	smp.mu.Lock()
	defer smp.mu.Unlock()

	// Process SQL/database dependencies
	if len(span.Sql) > 0 {
		for _, sqlItem := range span.Sql {
			if sqlMap, ok := sqlItem.(map[string]interface{}); ok {
				dbSystem := ""
				if db, ok := sqlMap["db_system"].(string); ok && db != "" {
					dbSystem = db
				} else if db, ok := sqlMap["dbSystem"].(string); ok && db != "" {
					dbSystem = db
				}
				
				if dbSystem != "" {
					// Try to extract hostname if available
					dbHost := ""
					if host, ok := sqlMap["db_host"].(string); ok && host != "" {
						dbHost = host
					} else if host, ok := sqlMap["host"].(string); ok && host != "" {
						dbHost = host
					} else if host, ok := sqlMap["server"].(string); ok && host != "" {
						dbHost = host
					}
					
					// Try to get DSN (preferred if available, as it contains full connection info)
					dbDsn := ""
					if dsn, ok := sqlMap["db_dsn"].(string); ok && dsn != "" {
						dbDsn = dsn
					}
					
					// Format target: prefer DSN (without password), then hostname, then just db system
					target := ""
					if dbDsn != "" {
						// Use DSN as target (already has password masked)
						target = dbDsn
					} else if dbHost != "" {
						target = fmt.Sprintf("%s://%s", dbSystem, dbHost)
					} else {
						target = fmt.Sprintf("db:%s", dbSystem)
					}
					key := fmt.Sprintf("%s->database->%s", span.Service, target)
					
					dep, exists := smp.externalDeps[key]
					if !exists {
						dep = &ExternalDependency{
							FromService:     span.Service,
							DependencyType:  "database",
							DependencyTarget: target,
						}
						smp.externalDeps[key] = dep
					}
					
					dep.CallCount++
					if duration, ok := sqlMap["duration_ms"].(float64); ok {
						dep.TotalDuration += duration
					} else if duration, ok := sqlMap["duration"].(float64); ok {
						dep.TotalDuration += duration * 1000 // Convert seconds to ms
					}
					if status, ok := sqlMap["status"].(string); ok && (status == "error" || status == "0") {
						dep.ErrorCount++
					}
				}
			}
		}
	}

	// Process HTTP/cURL dependencies
	if len(span.Http) > 0 {
		for _, httpItem := range span.Http {
			if httpMap, ok := httpItem.(map[string]interface{}); ok {
				url := ""
				if u, ok := httpMap["url"].(string); ok && u != "" {
					url = u
				} else if u, ok := httpMap["URL"].(string); ok && u != "" {
					url = u
				} else if host, ok := httpMap["host"].(string); ok && host != "" {
					scheme := "http"
					if s, ok := httpMap["scheme"].(string); ok && s != "" {
						scheme = s
					}
					url = fmt.Sprintf("%s://%s", scheme, host)
				}
				
				if url != "" {
					// Check if URL contains service name (skip if it does - internal call)
					if strings.Contains(url, span.Service) {
						continue
					}
					
					key := fmt.Sprintf("%s->http->%s", span.Service, url)
					
					dep, exists := smp.externalDeps[key]
					if !exists {
						dep = &ExternalDependency{
							FromService:     span.Service,
							DependencyType:  "http",
							DependencyTarget: url,
						}
						smp.externalDeps[key] = dep
					}
					
					dep.CallCount++
					if duration, ok := httpMap["duration_ms"].(float64); ok {
						dep.TotalDuration += duration
					} else if duration, ok := httpMap["duration"].(float64); ok {
						dep.TotalDuration += duration * 1000
					}
					if status, ok := httpMap["status"].(string); ok && (status == "error" || status == "0") {
						dep.ErrorCount++
					}
					if sent, ok := httpMap["bytes_sent"].(float64); ok {
						dep.BytesSent += int64(sent)
					}
					if recv, ok := httpMap["bytes_received"].(float64); ok {
						dep.BytesReceived += int64(recv)
					}
				}
			}
		}
	}

	// Process Redis dependencies
	if len(span.Redis) > 0 {
		for _, redisItem := range span.Redis {
			if redisMap, ok := redisItem.(map[string]interface{}); ok {
				host := "redis"
				if h, ok := redisMap["host"].(string); ok && h != "" {
					host = fmt.Sprintf("redis://%s", h)
				} else if h, ok := redisMap["Host"].(string); ok && h != "" {
					host = fmt.Sprintf("redis://%s", h)
				}
				
				key := fmt.Sprintf("%s->redis->%s", span.Service, host)
				
				dep, exists := smp.externalDeps[key]
				if !exists {
					dep = &ExternalDependency{
						FromService:     span.Service,
						DependencyType:  "redis",
						DependencyTarget: host,
					}
					smp.externalDeps[key] = dep
				}
				
				dep.CallCount++
				if duration, ok := redisMap["duration_ms"].(float64); ok {
					dep.TotalDuration += duration
				} else if duration, ok := redisMap["duration"].(float64); ok {
					dep.TotalDuration += duration * 1000
				}
				if status, ok := redisMap["status"].(string); ok && (status == "error" || status == "0") {
					dep.ErrorCount++
				}
			}
		}
	}

	// Process Cache dependencies
	if len(span.Cache) > 0 {
		for _, cacheItem := range span.Cache {
			if cacheMap, ok := cacheItem.(map[string]interface{}); ok {
				cacheType := "cache"
				if ct, ok := cacheMap["type"].(string); ok && ct != "" {
					cacheType = fmt.Sprintf("cache:%s", ct)
				} else if ct, ok := cacheMap["Type"].(string); ok && ct != "" {
					cacheType = fmt.Sprintf("cache:%s", ct)
				} else if host, ok := cacheMap["host"].(string); ok && host != "" {
					cacheType = fmt.Sprintf("cache://%s", host)
				}
				
				key := fmt.Sprintf("%s->cache->%s", span.Service, cacheType)
				
				dep, exists := smp.externalDeps[key]
				if !exists {
					dep = &ExternalDependency{
						FromService:     span.Service,
						DependencyType:  "cache",
						DependencyTarget: cacheType,
					}
					smp.externalDeps[key] = dep
				}
				
				dep.CallCount++
				if duration, ok := cacheMap["duration_ms"].(float64); ok {
					dep.TotalDuration += duration
				} else if duration, ok := cacheMap["duration"].(float64); ok {
					dep.TotalDuration += duration * 1000
				}
				if status, ok := cacheMap["status"].(string); ok && (status == "error" || status == "0") {
					dep.ErrorCount++
				}
			}
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
			"dependency_type": "service",
			"dependency_target": "",
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
		// Use the same approach as ClickHouseWriter - POST with data in body
		baseURL := smp.clickhouseURL
		if baseURL == "" {
			// Fallback: try to get from queryClient (we need to store it)
			// For now, skip if URL not set
			continue
		}
		url := fmt.Sprintf("%s/?query=INSERT%%20INTO%%20opa.service_dependencies%%20FORMAT%%20JSONEachRow", 
			strings.TrimRight(baseURL, "/"))
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

	// Flush external dependencies
	for _, extDep := range smp.externalDeps {
		if extDep.CallCount == 0 {
			continue
		}
		
		avgDuration := extDep.TotalDuration / float64(extDep.CallCount)
		errorRate := float64(extDep.ErrorCount) / float64(extDep.CallCount) * 100.0

		row := map[string]interface{}{
			"organization_id": smp.orgID,
			"project_id":      smp.projectID,
			"from_service":    extDep.FromService,
			"to_service":      extDep.DependencyTarget, // Use target as to_service for external deps
			"dependency_type": extDep.DependencyType,
			"dependency_target": extDep.DependencyTarget,
			"date":            date,
			"hour":            hour,
			"call_count":      extDep.CallCount,
			"total_duration_ms": extDep.TotalDuration,
			"avg_duration_ms":   avgDuration,
			"max_duration_ms":   extDep.TotalDuration,
			"error_count":       extDep.ErrorCount,
			"error_rate":         errorRate,
			"bytes_sent":         extDep.BytesSent,
			"bytes_received":     extDep.BytesReceived,
		}

		jsonData, _ := json.Marshal(row)
		baseURL := smp.clickhouseURL
		if baseURL == "" {
			continue
		}
		url := fmt.Sprintf("%s/?query=INSERT%%20INTO%%20opa.service_dependencies%%20FORMAT%%20JSONEachRow", 
			strings.TrimRight(baseURL, "/"))
		req, _ := http.NewRequest("POST", url, bytes.NewReader(jsonData))
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			LogError(err, "Failed to insert external dependency", map[string]interface{}{
				"from_service": extDep.FromService,
				"type": extDep.DependencyType,
				"target": extDep.DependencyTarget,
			})
		} else {
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				bodyBytes := make([]byte, 1024)
				n, _ := resp.Body.Read(bodyBytes)
				bodyStr := string(bodyBytes[:n])
				resp.Body.Close()
				LogError(fmt.Errorf("failed to insert external dependency"), "ServiceMap flush error", map[string]interface{}{
					"status_code": resp.StatusCode,
					"from_service": extDep.FromService,
					"target": extDep.DependencyTarget,
					"type": extDep.DependencyType,
					"response": bodyStr,
				})
			}
			resp.Body.Close()
		}
	}

	// Clear after flush
	smp.dependencies = make(map[string]*ServiceDependency)
	smp.externalDeps = make(map[string]*ExternalDependency)
}

// GetHealthThresholds retrieves configurable health thresholds from database
func (smp *ServiceMapProcessor) GetHealthThresholds() (degradedErrorRate, downErrorRate, degradedLatency, downLatency float64) {
	// Default values
	degradedErrorRate = 10.0
	downErrorRate = 50.0
	degradedLatency = 1000.0
	downLatency = 5000.0

	query := fmt.Sprintf(`SELECT 
		degraded_error_rate,
		down_error_rate,
		degraded_latency_ms,
		down_latency_ms
		FROM opa.service_map_thresholds
		WHERE organization_id = '%s' AND project_id = '%s'
		ORDER BY updated_at DESC
		LIMIT 1`,
		escapeSQL(smp.orgID), escapeSQL(smp.projectID))

	rows, err := smp.queryClient.Query(query)
	if err == nil && len(rows) > 0 {
		if val, ok := rows[0]["degraded_error_rate"].(float64); ok {
			degradedErrorRate = val
		}
		if val, ok := rows[0]["down_error_rate"].(float64); ok {
			downErrorRate = val
		}
		if val, ok := rows[0]["degraded_latency_ms"].(float64); ok {
			degradedLatency = val
		}
		if val, ok := rows[0]["down_latency_ms"].(float64); ok {
			downLatency = val
		}
	}

	return
}

// CalculateHealthStatus determines health status based on thresholds
func (smp *ServiceMapProcessor) CalculateHealthStatus(errorRate, avgLatency float64) string {
	degradedErr, downErr, degradedLat, downLat := smp.GetHealthThresholds()

	if errorRate >= downErr || avgLatency >= downLat {
		return "down"
	}
	if errorRate >= degradedErr || avgLatency >= degradedLat {
		return "degraded"
	}
	return "healthy"
}

// Update service map metadata
func (smp *ServiceMapProcessor) UpdateMetadata() {
	smp.mu.RLock()
	defer smp.mu.RUnlock()

	if len(smp.dependencies) == 0 {
		return // No dependencies to update
	}

	for _, dep := range smp.dependencies {
		if dep.CallCount == 0 {
			continue // Skip empty dependencies
		}
		avgDuration := dep.TotalDuration / float64(dep.CallCount)
		errorRate := float64(dep.ErrorCount) / float64(dep.CallCount) * 100.0

		healthStatus := smp.CalculateHealthStatus(errorRate, avgDuration)

		query := fmt.Sprintf(`INSERT INTO opa.service_map_metadata 
			(organization_id, project_id, from_service, to_service, last_seen, avg_latency_ms, error_rate, call_count, health_status, bytes_sent, bytes_received)
			VALUES ('%s', '%s', '%s', '%s', now(), %.2f, %.2f, %d, '%s', %d, %d)`,
			escapeSQL(smp.orgID),
			escapeSQL(smp.projectID),
			escapeSQL(dep.FromService),
			escapeSQL(dep.ToService),
			avgDuration,
			errorRate,
			dep.CallCount,
			healthStatus,
			dep.BytesSent,
			dep.BytesReceived)

		smp.queryClient.Execute(query)
	}
}

