package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	MAX_QUEUE_SIZE = 50000
)

var (
	socketPath     = flag.String("socket", "/var/run/opa.sock", "unix socket path")
	tcpAddr        = flag.String("tcp", "", "TCP address to listen on (e.g., :9090 or 0.0.0.0:9090). If empty, TCP listener is disabled")
	metricsAddr    = flag.String("metrics", ":2112", "metrics addr")
	apiAddr        = flag.String("api", ":8080", "admin API addr")
	wsAddr         = flag.String("ws", ":8082", "WebSocket server addr")
	clickhouseURL  = flag.String("clickhouse", "http://clickhouse:8123", "ClickHouse HTTP endpoint")
	batchSize      = flag.Int("batch", 100, "ClickHouse batch size")
	batchInterval  = flag.Int("batch_interval_ms", 1000, "ClickHouse batch interval in ms")
	samplingRate   = flag.Float64("sampling_rate", 1.0, "Default sampling rate")
	alertWorker    *AlertWorker
)


// Extract language metadata from tags or use defaults
func extractLanguageMetadata(inc *Incoming) (language, langVersion, framework, frameworkVersion string) {
	// Default to PHP for backward compatibility
	language = "php"
	
	// First check direct fields
	if inc.Language != nil && *inc.Language != "" {
		language = *inc.Language
	}
	if inc.LanguageVersion != nil {
		langVersion = *inc.LanguageVersion
	}
	if inc.Framework != nil {
		framework = *inc.Framework
	}
	if inc.FrameworkVersion != nil {
		frameworkVersion = *inc.FrameworkVersion
	}
	
	// If not provided directly, try to extract from tags
	if len(inc.Tags) > 0 {
		var tags map[string]interface{}
		if err := json.Unmarshal(inc.Tags, &tags); err == nil {
			// Check for language in tags
			if language == "php" {
				if v, ok := tags["language"].(string); ok && v != "" {
					language = v
				} else if v, ok := tags["lang"].(string); ok && v != "" {
					language = v
				}
			}
			
			// Check for language version
			if langVersion == "" {
				if v, ok := tags["language_version"].(string); ok && v != "" {
					langVersion = v
				} else if v, ok := tags["lang_version"].(string); ok && v != "" {
					langVersion = v
				} else if v, ok := tags["version"].(string); ok && v != "" {
					langVersion = v
				}
			}
			
			// Check for framework
			if framework == "" {
				if v, ok := tags["framework"].(string); ok && v != "" {
					framework = v
				}
			}
			
			// Check for framework version
			if frameworkVersion == "" {
				if v, ok := tags["framework_version"].(string); ok && v != "" {
					frameworkVersion = v
				}
			}
		}
	}
	
	return language, langVersion, framework, frameworkVersion
}


// getStringPtrFromMapForSQL returns a SQL-escaped string for use in SQL queries
// Helper functions for extracting values from maps (getStringFromMap is in utils.go)
func getInt64FromMap(m map[string]interface{}, key string) int64 {
	if v, ok := m[key]; ok {
		if f, ok := v.(float64); ok {
			return int64(f)
		}
		if i, ok := v.(int64); ok {
			return i
		}
		if i, ok := v.(int); ok {
			return int64(i)
		}
	}
	return 0
}

func getStringPtrFromMapForSQL(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if v == nil {
			return "NULL"
		}
		if s, ok := v.(string); ok {
			if s == "" {
				return "NULL"
			}
			return fmt.Sprintf("'%s'", escapeSQL(s))
		}
	}
	return "NULL"
}

func parseDateTime(row map[string]interface{}, key string) int64 {
	if v, ok := row[key]; ok {
		if s, ok := v.(string); ok {
			// Parse ClickHouse DateTime64 format: "2006-01-02 15:04:05.000"
			t, err := time.Parse("2006-01-02 15:04:05.000", s)
			if err != nil {
				// Try without milliseconds
				t, err = time.Parse("2006-01-02 15:04:05", s)
				if err != nil {
					return 0
				}
			}
			return t.UnixMilli()
		}
	}
	return 0
}

// Enrich spans with full details from spans_full
func enrichSpansWithFullDetails(spanList []*Span, traceID string, queryClient *ClickHouseQuery) {
	if len(spanList) == 0 {
		return
	}
	
	traceIDEscaped := strings.ReplaceAll(traceID, "'", "''")
	// Query for full details from spans_full
	query := fmt.Sprintf("SELECT span_id, net, sql, stack, tags, dumps FROM opa.spans_full WHERE trace_id = '%s'", traceIDEscaped)
	detailRows, err := queryClient.Query(query)
	if err != nil || len(detailRows) == 0 {
		// No full details available, that's ok
		return
	}
	
	// Create a map of span_id -> details
	detailMap := make(map[string]map[string]interface{})
	for _, row := range detailRows {
		spanID := getString(row, "span_id")
		detailMap[spanID] = row
	}
	
	// Enrich each span with full details
	for _, span := range spanList {
		if details, ok := detailMap[span.SpanID]; ok {
			// Enrich with network data
			if netStr := getString(details, "net"); netStr != "" && netStr != "{}" {
				if span.Net == nil {
					span.Net = make(map[string]interface{})
				}
				json.Unmarshal([]byte(netStr), &span.Net)
			}
			
			// Enrich with SQL data
			if sqlStr := getString(details, "sql"); sqlStr != "" && sqlStr != "[]" {
				var sqlArray []interface{}
				if err := json.Unmarshal([]byte(sqlStr), &sqlArray); err == nil {
					span.Sql = sqlArray
				}
			}
			
			// Enrich with HTTP requests
			if httpStr := getString(details, "http"); httpStr != "" && httpStr != "[]" {
				var httpArray []interface{}
				if err := json.Unmarshal([]byte(httpStr), &httpArray); err == nil {
					span.Http = httpArray
				}
			}
			
			// Enrich with cache operations
			if cacheStr := getString(details, "cache"); cacheStr != "" && cacheStr != "[]" {
				var cacheArray []interface{}
				if err := json.Unmarshal([]byte(cacheStr), &cacheArray); err == nil {
					span.Cache = cacheArray
				}
			}
			
			// Enrich with Redis operations
			if redisStr := getString(details, "redis"); redisStr != "" && redisStr != "[]" {
				var redisArray []interface{}
				if err := json.Unmarshal([]byte(redisStr), &redisArray); err == nil {
					span.Redis = redisArray
				}
			}
			
			// Enrich with stack trace (needs to be parsed with parseCallStack)
			if stackStr := getString(details, "stack"); stackStr != "" && stackStr != "[]" {
				var stackArray []interface{}
				if err := json.Unmarshal([]byte(stackStr), &stackArray); err == nil {
					span.Stack = parseCallStack(stackArray)
					// Also create flat version for stack trace viewer
					span.StackFlat = flattenCallNodes(span.Stack)
				}
			}
			
			// Enrich with tags
			if tagsStr := getString(details, "tags"); tagsStr != "" && tagsStr != "{}" {
				if span.Tags == nil {
					span.Tags = make(map[string]interface{})
				}
				json.Unmarshal([]byte(tagsStr), &span.Tags)
			}
			
			// Enrich with dumps
			if dumpsStr := getString(details, "dumps"); dumpsStr != "" && dumpsStr != "[]" && dumpsStr != "null" {
				var dumpsArray []interface{}
				if err := json.Unmarshal([]byte(dumpsStr), &dumpsArray); err == nil {
					span.Dumps = dumpsArray
				}
			}
		}
	}
}

// Reconstruct trace from spans
// Parse call stack from interface{} array to CallNode array
// Helper function to parse a single call node (recursive)
func parseCallNode(itemMap map[string]interface{}) *CallNode {
	callNode := &CallNode{}
	if v, ok := itemMap["call_id"].(string); ok {
		callNode.CallID = v
	}
	if v, ok := itemMap["function"].(string); ok {
		callNode.Function = v
	}
	if v, ok := itemMap["class"].(string); ok {
		callNode.Class = v
	}
	if v, ok := itemMap["file"].(string); ok {
		callNode.File = v
	}
	if v, ok := itemMap["line"].(float64); ok {
		callNode.Line = int(v)
	}
	if v, ok := itemMap["duration_ms"].(float64); ok {
		callNode.DurationMs = v
	}
	if v, ok := itemMap["cpu_ms"].(float64); ok {
		callNode.CPUMs = v
	}
	if v, ok := itemMap["memory_delta"].(float64); ok {
		callNode.MemoryDelta = int64(v)
	}
	if v, ok := itemMap["network_bytes_sent"].(float64); ok {
		callNode.NetworkBytesSent = int64(v)
	}
	if v, ok := itemMap["network_bytes_received"].(float64); ok {
		callNode.NetworkBytesReceived = int64(v)
	}
	// Handle parent_id: can be string or null
	if parentIDVal, exists := itemMap["parent_id"]; exists {
		if parentIDStr, ok := parentIDVal.(string); ok && parentIDStr != "" {
			callNode.ParentID = parentIDStr
		} else if parentIDVal == nil {
			// parent_id is null, leave ParentID empty (root call)
			callNode.ParentID = ""
		}
	}
	if v, ok := itemMap["depth"].(float64); ok {
		callNode.Depth = int(v)
	}
	if v, ok := itemMap["function_type"].(float64); ok {
		callNode.FunctionType = int(v)
	}
	if v, ok := itemMap["sql_queries"].([]interface{}); ok {
		callNode.SQLQueries = v
	}
	if v, ok := itemMap["http_requests"].([]interface{}); ok {
		callNode.HttpRequests = v
	}
	if v, ok := itemMap["cache_operations"].([]interface{}); ok {
		callNode.CacheOperations = v
	}
	if v, ok := itemMap["redis_operations"].([]interface{}); ok {
		callNode.RedisOperations = v
	}
	
	// Don't parse children here - they will be rebuilt from parent_id relationships in parseCallStack
	
	return callNode
}

func parseCallStack(stack []interface{}) []*CallNode {
	var callNodes []*CallNode
	callNodeMap := make(map[string]*CallNode)
	
	// First pass: create all call nodes (flat list, no children yet)
	for _, item := range stack {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		
		callNode := parseCallNode(itemMap)
		if callNode.CallID != "" {
			callNodeMap[callNode.CallID] = callNode
		}
	}
	
	// Second pass: build parent-child relationships and find roots
	// Also track insertion order to maintain execution order
	type nodeWithIndex struct {
		node  *CallNode
		index int
	}
	nodeIndices := make(map[string]int)
	for idx, item := range stack {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		callID := ""
		if id, ok := itemMap["call_id"].(string); ok {
			callID = id
		}
		if callID != "" {
			nodeIndices[callID] = idx
		}
	}
	
	for _, callNode := range callNodeMap {
		if callNode.ParentID == "" {
			// Root call - add to result
			callNodes = append(callNodes, callNode)
		} else {
			// Child call - find parent and add as child
			if parent, ok := callNodeMap[callNode.ParentID]; ok {
				parent.Children = append(parent.Children, callNode)
			} else {
				// Orphan node (parent not found) - add as root
				callNodes = append(callNodes, callNode)
			}
		}
	}
	
	// Sort children by their original order in the flat list (execution order)
	for _, callNode := range callNodeMap {
		if len(callNode.Children) > 0 {
			sort.Slice(callNode.Children, func(i, j int) bool {
				idxI := nodeIndices[callNode.Children[i].CallID]
				idxJ := nodeIndices[callNode.Children[j].CallID]
				return idxI < idxJ
			})
		}
	}
	
	// Sort root nodes by their original order
	sort.Slice(callNodes, func(i, j int) bool {
		idxI := nodeIndices[callNodes[i].CallID]
		idxJ := nodeIndices[callNodes[j].CallID]
		return idxI < idxJ
	})
	
	return callNodes
}

// flattenCallNodes recursively flattens a hierarchical call tree into a flat list
func flattenCallNodes(nodes []*CallNode) []*CallNode {
	var flat []*CallNode
	var flatten func([]*CallNode)
	
	flatten = func(nodes []*CallNode) {
		for _, node := range nodes {
			// Create a copy without children for the flat list
			flatNode := &CallNode{
				CallID:              node.CallID,
				Function:            node.Function,
				Class:               node.Class,
				File:                node.File,
				Line:                node.Line,
				DurationMs:          node.DurationMs,
				CPUMs:               node.CPUMs,
				MemoryDelta:         node.MemoryDelta,
				NetworkBytesSent:    node.NetworkBytesSent,
				NetworkBytesReceived: node.NetworkBytesReceived,
				ParentID:            node.ParentID,
				Depth:               node.Depth,
				FunctionType:        node.FunctionType,
				SQLQueries:          node.SQLQueries,
				// Children is intentionally omitted for flat list
			}
			flat = append(flat, flatNode)
			// Recursively flatten children
			if len(node.Children) > 0 {
				flatten(node.Children)
			}
		}
	}
	
	flatten(nodes)
	return flat
}

// collectSQLQueriesFromCallStack recursively collects all SQL queries from call stack
func collectSQLQueriesFromCallStack(nodes []*CallNode) []interface{} {
	var allQueries []interface{}
	
	var collect func([]*CallNode)
	collect = func(nodes []*CallNode) {
		for _, node := range nodes {
			// Collect SQL queries from this node
			if len(node.SQLQueries) > 0 {
				allQueries = append(allQueries, node.SQLQueries...)
			}
			// Recursively collect from children
			if len(node.Children) > 0 {
				collect(node.Children)
			}
		}
	}
	
	collect(nodes)
	return allQueries
}

// collectHttpRequestsFromCallStack recursively collects all HTTP requests from call stack
func collectHttpRequestsFromCallStack(nodes []*CallNode) []interface{} {
	var allRequests []interface{}
	
	var collect func([]*CallNode)
	collect = func(nodes []*CallNode) {
		for _, node := range nodes {
			// Collect HTTP requests from this node
			if len(node.HttpRequests) > 0 {
				allRequests = append(allRequests, node.HttpRequests...)
			}
			// Recursively collect from children
			if len(node.Children) > 0 {
				collect(node.Children)
			}
		}
	}
	
	collect(nodes)
	return allRequests
}

// collectCacheOperationsFromCallStack recursively collects all cache operations from call stack
func collectCacheOperationsFromCallStack(nodes []*CallNode) []interface{} {
	var allOps []interface{}
	
	var collect func([]*CallNode)
	collect = func(nodes []*CallNode) {
		for _, node := range nodes {
			// Collect cache operations from this node
			if len(node.CacheOperations) > 0 {
				allOps = append(allOps, node.CacheOperations...)
			}
			// Recursively collect from children
			if len(node.Children) > 0 {
				collect(node.Children)
			}
		}
	}
	
	collect(nodes)
	return allOps
}

// collectRedisOperationsFromCallStack recursively collects all Redis operations from call stack
func collectRedisOperationsFromCallStack(nodes []*CallNode) []interface{} {
	var allOps []interface{}
	
	var collect func([]*CallNode)
	collect = func(nodes []*CallNode) {
		for _, node := range nodes {
			// Collect Redis operations from this node
			if len(node.RedisOperations) > 0 {
				allOps = append(allOps, node.RedisOperations...)
			}
			// Recursively collect from children
			if len(node.Children) > 0 {
				collect(node.Children)
			}
		}
	}
	
	collect(nodes)
	return allOps
}

func reconstructTrace(spans []*Span) *Trace {
	if len(spans) == 0 {
		return nil
	}
	
	// Sort by start time
	sort.Slice(spans, func(i, j int) bool {
		return spans[i].StartTS < spans[j].StartTS
	})
	
	// Build span map
	spanMap := make(map[string]*Span)
	for _, span := range spans {
		spanMap[span.SpanID] = span
	}
	
	// Build tree
	var root *Span
	for _, span := range spans {
		if span.ParentID == nil {
			root = span
		} else {
			if parent, ok := spanMap[*span.ParentID]; ok {
				parent.Children = append(parent.Children, span)
			}
		}
	}
	
	return &Trace{
		TraceID: spans[0].TraceID,
		Spans:   spans,
		Root:    root,
	}
}

// logResponseWriter wraps http.ResponseWriter to capture status code and response size
type logResponseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int
}

func (lrw *logResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *logResponseWriter) Write(b []byte) (int, error) {
	if lrw.statusCode == 0 {
		lrw.statusCode = 200
	}
	size, err := lrw.ResponseWriter.Write(b)
	lrw.size += size
	return size, err
}

func main() {
	flag.Parse()
	
	// Override flags with environment variables if set
	if tcpEnv := os.Getenv("TRANSPORT_TCP"); tcpEnv != "" {
		*tcpAddr = tcpEnv
	}
	if socketEnv := os.Getenv("SOCKET_PATH"); socketEnv != "" {
		*socketPath = socketEnv
	}
	if wsPortEnv := os.Getenv("WS_PORT"); wsPortEnv != "" {
		*wsAddr = ":" + wsPortEnv
	}
	
	// Start metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("metrics at %s", *metricsAddr)
		log.Fatal(http.ListenAndServe(*metricsAddr, nil))
	}()
	
	// Admin API
	mux := http.NewServeMux()
	
	// Initialize auth handler
	authHandler := NewAuthHandler(queryClient)
	
	// Public endpoints (no auth required)
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	
	// Auth endpoints
	mux.HandleFunc("/api/auth/login", authHandler.Login)
	mux.HandleFunc("/api/auth/register", authHandler.Register)
	
	// API Key endpoints
	mux.HandleFunc("/api/api-keys", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			authHandler.ListAPIKeys(w, r)
		case "POST":
			authHandler.CreateAPIKey(w, r)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	mux.HandleFunc("/api/api-keys/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" {
			authHandler.DeleteAPIKey(w, r)
		} else {
			http.Error(w, "method not allowed", 405)
		}
	})
	
	// Organizations endpoints
	mux.HandleFunc("/api/organizations", func(w http.ResponseWriter, r *http.Request) {
		ctx, _ := ExtractTenantContext(r, queryClient)
		switch r.Method {
		case "GET":
			query := "SELECT org_id, name, settings, created_at, updated_at FROM opa.organizations"
			// If user has organization context, filter by it
			if ctx.OrganizationID != "default-org" {
				query += fmt.Sprintf(" WHERE org_id = '%s'", escapeSQL(ctx.OrganizationID))
			}
			query += " ORDER BY created_at DESC"
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			var orgs []map[string]interface{}
			for _, row := range rows {
				orgs = append(orgs, map[string]interface{}{
					"org_id":     getString(row, "org_id"),
					"name":       getString(row, "name"),
					"settings":   getString(row, "settings"),
					"created_at": getString(row, "created_at"),
					"updated_at": getString(row, "updated_at"),
				})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"organizations": orgs})
		case "POST":
			var org struct {
				Name     string `json:"name"`
				Settings string `json:"settings"`
			}
			if err := json.NewDecoder(r.Body).Decode(&org); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			orgID := generateID()
			if org.Settings == "" {
				org.Settings = "{}"
			}
			query := fmt.Sprintf(`INSERT INTO opa.organizations (org_id, name, settings) 
				VALUES ('%s', '%s', '%s')`,
				escapeSQL(orgID),
				escapeSQL(org.Name),
				escapeSQL(org.Settings))
			if err := queryClient.Execute(query); err != nil {
				LogError(err, "ClickHouse insert error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("insert error: %v", err), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"org_id":    orgID,
				"name":      org.Name,
				"settings":  org.Settings,
				"created_at": time.Now().Format("2006-01-02 15:04:05"),
			})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	// Projects endpoints
	mux.HandleFunc("/api/projects", func(w http.ResponseWriter, r *http.Request) {
		ctx, _ := ExtractTenantContext(r, queryClient)
		switch r.Method {
		case "GET":
			orgID := r.URL.Query().Get("organization_id")
			if orgID == "" {
				orgID = ctx.OrganizationID
			}
			query := fmt.Sprintf("SELECT project_id, org_id, name, dsn, created_at, updated_at FROM opa.projects WHERE org_id = '%s' ORDER BY created_at DESC",
				escapeSQL(orgID))
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			var projects []map[string]interface{}
			for _, row := range rows {
				projects = append(projects, map[string]interface{}{
					"project_id": getString(row, "project_id"),
					"org_id":     getString(row, "org_id"),
					"name":       getString(row, "name"),
					"dsn":        getString(row, "dsn"),
					"created_at": getString(row, "created_at"),
					"updated_at": getString(row, "updated_at"),
				})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"projects": projects})
		case "POST":
			var project struct {
				OrgID string `json:"org_id"`
				Name  string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&project); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			if project.OrgID == "" {
				project.OrgID = ctx.OrganizationID
			}
			projectID := generateID()
			dsn := GenerateDSN(project.OrgID, projectID)
			query := fmt.Sprintf(`INSERT INTO opa.projects (project_id, org_id, name, dsn) 
				VALUES ('%s', '%s', '%s', '%s')`,
				escapeSQL(projectID),
				escapeSQL(project.OrgID),
				escapeSQL(project.Name),
				escapeSQL(dsn))
			if err := queryClient.Execute(query); err != nil {
				LogError(err, "ClickHouse insert error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("insert error: %v", err), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"project_id": projectID,
				"org_id":     project.OrgID,
				"name":       project.Name,
				"dsn":        dsn,
				"created_at": time.Now().Format("2006-01-02 15:04:05"),
			})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	// Health check
	
	// Stats
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := map[string]interface{}{
			"queue_size": atomic.LoadInt64(&currentQueueSize),
		}
		json.NewEncoder(w).Encode(stats)
	})
	
	// POST /api/traces/batch-delete - Delete multiple traces
	// This must be registered BEFORE /api/traces/ to ensure proper routing
	mux.HandleFunc("/api/traces/batch-delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		ctx, _ := ExtractTenantContext(r, queryClient)
		
		var reqBody struct {
			TraceIDs []string `json:"trace_ids"`
		}
		
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %v", err), 400)
			return
		}
		
		if len(reqBody.TraceIDs) == 0 {
			http.Error(w, "trace_ids must be a non-empty array", 400)
			return
		}
		
		var deleted []string
		var failed []map[string]interface{}
		
		for _, traceID := range reqBody.TraceIDs {
			traceIDEscaped := strings.ReplaceAll(traceID, "'", "''")
			
			// Delete from spans_min with tenant filter
			queryMin := fmt.Sprintf("ALTER TABLE opa.spans_min DELETE WHERE trace_id = '%s' AND organization_id = '%s' AND project_id = '%s'",
				traceIDEscaped, escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			errMin := queryClient.Execute(queryMin)
			
			// Delete from spans_full with tenant filter
			queryFull := fmt.Sprintf("ALTER TABLE opa.spans_full DELETE WHERE trace_id = '%s' AND organization_id = '%s' AND project_id = '%s'",
				traceIDEscaped, escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			errFull := queryClient.Execute(queryFull)
			
			if errMin != nil && errFull != nil {
				failed = append(failed, map[string]interface{}{
					"id":    traceID,
					"error": fmt.Sprintf("min: %v, full: %v", errMin, errFull),
				})
			} else {
				deleted = append(deleted, traceID)
				// Also remove from buffer if present
				tb.GetAndClear(traceID)
			}
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":       "completed",
			"deleted":      deleted,
			"failed":       failed,
			"deleted_count": len(deleted),
			"failed_count": len(failed),
		})
	})
	
	// Helper function to handle list traces request
	handleListTraces := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		ctx, _ := ExtractTenantContext(r, queryClient)
		
		// Parse query parameters
		service := r.URL.Query().Get("service")
		status := r.URL.Query().Get("status")
		language := r.URL.Query().Get("language")
		framework := r.URL.Query().Get("framework")
		version := r.URL.Query().Get("version")
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		minDuration := r.URL.Query().Get("min_duration")
		maxDuration := r.URL.Query().Get("max_duration")
		scheme := r.URL.Query().Get("scheme")
		host := r.URL.Query().Get("host")
		uri := r.URL.Query().Get("uri")
		queryString := r.URL.Query().Get("query_string")
		sortBy := r.URL.Query().Get("sort")
		if sortBy == "" {
			sortBy = "time"
		}
		sortOrder := r.URL.Query().Get("order")
		if sortOrder == "" {
			sortOrder = "desc"
		}
		limit := r.URL.Query().Get("limit")
		if limit == "" {
			limit = "50"
		}
		offset := r.URL.Query().Get("offset")
		if offset == "" {
			offset = "0"
		}
		
		// Build query - use subquery to filter before aggregation
		// If HTTP request filters are present, we need to join with spans_full to access tags
		hasHttpFilters := scheme != "" || host != "" || uri != "" || queryString != ""
		
		var baseQuery string
		if hasHttpFilters {
			// Join spans_min with spans_full to access tags for HTTP filtering
			baseQuery = "SELECT DISTINCT sm.trace_id, sm.service, sm.start_ts, sm.end_ts, sm.duration_ms, sm.status, sm.language, sm.language_version, sm.framework, sm.framework_version "
			baseQuery += "FROM opa.spans_min sm "
			baseQuery += "INNER JOIN opa.spans_full sf ON sm.trace_id = sf.trace_id AND sm.span_id = sf.span_id "
			if ctx.IsAllTenants() {
				baseQuery += "WHERE 1=1"
			} else {
				baseQuery += fmt.Sprintf("WHERE (coalesce(nullif(sm.organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(sm.project_id, ''), 'default-project') = '%s')",
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			}
			
			// Add HTTP request filters using JSONExtractString
			if scheme != "" {
				baseQuery += fmt.Sprintf(" AND JSONExtractString(sf.tags, 'http_request', 'scheme') = '%s'", strings.ReplaceAll(scheme, "'", "''"))
			}
			if host != "" {
				baseQuery += fmt.Sprintf(" AND JSONExtractString(sf.tags, 'http_request', 'host') LIKE '%%%s%%'", strings.ReplaceAll(host, "'", "''"))
			}
			if uri != "" {
				baseQuery += fmt.Sprintf(" AND JSONExtractString(sf.tags, 'http_request', 'uri') LIKE '%%%s%%'", strings.ReplaceAll(uri, "'", "''"))
			}
			if queryString != "" {
				baseQuery += fmt.Sprintf(" AND JSONExtractString(sf.tags, 'http_request', 'query_string') LIKE '%%%s%%'", strings.ReplaceAll(queryString, "'", "''"))
			}
		} else {
			// No HTTP filters, use simpler query on spans_min only
			baseQuery = "SELECT trace_id, service, start_ts, end_ts, duration_ms, status, language, language_version, framework, framework_version "
			if ctx.IsAllTenants() {
				baseQuery += "FROM opa.spans_min WHERE 1=1"
			} else {
				baseQuery += fmt.Sprintf("FROM opa.spans_min WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			}
		}
		
		// Add other filters
		if service != "" {
			if hasHttpFilters {
				baseQuery += fmt.Sprintf(" AND sm.service = '%s'", strings.ReplaceAll(service, "'", "''"))
			} else {
				baseQuery += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
			}
		}
		if status != "" && status != "all" {
			if hasHttpFilters {
				baseQuery += fmt.Sprintf(" AND sm.status = '%s'", strings.ReplaceAll(status, "'", "''"))
			} else {
				baseQuery += fmt.Sprintf(" AND status = '%s'", strings.ReplaceAll(status, "'", "''"))
			}
		}
		if language != "" {
			if hasHttpFilters {
				baseQuery += fmt.Sprintf(" AND sm.language = '%s'", strings.ReplaceAll(language, "'", "''"))
			} else {
				baseQuery += fmt.Sprintf(" AND language = '%s'", strings.ReplaceAll(language, "'", "''"))
			}
		}
		if framework != "" {
			if hasHttpFilters {
				baseQuery += fmt.Sprintf(" AND sm.framework = '%s'", strings.ReplaceAll(framework, "'", "''"))
			} else {
				baseQuery += fmt.Sprintf(" AND framework = '%s'", strings.ReplaceAll(framework, "'", "''"))
			}
		}
		if version != "" {
			if hasHttpFilters {
				baseQuery += fmt.Sprintf(" AND sm.language_version = '%s'", strings.ReplaceAll(version, "'", "''"))
			} else {
				baseQuery += fmt.Sprintf(" AND language_version = '%s'", strings.ReplaceAll(version, "'", "''"))
			}
		}
		// Only apply time filter if explicitly provided
		if timeFrom != "" {
			if hasHttpFilters {
				baseQuery += fmt.Sprintf(" AND sm.start_ts >= '%s'", timeFrom)
			} else {
				baseQuery += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
			}
		}
		if timeTo != "" {
			if hasHttpFilters {
				baseQuery += fmt.Sprintf(" AND sm.start_ts <= '%s'", timeTo)
			} else {
				baseQuery += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
			}
		}
		
		// Build final aggregation query
		query := "SELECT trace_id, service, min(start_ts) as start_ts, max(end_ts) as end_ts, "
		query += "sum(duration_ms) as duration_ms, count(*) as span_count, "
		query += "any(status) as status, any(language) as language, any(language_version) as language_version, "
		query += "any(framework) as framework, any(framework_version) as framework_version FROM (" + baseQuery + ") GROUP BY trace_id, service"
		
		// Apply duration filters after grouping
		if minDuration != "" {
			query += fmt.Sprintf(" HAVING duration_ms >= %s", minDuration)
		}
		if maxDuration != "" {
			if minDuration != "" {
				query += fmt.Sprintf(" AND duration_ms <= %s", maxDuration)
			} else {
				query += fmt.Sprintf(" HAVING duration_ms <= %s", maxDuration)
			}
		}
		
		// Sorting
		if sortBy == "time" {
			query += fmt.Sprintf(" ORDER BY start_ts %s", strings.ToUpper(sortOrder))
		} else if sortBy == "duration" {
			query += fmt.Sprintf(" ORDER BY duration_ms %s", strings.ToUpper(sortOrder))
		}
		
		// Pagination
		limitInt, _ := strconv.Atoi(limit)
		offsetInt, _ := strconv.Atoi(offset)
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", limitInt, offsetInt)
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		// Convert to response format
		var traces []map[string]interface{}
		for _, row := range rows {
			trace := map[string]interface{}{
				"trace_id":   getString(row, "trace_id"),
				"service":    getString(row, "service"),
				"start_ts":    getString(row, "start_ts"),
				"end_ts":      getString(row, "end_ts"),
				"duration_ms": getFloat64(row, "duration_ms"),
				"span_count":  getUint64(row, "span_count"),
				"status":      getString(row, "status"),
			}
			// Add optional language metadata fields
			if lang := getStringPtr(row, "language"); lang != nil {
				trace["language"] = *lang
			}
			if langVer := getStringPtr(row, "language_version"); langVer != nil {
				trace["language_version"] = *langVer
			}
			if fw := getStringPtr(row, "framework"); fw != nil {
				trace["framework"] = *fw
			}
			if fwVer := getStringPtr(row, "framework_version"); fwVer != nil {
				trace["framework_version"] = *fwVer
			}
			traces = append(traces, trace)
		}
		
		// Get total count for pagination
		countQuery := strings.Split(query, "LIMIT")[0]
		countQuery = "SELECT count(*) as total FROM (" + countQuery + ")"
		countRows, _ := queryClient.Query(countQuery)
		total := int64(0)
		if len(countRows) > 0 {
			total = int64(getUint64(countRows[0], "total"))
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"traces": traces,
			"total":  total,
			"limit":  limitInt,
			"offset": offsetInt,
		})
	}

	// DELETE /api/traces/:trace_id - Delete a single trace
	// GET /api/traces/:trace_id - Get a single trace
	// This must be registered BEFORE /api/traces to ensure proper routing
	mux.HandleFunc("/api/traces/", func(w http.ResponseWriter, r *http.Request) {
		// Parse trace ID from path - handle URL encoding
		path := strings.TrimPrefix(r.URL.Path, "/api/traces/")
		path = strings.TrimSuffix(path, "/")
		parts := strings.Split(path, "/")
		traceID := parts[0]
		
		
		// Handle logs endpoint
		if len(parts) >= 2 && parts[1] == "logs" {
			if r.Method != "GET" {
				http.Error(w, "method not allowed", 405)
				return
			}
			
			limit := r.URL.Query().Get("limit")
			if limit == "" {
				limit = "100"
			}
			limitInt, _ := strconv.Atoi(limit)
			
			if logCorrelation == nil {
				http.Error(w, "log correlation not initialized", 500)
				return
			}
			
			logs, err := logCorrelation.GetLogsForTrace(traceID, limitInt)
			if err != nil {
				http.Error(w, fmt.Sprintf("error: %v", err), 500)
				return
			}
			
			json.NewEncoder(w).Encode(map[string]interface{}{
				"logs":  logs,
				"count": len(logs),
			})
			return
		}
		
		// If no trace ID in path and it's a GET with query params, handle as list request
		if traceID == "" {
			// Check if this looks like a list request (GET with query params)
			if r.Method == "GET" && len(r.URL.Query()) > 0 {
				// Handle as list request
				handleListTraces(w, r)
				return
			}
			http.Error(w, "bad request - trace ID required", 400)
			return
		}
		
		// URL decode the trace ID
		decodedTraceID, err := url.QueryUnescape(traceID)
		if err == nil {
			traceID = decodedTraceID
		}
		
		// Skip batch-delete endpoint - it's handled separately
		if traceID == "batch-delete" {
			http.Error(w, "not found", 404)
			return
		}
		
		// Handle DELETE method
		if r.Method == "DELETE" {
			// Escape trace_id for SQL
			traceIDEscaped := strings.ReplaceAll(traceID, "'", "''")
			
			// Delete from spans_min
			queryMin := fmt.Sprintf("ALTER TABLE opa.spans_min DELETE WHERE trace_id = '%s'", traceIDEscaped)
			errMin := queryClient.Execute(queryMin)
			
			// Delete from spans_full
			queryFull := fmt.Sprintf("ALTER TABLE opa.spans_full DELETE WHERE trace_id = '%s'", traceIDEscaped)
			errFull := queryClient.Execute(queryFull)
			
			// If both fail, return error. If one succeeds, we consider it a success
			if errMin != nil && errFull != nil {
				http.Error(w, fmt.Sprintf("failed to delete trace: %v, %v", errMin, errFull), 500)
				return
			}
			
			// Also remove from buffer if present
			tb.GetAndClear(traceID)
			
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status":    "deleted",
				"trace_id":  traceID,
			})
			return
		}
		
		// Handle GET method (existing logic)
		// Check if it's /full endpoint
		isFullEndpoint := len(parts) >= 2 && parts[1] == "full"
		if isFullEndpoint {
			traceIDEscaped := strings.ReplaceAll(traceID, "'", "''")
			var rows []map[string]interface{}
			var err error
			fromFull := false
			
			// First try to get from spans_full
			query := fmt.Sprintf("SELECT * FROM opa.spans_full WHERE trace_id = '%s' ORDER BY start_ts", traceIDEscaped)
			rows, err = queryClient.Query(query)
			if err != nil {
				LogError(err, "Error querying spans_full", map[string]interface{}{
					"trace_id": traceID,
				})
			}
			fromFull = (err == nil && len(rows) > 0)
			
			// If not found in spans_full, try spans_min
			if !fromFull {
				query = fmt.Sprintf("SELECT * FROM opa.spans_min WHERE trace_id = '%s' ORDER BY start_ts", traceIDEscaped)
				rows, err = queryClient.Query(query)
				if err != nil {
					LogError(err, "Error querying spans_min", map[string]interface{}{
						"trace_id": traceID,
					})
				} else {
					log.Printf("Found %d rows in spans_min for trace_id=%s", len(rows), traceID)
				}
			}
			
			// If found in ClickHouse, process it
			if err == nil && len(rows) > 0 {
				var spanList []*Span
				for _, row := range rows {
					span := &Span{
						TraceID:  getString(row, "trace_id"),
						SpanID:   getString(row, "span_id"),
						Service:  getString(row, "service"),
						Name:     getString(row, "name"),
						Status:   getString(row, "status"),
					}
					if pid := getStringPtr(row, "parent_id"); pid != nil {
						span.ParentID = pid
					}
					if startTS := parseDateTime(row, "start_ts"); startTS > 0 {
						span.StartTS = startTS
					}
					if endTS := parseDateTime(row, "end_ts"); endTS > 0 {
						span.EndTS = endTS
					}
					span.Duration = getFloat64(row, "duration_ms")
					span.CPUms = getFloat64(row, "cpu_ms")
					
					// If reading from spans_full, load full data directly
					if fromFull {
						if netStr := getString(row, "net"); netStr != "" && netStr != "{}" {
							span.Net = make(map[string]interface{})
							json.Unmarshal([]byte(netStr), &span.Net)
						}
						if sqlStr := getString(row, "sql"); sqlStr != "" && sqlStr != "[]" {
							var sqlArray []interface{}
							if err := json.Unmarshal([]byte(sqlStr), &sqlArray); err == nil {
								span.Sql = sqlArray
							}
						}
						// Parse stack correctly: JSON string -> []interface{} -> []*CallNode
						if stackStr := getString(row, "stack"); stackStr != "" && stackStr != "[]" {
							var stackArray []interface{}
							if err := json.Unmarshal([]byte(stackStr), &stackArray); err == nil {
								span.Stack = parseCallStack(stackArray)
								// Also create flat version for stack trace viewer
								span.StackFlat = flattenCallNodes(span.Stack)
							}
						}
						if tagsStr := getString(row, "tags"); tagsStr != "" && tagsStr != "{}" {
							span.Tags = make(map[string]interface{})
							json.Unmarshal([]byte(tagsStr), &span.Tags)
						}
					dumpsStr := getString(row, "dumps")
					if dumpsStr != "" && dumpsStr != "[]" && dumpsStr != "null" {
						var dumpsArray []interface{}
						if err := json.Unmarshal([]byte(dumpsStr), &dumpsArray); err == nil {
							span.Dumps = dumpsArray
						}
					}
					}
					spanList = append(spanList, span)
				}
				
				// Always try to enrich with full details from spans_full, even if base spans came from spans_min
				if !fromFull {
					enrichSpansWithFullDetails(spanList, traceID, queryClient)
				}
				
				trace := reconstructTrace(spanList)
				if trace != nil && len(trace.Spans) > 0 {
					json.NewEncoder(w).Encode(trace)
					return
				}
				// If reconstruction failed or empty, log and fall through
				LogWarn("Trace reconstruction failed or empty", map[string]interface{}{
					"trace_id": traceID,
					"span_count": len(spanList),
				})
			}
			
			// If not found in ClickHouse, try buffer as last resort
			spans := tb.GetAndClear(traceID)
			if len(spans) > 0 {
				var spanList []*Span
				for _, raw := range spans {
					var inc Incoming
					if err := json.Unmarshal(raw, &inc); err != nil {
						continue
					}
					span := &Span{
						TraceID:  inc.TraceID,
						SpanID:  inc.SpanID,
						ParentID: inc.ParentID,
						Service:  inc.Service,
						Name:     inc.Name,
						StartTS:  inc.StartTS,
						EndTS:    inc.EndTS,
						Duration: inc.Duration,
						CPUms:    inc.CPUms,
						Status:   inc.Status,
					}
					if len(inc.Net) > 0 {
						json.Unmarshal(inc.Net, &span.Net)
					}
					if len(inc.Sql) > 0 {
						var sqlArray []interface{}
						if err := json.Unmarshal(inc.Sql, &sqlArray); err == nil {
							span.Sql = sqlArray
						}
					}
					if len(inc.Stack) > 0 {
						span.Stack = parseCallStack(inc.Stack)
						// Also create flat version for stack trace viewer
						span.StackFlat = flattenCallNodes(span.Stack)
					}
					if len(inc.Tags) > 0 {
						var tagsMap map[string]interface{}
						if err := json.Unmarshal(inc.Tags, &tagsMap); err == nil {
							span.Tags = tagsMap
						}
					}
					if len(inc.Dumps) > 0 {
						var dumpsArray []interface{}
						if err := json.Unmarshal(inc.Dumps, &dumpsArray); err == nil {
							span.Dumps = dumpsArray
						}
					}
					spanList = append(spanList, span)
				}
				trace := reconstructTrace(spanList)
				if trace != nil {
					json.NewEncoder(w).Encode(trace)
					return
				}
			}
			
			// Not found anywhere
			http.Error(w, "trace not found", 404)
			return
		}
		
		// Get from buffer first, then try ClickHouse
		spans := tb.GetAndClear(traceID)
		if len(spans) == 0 {
			// Try to get from spans_min
			traceIDEscaped := strings.ReplaceAll(traceID, "'", "''")
			query := fmt.Sprintf("SELECT * FROM opa.spans_min WHERE trace_id = '%s' ORDER BY start_ts", traceIDEscaped)
			rows, err := queryClient.Query(query)
			if err != nil || len(rows) == 0 {
			http.Error(w, "trace not found", 404)
				return
			}
			
			// Convert spans_min rows to spans
			var spanList []*Span
			for _, row := range rows {
				span := &Span{
					TraceID:  getString(row, "trace_id"),
					SpanID:   getString(row, "span_id"),
					Service:  getString(row, "service"),
					Name:     getString(row, "name"),
					Status:   getString(row, "status"),
				}
				if lang := getStringPtr(row, "language"); lang != nil {
					span.Language = lang
				}
				if langVer := getStringPtr(row, "language_version"); langVer != nil {
					span.LanguageVersion = langVer
				}
				if fw := getStringPtr(row, "framework"); fw != nil {
					span.Framework = fw
				}
				if fwVer := getStringPtr(row, "framework_version"); fwVer != nil {
					span.FrameworkVersion = fwVer
				}
				if pid := getStringPtr(row, "parent_id"); pid != nil {
					span.ParentID = pid
				}
				if startTS := parseDateTime(row, "start_ts"); startTS > 0 {
					span.StartTS = startTS
				}
				if endTS := parseDateTime(row, "end_ts"); endTS > 0 {
					span.EndTS = endTS
				}
				span.Duration = getFloat64(row, "duration_ms")
				span.CPUms = getFloat64(row, "cpu_ms")
				spanList = append(spanList, span)
			}
			
			// Enrich with full details from spans_full (including dumps)
			enrichSpansWithFullDetails(spanList, traceID, queryClient)
			
			trace := reconstructTrace(spanList)
			json.NewEncoder(w).Encode(trace)
			return
		}
		
		var spanList []*Span
		for _, raw := range spans {
			var inc Incoming
			if err := json.Unmarshal(raw, &inc); err != nil {
				continue
			}
			span := &Span{
				TraceID:  inc.TraceID,
				SpanID:  inc.SpanID,
				ParentID: inc.ParentID,
				Service:  inc.Service,
				Name:     inc.Name,
				StartTS:  inc.StartTS,
				EndTS:    inc.EndTS,
				Duration: inc.Duration,
				CPUms:    inc.CPUms,
				Status:   inc.Status,
			}
			span.Language = inc.Language
			span.LanguageVersion = inc.LanguageVersion
			span.Framework = inc.Framework
			span.FrameworkVersion = inc.FrameworkVersion
			if len(inc.Net) > 0 {
				json.Unmarshal(inc.Net, &span.Net)
			}
			if len(inc.Sql) > 0 {
				var sqlArray []interface{}
				if err := json.Unmarshal(inc.Sql, &sqlArray); err == nil {
					span.Sql = sqlArray
				}
			}
			// Parse call stack
			if len(inc.Stack) > 0 {
				span.Stack = parseCallStack(inc.Stack)
			}
			if len(inc.Tags) > 0 {
				var tagsMap map[string]interface{}
				if err := json.Unmarshal(inc.Tags, &tagsMap); err == nil {
					span.Tags = tagsMap
				}
			}
			spanList = append(spanList, span)
		}
		
		trace := reconstructTrace(spanList)
		json.NewEncoder(w).Encode(trace)
	})
	
	// List traces with filtering and pagination
	mux.HandleFunc("/api/traces", handleListTraces)
	
	// Get services overview
	mux.HandleFunc("/api/services", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		ctx, _ := ExtractTenantContext(r, queryClient)
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		
		// Build time filter - skip tenant filtering if "all" is selected
		var timeFilter string
		if ctx.IsAllTenants() {
			// No tenant filtering - show all data
			timeFilter = " WHERE 1=1"
		} else {
			// Handle NULL/empty tenant IDs by treating them as default tenant
			timeFilter = fmt.Sprintf(" WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		// Only apply time filter if explicitly provided
		if timeFrom != "" {
			timeFilter += fmt.Sprintf(" AND start_ts >= '%s'", escapeSQL(timeFrom))
		}
		if timeTo != "" {
			timeFilter += fmt.Sprintf(" AND start_ts <= '%s'", escapeSQL(timeTo))
		}
		
		// Global aggregation query for totals across all services
		globalQuery := `SELECT 
			count(DISTINCT trace_id) as total_traces,
			count(*) as total_spans,
			sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count,
			sum(cpu_ms) as total_cpu_ms,
			sum(bytes_sent) as total_bytes_sent,
			sum(bytes_received) as total_bytes_received,
			sum(http_requests_count) as total_http_requests,
			sum(CASE WHEN query_fingerprint IS NOT NULL AND query_fingerprint != '' THEN 1 ELSE 0 END) as total_sql_queries,
			avg(duration_ms) as avg_duration
			FROM opa.spans_min` + timeFilter
		
		globalRows, err := queryClient.Query(globalQuery)
		var globalTotals map[string]interface{}
		
		if err != nil {
			// Log error with context
			LogError(err, "Global aggregation query failed", map[string]interface{}{
				"endpoint": "/api/services",
				"query":    globalQuery,
			})
			// Return error to frontend instead of silently defaulting to zeros
			http.Error(w, fmt.Sprintf("Failed to fetch global statistics: %v", err), 500)
			return
		}
		
		// Aggregate queries should always return at least one row, even for empty tables
		if len(globalRows) == 0 {
			LogError(nil, "Global aggregation query returned no rows (unexpected for aggregate query)", map[string]interface{}{
				"endpoint": "/api/services",
				"query":    globalQuery,
			})
			// Default to zeros but log the issue
			globalTotals = map[string]interface{}{
				"total_traces":      0,
				"total_spans":       0,
				"error_count":       0,
				"total_cpu_ms":      0.0,
				"total_bytes_sent":  0,
				"total_bytes_received": 0,
				"total_http_requests": 0,
				"total_sql_queries":  0,
				"avg_duration":      0.0,
			}
		} else {
			globalRow := globalRows[0]
			
			// Validate required fields exist in response
			requiredFields := []string{"total_traces", "total_spans", "error_count", "total_cpu_ms", 
				"total_bytes_sent", "total_bytes_received", "total_http_requests", "total_sql_queries", "avg_duration"}
			missingFields := []string{}
			for _, field := range requiredFields {
				if _, exists := globalRow[field]; !exists {
					missingFields = append(missingFields, field)
				}
			}
			if len(missingFields) > 0 {
				LogError(nil, "Global query result missing required fields", map[string]interface{}{
					"endpoint":      "/api/services",
					"missing_fields": missingFields,
					"available_keys": getMapKeys(globalRow),
				})
			}
			
			globalTotals = map[string]interface{}{
				"total_traces":      getUint64(globalRow, "total_traces"),
				"total_spans":       getUint64(globalRow, "total_spans"),
				"error_count":       getUint64(globalRow, "error_count"),
				"total_cpu_ms":      getFloat64(globalRow, "total_cpu_ms"),
				"total_bytes_sent":  getUint64(globalRow, "total_bytes_sent"),
				"total_bytes_received": getUint64(globalRow, "total_bytes_received"),
				"total_http_requests": getUint64(globalRow, "total_http_requests"),
				"total_sql_queries":  getUint64(globalRow, "total_sql_queries"),
				"avg_duration":       getFloat64(globalRow, "avg_duration"),
			}
			
		}
		
		// Per-service query with enhanced metrics
		query := `SELECT service, 
			any(language) as language,
			any(language_version) as language_version,
			any(framework) as framework,
			any(framework_version) as framework_version,
			count(DISTINCT trace_id) as total_traces,
			count(*) as total_spans,
			sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count,
			sum(cpu_ms) as total_cpu_ms,
			sum(bytes_sent) as total_bytes_sent,
			sum(bytes_received) as total_bytes_received,
			sum(http_requests_count) as total_http_requests,
			count(DISTINCT query_fingerprint) as sql_query_count,
			avg(duration_ms) as avg_duration,
			quantile(0.95)(duration_ms) as p95_duration,
			quantile(0.99)(duration_ms) as p99_duration
			FROM opa.spans_min` + timeFilter + ` GROUP BY service ORDER BY total_traces DESC`
		
		rows, err := queryClient.Query(query)
		if err != nil {
			// Log error with context
			LogError(err, "Per-service aggregation query failed", map[string]interface{}{
				"endpoint": "/api/services",
				"query":    query,
			})
			http.Error(w, fmt.Sprintf("Failed to fetch service statistics: %v", err), 500)
			return
		}
		
		services := make([]map[string]interface{}, 0)
		for _, row := range rows {
			serviceName := getString(row, "service")
			totalTraces := getUint64(row, "total_traces")
			totalSpans := getUint64(row, "total_spans")
			errorCount := getUint64(row, "error_count")
			errorRate := 0.0
			if totalSpans > 0 {
				errorRate = float64(errorCount) / float64(totalSpans) * 100
			}
			
			// Get top SQL queries for this service
			var sqlQuery string
			if ctx.IsAllTenants() {
				sqlQuery = fmt.Sprintf(`SELECT query_fingerprint, count(*) as execution_count, avg(duration_ms) as avg_duration
					FROM opa.spans_min 
					WHERE service = '%s' AND query_fingerprint != ''`,
					escapeSQL(serviceName))
			} else {
				sqlQuery = fmt.Sprintf(`SELECT query_fingerprint, count(*) as execution_count, avg(duration_ms) as avg_duration
					FROM opa.spans_min 
					WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s') AND service = '%s' AND query_fingerprint != ''`,
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), escapeSQL(serviceName))
			}
			if timeFrom != "" {
				sqlQuery += fmt.Sprintf(" AND start_ts >= '%s'", escapeSQL(timeFrom))
			}
			if timeTo != "" {
				sqlQuery += fmt.Sprintf(" AND start_ts <= '%s'", escapeSQL(timeTo))
			}
			sqlQuery += " GROUP BY query_fingerprint ORDER BY execution_count DESC LIMIT 10"
			
			sqlRows, sqlErr := queryClient.Query(sqlQuery)
			if sqlErr != nil {
				LogError(sqlErr, "Failed to fetch top SQL queries for service", map[string]interface{}{
					"service": serviceName,
					"query":   sqlQuery,
				})
				// Continue without SQL queries rather than failing the whole request
			}
			topSqlQueries := make([]map[string]interface{}, 0)
			for _, sqlRow := range sqlRows {
				topSqlQueries = append(topSqlQueries, map[string]interface{}{
					"fingerprint":     getString(sqlRow, "query_fingerprint"),
					"execution_count": getUint64(sqlRow, "execution_count"),
					"avg_duration":     getFloat64(sqlRow, "avg_duration"),
				})
			}
			
			// Get top HTTP requests for this service (from spans_full)
			var httpQuery string
			if ctx.IsAllTenants() {
				httpQuery = fmt.Sprintf(`SELECT 
					coalesce(concat(url_scheme, '://', url_host, url_path), name) as endpoint,
					count(*) as request_count,
					avg(duration_ms) as avg_duration,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count
					FROM opa.spans_min
					WHERE service = '%s' AND (url_scheme IS NOT NULL OR url_host IS NOT NULL)`,
					escapeSQL(serviceName))
			} else {
				httpQuery = fmt.Sprintf(`SELECT 
					coalesce(concat(url_scheme, '://', url_host, url_path), name) as endpoint,
					count(*) as request_count,
					avg(duration_ms) as avg_duration,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count
					FROM opa.spans_min
					WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s') AND service = '%s' AND (url_scheme IS NOT NULL OR url_host IS NOT NULL)`,
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), escapeSQL(serviceName))
			}
			if timeFrom != "" {
				httpQuery += fmt.Sprintf(" AND start_ts >= '%s'", escapeSQL(timeFrom))
			}
			if timeTo != "" {
				httpQuery += fmt.Sprintf(" AND start_ts <= '%s'", escapeSQL(timeTo))
			}
			httpQuery += " GROUP BY coalesce(concat(url_scheme, '://', url_host, url_path), name) ORDER BY request_count DESC LIMIT 10"
			
			httpRows, httpErr := queryClient.Query(httpQuery)
			if httpErr != nil {
				LogError(httpErr, "Failed to fetch top HTTP requests for service", map[string]interface{}{
					"service": serviceName,
					"query":   httpQuery,
				})
				// Continue without HTTP requests rather than failing the whole request
			}
			topHttpRequests := make([]map[string]interface{}, 0)
			for _, httpRow := range httpRows {
				topHttpRequests = append(topHttpRequests, map[string]interface{}{
					"endpoint":      getString(httpRow, "endpoint"),
					"request_count": getUint64(httpRow, "request_count"),
					"avg_duration":  getFloat64(httpRow, "avg_duration"),
					"error_count":   getUint64(httpRow, "error_count"),
				})
			}
			
			service := map[string]interface{}{
				"service":              getString(row, "service"),
				"language":             getString(row, "language"),
				"language_version":     getStringPtr(row, "language_version"),
				"framework":            getStringPtr(row, "framework"),
				"framework_version":    getStringPtr(row, "framework_version"),
				"total_traces":         totalTraces,
				"total_spans":          totalSpans,
				"error_count":          errorCount,
				"error_rate":           errorRate,
				"total_cpu_ms":         getFloat64(row, "total_cpu_ms"),
				"total_bytes_sent":     getUint64(row, "total_bytes_sent"),
				"total_bytes_received": getUint64(row, "total_bytes_received"),
				"total_http_requests":   getUint64(row, "total_http_requests"),
				"sql_query_count":      getUint64(row, "sql_query_count"),
				"avg_duration":         getFloat64(row, "avg_duration"),
				"p95_duration":         getFloat64(row, "p95_duration"),
				"p99_duration":         getFloat64(row, "p99_duration"),
				"top_sql_queries":      topSqlQueries,
				"top_http_requests":    topHttpRequests,
			}
			services = append(services, service)
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"global_totals": globalTotals,
			"services":      services,
		})
	})
	
	// Service Map endpoint
	mux.HandleFunc("/api/service-map", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}

		ctx, _ := ExtractTenantContext(r, queryClient)
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		
		if timeFrom == "" {
			timeFrom = "now() - INTERVAL 24 HOUR"
		} else {
			timeFrom = fmt.Sprintf("'%s'", timeFrom)
		}
		if timeTo == "" {
			timeTo = "now()"
		} else {
			timeTo = fmt.Sprintf("'%s'", timeTo)
		}

		// First try to get from service_map_metadata (if populated)
		query := fmt.Sprintf(`SELECT 
			from_service,
			to_service,
			avg_latency_ms,
			error_rate,
			call_count,
			health_status
			FROM opa.service_map_metadata
			WHERE organization_id = '%s' AND project_id = '%s'
			ORDER BY from_service, to_service`,
			escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))

		rows, err := queryClient.Query(query)
		var nodes []map[string]interface{}
		var edges []map[string]interface{}
		services := make(map[string]bool)

		// If metadata table has data, use it
		if err == nil && len(rows) > 0 {
			for _, row := range rows {
				fromService := getString(row, "from_service")
				toService := getString(row, "to_service")
				services[fromService] = true
				services[toService] = true

				edges = append(edges, map[string]interface{}{
					"from":          fromService,
					"to":            toService,
					"avg_latency_ms": getFloat64(row, "avg_latency_ms"),
					"error_rate":     getFloat64(row, "error_rate"),
					"call_count":     getUint64(row, "call_count"),
					"health_status":  getString(row, "health_status"),
				})
			}
		} else {
			// Fallback: derive service dependencies from spans_min by analyzing parent-child relationships
			// Get all spans with their parent relationships
			depsQuery := fmt.Sprintf(`SELECT 
				parent.service as from_service,
				child.service as to_service,
				avg(child.duration_ms) as avg_latency_ms,
				sum(CASE WHEN child.status = 'error' OR child.status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate,
				count(*) as call_count
				FROM opa.spans_min as child
				INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id AND child.trace_id = parent.trace_id
				WHERE child.organization_id = '%s' AND child.project_id = '%s'
					AND parent.organization_id = '%s' AND parent.project_id = '%s'
					AND child.service != parent.service
					AND child.start_ts >= %s AND child.start_ts <= %s
					AND parent.start_ts >= %s AND parent.start_ts <= %s
				GROUP BY parent.service, child.service
				ORDER BY parent.service, child.service`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID),
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID),
				timeFrom, timeTo, timeFrom, timeTo)

			depsRows, depsErr := queryClient.Query(depsQuery)
			if depsErr == nil {
				for _, row := range depsRows {
					fromService := getString(row, "from_service")
					toService := getString(row, "to_service")
					if fromService == "" || toService == "" {
						continue
					}
					services[fromService] = true
					services[toService] = true

					avgLatency := getFloat64(row, "avg_latency_ms")
					errorRate := getFloat64(row, "error_rate")
					callCount := getUint64(row, "call_count")

					healthStatus := "healthy"
					if errorRate > 10.0 || avgLatency > 1000.0 {
						healthStatus = "degraded"
					}
					if errorRate > 50.0 {
						healthStatus = "down"
					}

					edges = append(edges, map[string]interface{}{
						"from":          fromService,
						"to":            toService,
						"avg_latency_ms": avgLatency,
						"error_rate":     errorRate,
						"call_count":     callCount,
						"health_status":  healthStatus,
					})
				}
			}
		}

		// If no edges found, at least show all services that exist
		if len(edges) == 0 {
			allServicesQuery := fmt.Sprintf(`SELECT DISTINCT service
				FROM opa.spans_min
				WHERE organization_id = '%s' AND project_id = '%s'
					AND start_ts >= %s AND start_ts <= %s`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), timeFrom, timeTo)
			
			serviceRows, _ := queryClient.Query(allServicesQuery)
			for _, row := range serviceRows {
				service := getString(row, "service")
				if service != "" {
					services[service] = true
				}
			}
		}

		// Create nodes for all services
		for service := range services {
			// Get service stats
			statsQuery := fmt.Sprintf(`SELECT 
				count(*) as total_spans,
				avg(duration_ms) as avg_duration,
				sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate
				FROM opa.spans_min 
				WHERE organization_id = '%s' AND project_id = '%s' AND service = '%s' AND start_ts >= %s AND start_ts <= %s`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), escapeSQL(service), timeFrom, timeTo)
			
			statsRows, _ := queryClient.Query(statsQuery)
			healthStatus := "healthy"
			if len(statsRows) > 0 {
				errorRate := getFloat64(statsRows[0], "error_rate")
				avgDuration := getFloat64(statsRows[0], "avg_duration")
				if errorRate > 10.0 || avgDuration > 1000.0 {
					healthStatus = "degraded"
				}
				if errorRate > 50.0 {
					healthStatus = "down"
				}
			}

			nodes = append(nodes, map[string]interface{}{
				"id":            service,
				"service":       service,
				"health_status": healthStatus,
			})
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"nodes": nodes,
			"edges": edges,
		})
	})
	
	// Key Transactions endpoints
	mux.HandleFunc("/api/key-transactions", func(w http.ResponseWriter, r *http.Request) {
		ctx, _ := ExtractTenantContext(r, queryClient)
		switch r.Method {
		case "GET":
			query := fmt.Sprintf(`SELECT transaction_id, name, service, pattern, description, enabled, created_at, updated_at 
				FROM opa.key_transactions 
				WHERE organization_id = '%s' AND project_id = '%s' 
				ORDER BY created_at DESC`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			var transactions []map[string]interface{}
			for _, row := range rows {
				transactions = append(transactions, map[string]interface{}{
					"transaction_id": getString(row, "transaction_id"),
					"name":           getString(row, "name"),
					"service":        getString(row, "service"),
					"pattern":        getString(row, "pattern"),
					"description":   getString(row, "description"),
					"enabled":        getUint64(row, "enabled") > 0,
					"created_at":     getString(row, "created_at"),
					"updated_at":     getString(row, "updated_at"),
				})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"transactions": transactions})
		case "POST":
			var tx struct {
				Name        string `json:"name"`
				Service     string `json:"service"`
				Pattern     string `json:"pattern"`
				Description string `json:"description"`
			}
			if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			txID := generateID()
			query := fmt.Sprintf(`INSERT INTO opa.key_transactions 
				(organization_id, project_id, transaction_id, name, service, pattern, description) 
				VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')`,
				escapeSQL(ctx.OrganizationID),
				escapeSQL(ctx.ProjectID),
				escapeSQL(txID),
				escapeSQL(tx.Name),
				escapeSQL(tx.Service),
				escapeSQL(tx.Pattern),
				escapeSQL(tx.Description))
			if err := queryClient.Execute(query); err != nil {
				LogError(err, "ClickHouse insert error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("insert error: %v", err), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"transaction_id": txID,
				"name":           tx.Name,
				"service":        tx.Service,
				"pattern":        tx.Pattern,
				"description":    tx.Description,
				"enabled":         true,
				"created_at":      time.Now().Format("2006-01-02 15:04:05"),
			})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	mux.HandleFunc("/api/key-transactions/", func(w http.ResponseWriter, r *http.Request) {
		ctx, _ := ExtractTenantContext(r, queryClient)
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 4 {
			http.Error(w, "bad request", 400)
			return
		}
		txID := parts[3]
		
		switch r.Method {
		case "GET":
			// Get transaction details with metrics
			query := fmt.Sprintf(`SELECT transaction_id, name, service, pattern, description, enabled, created_at, updated_at 
				FROM opa.key_transactions 
				WHERE organization_id = '%s' AND project_id = '%s' AND transaction_id = '%s' LIMIT 1`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), escapeSQL(txID))
			rows, err := queryClient.Query(query)
			if err != nil || len(rows) == 0 {
				http.Error(w, "transaction not found", 404)
				return
			}
			row := rows[0]
			
			// Get recent metrics
			metricsQuery := fmt.Sprintf(`SELECT 
				avg(avg_duration_ms) as avg_duration,
				quantile(0.50)(avg_duration_ms) as p50,
				quantile(0.95)(avg_duration_ms) as p95,
				quantile(0.99)(avg_duration_ms) as p99,
				avg(error_rate) as error_rate,
				sum(request_count) as total_requests,
				avg(apdex_score) as apdex
				FROM opa.key_transaction_metrics 
				WHERE organization_id = '%s' AND project_id = '%s' AND transaction_id = '%s' 
				AND date >= today() - INTERVAL 7 DAY`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), escapeSQL(txID))
			metricsRows, _ := queryClient.Query(metricsQuery)
			
			response := map[string]interface{}{
				"transaction_id": getString(row, "transaction_id"),
				"name":           getString(row, "name"),
				"service":        getString(row, "service"),
				"pattern":        getString(row, "pattern"),
				"description":    getString(row, "description"),
				"enabled":        getUint64(row, "enabled") > 0,
				"created_at":     getString(row, "created_at"),
				"updated_at":     getString(row, "updated_at"),
			}
			
			if len(metricsRows) > 0 {
				mRow := metricsRows[0]
				response["metrics"] = map[string]interface{}{
					"avg_duration":   getFloat64(mRow, "avg_duration"),
					"p50_duration":   getFloat64(mRow, "p50"),
					"p95_duration":   getFloat64(mRow, "p95"),
					"p99_duration":   getFloat64(mRow, "p99"),
					"error_rate":     getFloat64(mRow, "error_rate"),
					"total_requests": getUint64(mRow, "total_requests"),
					"apdex_score":    getFloat64(mRow, "apdex"),
				}
			}
			
			json.NewEncoder(w).Encode(response)
		case "PUT":
			var tx map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&tx); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			enabled := 1
			if v, ok := tx["enabled"].(bool); ok && !v {
				enabled = 0
			}
			query := fmt.Sprintf(`ALTER TABLE opa.key_transactions UPDATE 
				name = '%s', service = '%s', pattern = '%s', description = '%s', enabled = %d, updated_at = now()
				WHERE organization_id = '%s' AND project_id = '%s' AND transaction_id = '%s'`,
				escapeSQL(getStringFromMap(tx, "name")),
				escapeSQL(getStringFromMap(tx, "service")),
				escapeSQL(getStringFromMap(tx, "pattern")),
				escapeSQL(getStringFromMap(tx, "description")),
				enabled,
				escapeSQL(ctx.OrganizationID),
				escapeSQL(ctx.ProjectID),
				escapeSQL(txID))
			if err := queryClient.Execute(query); err != nil {
				http.Error(w, fmt.Sprintf("update error: %v", err), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
		case "DELETE":
			query := fmt.Sprintf(`ALTER TABLE opa.key_transactions DELETE 
				WHERE organization_id = '%s' AND project_id = '%s' AND transaction_id = '%s'`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), escapeSQL(txID))
			if err := queryClient.Execute(query); err != nil {
				http.Error(w, fmt.Sprintf("delete error: %v", err), 500)
				return
			}
			w.WriteHeader(204)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	// Get service stats
	mux.HandleFunc("/api/services/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 4 {
			http.Error(w, "bad request", 400)
			return
		}
		service := parts[3]
		
		if len(parts) >= 5 && parts[4] == "stats" {
			ctx, _ := ExtractTenantContext(r, queryClient)
			timeFrom := r.URL.Query().Get("from")
			timeTo := r.URL.Query().Get("to")
			
			// Service overview
			var query string
			if ctx.IsAllTenants() {
				query = fmt.Sprintf(`SELECT 
					count(DISTINCT trace_id) as total_traces,
					count(*) as total_spans,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count,
					avg(duration_ms) as avg_duration,
					quantile(0.50)(duration_ms) as p50_duration,
					quantile(0.95)(duration_ms) as p95_duration,
					quantile(0.99)(duration_ms) as p99_duration
					FROM opa.spans_min WHERE service = '%s'`,
					strings.ReplaceAll(service, "'", "''"))
			} else {
				query = fmt.Sprintf(`SELECT 
					count(DISTINCT trace_id) as total_traces,
					count(*) as total_spans,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count,
					avg(duration_ms) as avg_duration,
					quantile(0.50)(duration_ms) as p50_duration,
					quantile(0.95)(duration_ms) as p95_duration,
					quantile(0.99)(duration_ms) as p99_duration
					FROM opa.spans_min WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s') AND service = '%s'`,
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), strings.ReplaceAll(service, "'", "''"))
			}
			
			if timeFrom != "" {
				query += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
			}
			if timeTo != "" {
				query += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
			}
			
			rows, err := queryClient.Query(query)
			if err != nil || len(rows) == 0 {
				http.Error(w, "service not found", 404)
				return
			}
			
			row := rows[0]
			totalSpans := getUint64(row, "total_spans")
			errorCount := getUint64(row, "error_count")
			errorRate := 0.0
			if totalSpans > 0 {
				errorRate = float64(errorCount) / float64(totalSpans) * 100
			}
			
			// Top endpoints - merge URL components for display
			var endpointQuery string
			if ctx.IsAllTenants() {
				endpointQuery = fmt.Sprintf(`SELECT 
					coalesce(concat(url_scheme, '://', url_host, url_path), name) as name,
					count(*) as count,
					avg(duration_ms) as avg_duration,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count
					FROM opa.spans_min WHERE service = '%s'`,
					strings.ReplaceAll(service, "'", "''"))
			} else {
				endpointQuery = fmt.Sprintf(`SELECT 
					coalesce(concat(url_scheme, '://', url_host, url_path), name) as name,
					count(*) as count,
					avg(duration_ms) as avg_duration,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count
					FROM opa.spans_min WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s') AND service = '%s'`,
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), strings.ReplaceAll(service, "'", "''"))
			}
			if timeFrom != "" {
				endpointQuery += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
			}
			if timeTo != "" {
				endpointQuery += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
			}
			endpointQuery += " GROUP BY coalesce(concat(url_scheme, '://', url_host, url_path), name) ORDER BY count DESC LIMIT 10"
			
			endpointRows, _ := queryClient.Query(endpointQuery)
			var endpoints []map[string]interface{}
			for _, ep := range endpointRows {
				endpoints = append(endpoints, map[string]interface{}{
					"name":         getString(ep, "name"),
					"count":        getUint64(ep, "count"),
					"avg_duration": getFloat64(ep, "avg_duration"),
					"error_count":  getUint64(ep, "error_count"),
				})
			}
			
			json.NewEncoder(w).Encode(map[string]interface{}{
				"service":       service,
				"total_traces":  getUint64(row, "total_traces"),
				"total_spans":   totalSpans,
				"error_count":   errorCount,
				"error_rate":    errorRate,
				"avg_duration":  getFloat64(row, "avg_duration"),
				"p50_duration":  getFloat64(row, "p50_duration"),
				"p95_duration":  getFloat64(row, "p95_duration"),
				"p99_duration":  getFloat64(row, "p99_duration"),
				"top_endpoints": endpoints,
			})
			return
		}
		
		http.Error(w, "not found", 404)
	})
	
	// Get list of languages
	mux.HandleFunc("/api/languages", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		query := `SELECT DISTINCT language, 
			count(*) as span_count,
			count(DISTINCT service) as service_count
			FROM opa.spans_min 
			WHERE language != ''
			GROUP BY language 
			ORDER BY span_count DESC`
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var languages []map[string]interface{}
		for _, row := range rows {
			languages = append(languages, map[string]interface{}{
				"language":     getString(row, "language"),
				"span_count":   getUint64(row, "span_count"),
				"service_count": getUint64(row, "service_count"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"languages": languages,
		})
	})
	
	// Get list of frameworks by language
	mux.HandleFunc("/api/frameworks", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		language := r.URL.Query().Get("language")
		
		query := `SELECT DISTINCT framework, 
			any(framework_version) as framework_version,
			count(*) as span_count,
			count(DISTINCT service) as service_count
			FROM opa.spans_min 
			WHERE framework != '' AND framework IS NOT NULL`
		
		if language != "" {
			query += fmt.Sprintf(" AND language = '%s'", strings.ReplaceAll(language, "'", "''"))
		}
		
		query += " GROUP BY framework ORDER BY span_count DESC"
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var frameworks []map[string]interface{}
		for _, row := range rows {
			frameworks = append(frameworks, map[string]interface{}{
				"framework":        getString(row, "framework"),
				"framework_version": getStringPtr(row, "framework_version"),
				"span_count":      getUint64(row, "span_count"),
				"service_count":   getUint64(row, "service_count"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"frameworks": frameworks,
		})
	})
	
	// Get service metadata
	mux.HandleFunc("/api/services/metadata", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		service := r.URL.Query().Get("service")
		
		query := `SELECT service,
			any(language) as language,
			any(language_version) as language_version,
			any(framework) as framework,
			any(framework_version) as framework_version,
			count(*) as span_count
			FROM opa.spans_min WHERE 1=1`
		
		if service != "" {
			query += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
		}
		
		query += " GROUP BY service ORDER BY service"
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var services []map[string]interface{}
		for _, row := range rows {
			services = append(services, map[string]interface{}{
				"service":          getString(row, "service"),
				"language":         getString(row, "language"),
				"language_version": getStringPtr(row, "language_version"),
				"framework":        getStringPtr(row, "framework"),
				"framework_version": getStringPtr(row, "framework_version"),
				"span_count":       getUint64(row, "span_count"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"services": services,
		})
	})
	
	// Network metrics
	mux.HandleFunc("/api/metrics/network", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		interval := r.URL.Query().Get("interval")
		if interval == "" {
			interval = "1 HOUR"
		}
		
		query := `SELECT 
			toStartOfInterval(start_ts, INTERVAL ` + interval + `) as time,
			sum(bytes_sent) as bytes_sent,
			sum(bytes_received) as bytes_received,
			count(*) as request_count,
			avg(duration_ms) as avg_latency
			FROM opa.spans_min WHERE 1=1`
		
		if timeFrom != "" {
			query += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
		} else {
			query += " AND start_ts >= now() - INTERVAL 24 HOUR"
		}
		if timeTo != "" {
			query += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
		}
		
		query += " GROUP BY time ORDER BY time"
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var metrics []map[string]interface{}
		for _, row := range rows {
			metrics = append(metrics, map[string]interface{}{
				"time":           getString(row, "time"),
				"bytes_sent":     getUint64(row, "bytes_sent"),
				"bytes_received": getUint64(row, "bytes_received"),
				"request_count":  getUint64(row, "request_count"),
				"avg_latency":    getFloat64(row, "avg_latency"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"metrics": metrics,
		})
	})
	
	// Performance metrics
	mux.HandleFunc("/api/metrics/performance", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		interval := r.URL.Query().Get("interval")
		if interval == "" {
			interval = "1 HOUR"
		}
		
		query := `SELECT 
			toStartOfInterval(start_ts, INTERVAL ` + interval + `) as time,
			count(DISTINCT trace_id) as throughput,
			quantile(0.50)(duration_ms) as p50_duration,
			quantile(0.95)(duration_ms) as p95_duration,
			quantile(0.99)(duration_ms) as p99_duration,
			sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate
			FROM opa.spans_min WHERE 1=1`
		
		if timeFrom != "" {
			query += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
		} else {
			query += " AND start_ts >= now() - INTERVAL 24 HOUR"
		}
		if timeTo != "" {
			query += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
		}
		
		query += " GROUP BY time ORDER BY time"
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var metrics []map[string]interface{}
		for _, row := range rows {
			metrics = append(metrics, map[string]interface{}{
				"time":         getString(row, "time"),
				"throughput":   getUint64(row, "throughput"),
				"p50_duration": getFloat64(row, "p50_duration"),
				"p95_duration": getFloat64(row, "p95_duration"),
				"p99_duration": getFloat64(row, "p99_duration"),
				"error_rate":   getFloat64(row, "error_rate"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"metrics": metrics,
		})
	})
	
	// SQL queries list
	mux.HandleFunc("/api/sql/queries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		service := r.URL.Query().Get("service")
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		minDuration := r.URL.Query().Get("min_duration")
		limit := r.URL.Query().Get("limit")
		if limit == "" {
			limit = "100"
		}
		
		query := `SELECT 
			query_fingerprint as fingerprint,
			count(*) as execution_count,
			avg(duration_ms) as avg_duration,
			quantile(0.95)(duration_ms) as p95_duration,
			quantile(0.99)(duration_ms) as p99_duration,
			max(duration_ms) as max_duration
			FROM opa.spans_min WHERE query_fingerprint IS NOT NULL AND query_fingerprint != ''`
		
		if service != "" {
			query += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
		}
		if timeFrom != "" {
			query += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
		} else {
			query += " AND start_ts >= now() - INTERVAL 24 HOUR"
		}
		if timeTo != "" {
			query += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
		}
		
		query += " GROUP BY query_fingerprint"
		
		if minDuration != "" {
			query += fmt.Sprintf(" HAVING avg(duration_ms) >= %s", minDuration)
		}
		
		query += " ORDER BY execution_count DESC"
		limitInt, _ := strconv.Atoi(limit)
		query += fmt.Sprintf(" LIMIT %d", limitInt)
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var queries []map[string]interface{}
		for _, row := range rows {
			queries = append(queries, map[string]interface{}{
				"fingerprint":    getString(row, "fingerprint"),
				"execution_count": getUint64(row, "execution_count"),
				"avg_duration":   getFloat64(row, "avg_duration"),
				"p95_duration":   getFloat64(row, "p95_duration"),
				"p99_duration":   getFloat64(row, "p99_duration"),
				"max_duration":   getFloat64(row, "max_duration"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"queries": queries,
		})
	})
	
	// SQL query details
	mux.HandleFunc("/api/sql/queries/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 5 {
			http.Error(w, "bad request", 400)
			return
		}
		fingerprint := strings.Join(parts[4:], "/")
		fingerprintEscaped := strings.ReplaceAll(fingerprint, "'", "''")
		
		// Get query stats
		query := fmt.Sprintf(`SELECT 
			count(*) as total_executions,
			avg(duration_ms) as avg_duration,
			quantile(0.95)(duration_ms) as p95_duration,
			quantile(0.99)(duration_ms) as p99_duration,
			max(duration_ms) as max_duration
			FROM opa.spans_min WHERE query_fingerprint = '%s'`, fingerprintEscaped)
		
		rows, err := queryClient.Query(query)
		if err != nil || len(rows) == 0 {
			http.Error(w, "query not found", 404)
			return
		}
		
		row := rows[0]
		
		// Get example query from spans_full by matching trace_id with spans_min
		exampleQuery := fingerprint // Use fingerprint as default
		exampleRows, _ := queryClient.Query(fmt.Sprintf(`SELECT sql FROM opa.spans_full 
			WHERE trace_id IN (
				SELECT trace_id FROM opa.spans_min 
				WHERE query_fingerprint = '%s' LIMIT 1
			) AND sql != '' LIMIT 1`, fingerprintEscaped))
		if len(exampleRows) > 0 {
			sqlStr := getString(exampleRows[0], "sql")
			if sqlStr != "" {
				var sqlArray []interface{}
				if err := json.Unmarshal([]byte(sqlStr), &sqlArray); err == nil && len(sqlArray) > 0 {
					if sqlMap, ok := sqlArray[0].(map[string]interface{}); ok {
						if q, ok := sqlMap["query"].(string); ok {
							exampleQuery = q
						}
					}
				}
			}
		}
		
		// Performance trends
		trendsQuery := fmt.Sprintf(`SELECT 
			toStartOfHour(start_ts) as time,
			avg(duration_ms) as avg_duration,
			quantile(0.95)(duration_ms) as p95_duration
			FROM opa.spans_min WHERE query_fingerprint = '%s' 
			AND start_ts >= now() - INTERVAL 7 DAY
			GROUP BY time ORDER BY time`, fingerprintEscaped)
		
		trendRows, _ := queryClient.Query(trendsQuery)
		var trends []map[string]interface{}
		for _, tRow := range trendRows {
			trends = append(trends, map[string]interface{}{
				"time":         getString(tRow, "time"),
				"avg_duration": getFloat64(tRow, "avg_duration"),
				"p95_duration": getFloat64(tRow, "p95_duration"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"fingerprint":       fingerprint,
			"total_executions":  getUint64(row, "total_executions"),
			"avg_duration":      getFloat64(row, "avg_duration"),
			"p95_duration":      getFloat64(row, "p95_duration"),
			"p99_duration":      getFloat64(row, "p99_duration"),
			"max_duration":      getFloat64(row, "max_duration"),
			"example_query":     exampleQuery,
			"performance_trends": trends,
		})
	})
	
	// RUM metrics endpoint
	mux.HandleFunc("/api/rum/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		timeFrom := r.URL.Query().Get("from")
		if timeFrom == "" {
			timeFrom = time.Now().Add(-24 * time.Hour).Format("2006-01-02 15:04:05")
		}
		
		// Query RUM metrics
		query := fmt.Sprintf(`SELECT 
			avg(JSONExtractFloat(navigation_timing, 'total')) as avg_page_load_time,
			avg(JSONExtractFloat(navigation_timing, 'dom')) as avg_dom_ready_time,
			count(*) as total_page_views,
			sum(length(errors) > 2) as total_errors
			FROM opa.rum_events 
			WHERE occurred_at >= '%s'`, strings.ReplaceAll(timeFrom, "'", "''"))
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var metrics map[string]interface{}
		if len(rows) > 0 {
			row := rows[0]
			metrics = map[string]interface{}{
				"avg_page_load_time": getFloat64(row, "avg_page_load_time"),
				"avg_dom_ready_time": getFloat64(row, "avg_dom_ready_time"),
				"total_page_views":   getUint64(row, "total_page_views"),
				"total_errors":       getUint64(row, "total_errors"),
			}
		} else {
			metrics = map[string]interface{}{
				"avg_page_load_time": 0,
				"avg_dom_ready_time": 0,
				"total_page_views":   0,
				"total_errors":       0,
			}
		}
		
		// Get timeline data
		timelineQuery := fmt.Sprintf(`SELECT 
			toStartOfHour(occurred_at) as time,
			avg(JSONExtractFloat(navigation_timing, 'total')) as avg_load_time,
			quantile(0.95)(JSONExtractFloat(navigation_timing, 'total')) as p95_load_time
			FROM opa.rum_events 
			WHERE occurred_at >= '%s'
			GROUP BY time ORDER BY time`, strings.ReplaceAll(timeFrom, "'", "''"))
		
		timelineRows, _ := queryClient.Query(timelineQuery)
		var timeline []map[string]interface{}
		for _, tRow := range timelineRows {
			timeline = append(timeline, map[string]interface{}{
				"time":           getString(tRow, "time"),
				"avg_load_time":  getFloat64(tRow, "avg_load_time"),
				"p95_load_time":  getFloat64(tRow, "p95_load_time"),
			})
		}
		metrics["timeline"] = timeline
		
		json.NewEncoder(w).Encode(metrics)
	})
	
	// RUM endpoint - receive browser monitoring data
	mux.HandleFunc("/api/rum", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		var rumData map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&rumData); err != nil {
			http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
			return
		}
		
		// Extract data
		sessionID := getStringFromMap(rumData, "session_id")
		pageViewID := getStringFromMap(rumData, "page_view_id")
		pageURL := getStringFromMap(rumData, "page_url")
		userAgent := getStringFromMap(rumData, "user_agent")
		timestamp := getInt64FromMap(rumData, "timestamp")
		
		// Get navigation timing
		var navTiming map[string]interface{}
		if nt, ok := rumData["navigation_timing"].(map[string]interface{}); ok {
			navTiming = nt
		}
		
		// Get resource timing
		var resources []interface{}
		if res, ok := rumData["resource_timing"].([]interface{}); ok {
			resources = res
		}
		
		// Get AJAX requests
		var ajaxRequests []interface{}
		if ajax, ok := rumData["ajax_requests"].([]interface{}); ok {
			ajaxRequests = ajax
		}
		
		// Get errors
		var errors []interface{}
		if errs, ok := rumData["errors"].([]interface{}); ok {
			errors = errs
		}
		
		// Get viewport
		var viewport map[string]interface{}
		if vp, ok := rumData["viewport"].(map[string]interface{}); ok {
			viewport = vp
		}
		
		// Convert timestamp
		occurredAt := time.UnixMilli(timestamp)
		if timestamp == 0 {
			occurredAt = time.Now()
		}
		
		// Serialize complex fields
		navTimingJSON, _ := json.Marshal(navTiming)
		resourcesJSON, _ := json.Marshal(resources)
		ajaxJSON, _ := json.Marshal(ajaxRequests)
		errorsJSON, _ := json.Marshal(errors)
		viewportJSON, _ := json.Marshal(viewport)
		
		// Write to ClickHouse
		rumEvent := map[string]interface{}{
			"organization_id":  "default-org",
			"project_id":      "default-project",
			"session_id":      sessionID,
			"page_view_id":    pageViewID,
			"page_url":        pageURL,
			"user_agent":      userAgent,
			"navigation_timing": string(navTimingJSON),
			"resource_timing":   string(resourcesJSON),
			"ajax_requests":    string(ajaxJSON),
			"errors":           string(errorsJSON),
			"viewport":         string(viewportJSON),
			"occurred_at":      occurredAt.Format("2006-01-02 15:04:05"),
		}
		
		writer.AddRUM(rumEvent)
		
		w.WriteHeader(204) // No content
	})
	
	// Errors list
	mux.HandleFunc("/api/errors", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		service := r.URL.Query().Get("service")
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		limit := r.URL.Query().Get("limit")
		if limit == "" {
			limit = "100"
		}
		
		// Group errors by service and name
		query := `SELECT 
			service,
			name,
			count(*) as count,
			min(start_ts) as first_seen,
			max(start_ts) as last_seen
			FROM opa.spans_min WHERE (status = 'error' OR status = '0')`
		
		if service != "" {
			query += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
		}
		if timeFrom != "" {
			query += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
		} else {
			query += " AND start_ts >= now() - INTERVAL 24 HOUR"
		}
		if timeTo != "" {
			query += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
		}
		
		query += " GROUP BY service, name ORDER BY count DESC"
		limitInt, _ := strconv.Atoi(limit)
		query += fmt.Sprintf(" LIMIT %d", limitInt)
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var errors []map[string]interface{}
		for _, row := range rows {
			serviceName := getString(row, "service")
			errorName := getString(row, "name")
			errorId := fmt.Sprintf("%s:%s", serviceName, errorName)
			
			errors = append(errors, map[string]interface{}{
				"error_id":   errorId,
				"error_message": errorName,
				"service":    serviceName,
				"count":      getUint64(row, "count"),
				"first_seen": getString(row, "first_seen"),
				"last_seen":  getString(row, "last_seen"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"errors": errors,
		})
	})
	
	// GET /api/dumps - Get recent dumps from all traces
	mux.HandleFunc("/api/dumps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		limit := r.URL.Query().Get("limit")
		if limit == "" {
			limit = "100" // Smaller batch size for infinite scroll
		}
		limitInt, _ := strconv.Atoi(limit)
		if limitInt > 500 {
			limitInt = 500 // Cap at 500 per request
		}
		
		since := r.URL.Query().Get("since")
		service := r.URL.Query().Get("service")
		all := r.URL.Query().Get("all") // If "all" is set, fetch all historical dumps
		cursor := r.URL.Query().Get("cursor") // Timestamp cursor for pagination
		
		// Query spans_full for spans with dumps
		query := `SELECT 
			trace_id,
			span_id,
			service,
			name,
			start_ts,
			dumps
		FROM opa.spans_full 
		WHERE dumps != '' AND dumps != '[]' AND dumps != 'null'`
		
		if service != "" {
			query += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
		}
		
		// Cursor-based pagination: if cursor is provided, fetch dumps older than cursor
		if cursor != "" {
			// Parse cursor as timestamp (milliseconds since epoch)
			if cursorInt, err := strconv.ParseInt(cursor, 10, 64); err == nil {
				// Convert to ClickHouse datetime format
				cursorTime := time.UnixMilli(cursorInt).Format("2006-01-02 15:04:05.000")
				query += fmt.Sprintf(" AND start_ts < '%s'", cursorTime)
			}
		} else {
			// Only apply time restriction if "all" is not set and "since" is not provided
			if all == "" {
				if since != "" {
					query += fmt.Sprintf(" AND start_ts >= '%s'", since)
				} else {
					// Default to last 7 days for better coverage while still being reasonable
					query += " AND start_ts >= now() - INTERVAL 7 DAY"
				}
			} else if since != "" {
				// If "all" is set but "since" is also provided, use "since" as minimum
				query += fmt.Sprintf(" AND start_ts >= '%s'", since)
			}
			// If "all" is set and no "since", no time restriction (fetch all historical)
		}
		
		query += " ORDER BY start_ts DESC LIMIT " + strconv.Itoa(limitInt)
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var allDumps []map[string]interface{}
		dumpIDCounter := 0
		
		for _, row := range rows {
			traceID := getString(row, "trace_id")
			spanID := getString(row, "span_id")
			serviceName := getString(row, "service")
			spanName := getString(row, "name")
			startTS := getString(row, "start_ts")
			dumpsStr := getString(row, "dumps")
			
			if dumpsStr == "" || dumpsStr == "[]" || dumpsStr == "null" {
				continue
			}
			
			// Parse dumps JSON array
			var dumpsArray []interface{}
			if err := json.Unmarshal([]byte(dumpsStr), &dumpsArray); err != nil {
				LogError(err, "Failed to unmarshal dumps for span", map[string]interface{}{
					"span_id": spanID,
				})
				continue
			}
			
			// Flatten dumps into individual entries
			for _, dumpRaw := range dumpsArray {
				dumpIDCounter++
				dumpMap, ok := dumpRaw.(map[string]interface{})
				if !ok {
					continue
				}
				
				// Extract dump fields
				timestamp := int64(0)
				if ts, ok := dumpMap["timestamp"].(float64); ok {
					timestamp = int64(ts)
				} else if ts, ok := dumpMap["timestamp"].(int64); ok {
					timestamp = ts
				}
				
				file := ""
				if f, ok := dumpMap["file"].(string); ok {
					file = f
				}
				
				line := int64(0)
				if l, ok := dumpMap["line"].(float64); ok {
					line = int64(l)
				} else if l, ok := dumpMap["line"].(int64); ok {
					line = l
				}
				
				data := dumpMap["data"]
				text := ""
				if t, ok := dumpMap["text"].(string); ok {
					text = t
				}
				
				// Create unique ID for this dump entry
				dumpID := fmt.Sprintf("%s-%s-%d-%d", traceID, spanID, timestamp, dumpIDCounter)
				
				allDumps = append(allDumps, map[string]interface{}{
					"id":         dumpID,
					"trace_id":   traceID,
					"span_id":    spanID,
					"service":    serviceName,
					"span_name":  spanName,
					"timestamp":  timestamp,
					"file":       file,
					"line":       line,
					"data":       data,
					"text":       text,
					"span_start_ts": startTS,
				})
			}
		}
		
		// Sort by timestamp descending (newest first)
		sort.Slice(allDumps, func(i, j int) bool {
			tsI, _ := allDumps[i]["timestamp"].(int64)
			tsJ, _ := allDumps[j]["timestamp"].(int64)
			return tsI > tsJ
		})
		
		// Calculate next cursor (timestamp of the last dump, or 0 if no more)
		nextCursor := int64(0)
		hasMore := false
		if len(allDumps) > 0 {
			// Use the span_start_ts converted to timestamp for cursor
			// Or use the minimum timestamp from the dumps
			lastDump := allDumps[len(allDumps)-1]
			if spanStartTS, ok := lastDump["span_start_ts"].(string); ok {
				// Parse ClickHouse datetime format
				if t, err := time.Parse("2006-01-02 15:04:05.000", spanStartTS); err == nil {
					nextCursor = t.UnixMilli()
					hasMore = len(allDumps) == limitInt // If we got full batch, there might be more
				}
			}
			// Fallback to dump timestamp if span_start_ts parsing fails
			if nextCursor == 0 {
				if ts, ok := lastDump["timestamp"].(int64); ok && ts > 0 {
					nextCursor = ts
					hasMore = len(allDumps) == limitInt
				}
			}
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"dumps": allDumps,
			"total": len(allDumps),
			"has_more": hasMore,
			"next_cursor": nextCursor,
		})
	})
	
	// SLO endpoints
	mux.HandleFunc("/api/slos", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			query := "SELECT id, name, description, service, slo_type, target_value, window_hours, created_at, updated_at FROM opa.slos ORDER BY created_at DESC"
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			var slos []map[string]interface{}
			for _, row := range rows {
				slos = append(slos, map[string]interface{}{
					"id":           getString(row, "id"),
					"name":         getString(row, "name"),
					"description":  getString(row, "description"),
					"service":      getString(row, "service"),
					"slo_type":     getString(row, "slo_type"),
					"target_value": getFloat64(row, "target_value"),
					"window_hours": getUint64(row, "window_hours"),
					"created_at":    getString(row, "created_at"),
					"updated_at":    getString(row, "updated_at"),
				})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"slos": slos})
		case "POST":
			var slo map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&slo); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			sloID := fmt.Sprintf("slo-%d", time.Now().UnixNano())
			if id, ok := slo["id"].(string); ok && id != "" {
				sloID = id
			}
			query := fmt.Sprintf(`INSERT INTO opa.slos (id, name, description, service, slo_type, target_value, window_hours) VALUES ('%s', '%s', '%s', '%s', '%s', %f, %d)`,
				escapeSQL(sloID),
				escapeSQL(getStringFromMap(slo, "name")),
				escapeSQL(getStringFromMap(slo, "description")),
				escapeSQL(getStringFromMap(slo, "service")),
				escapeSQL(getStringFromMap(slo, "slo_type")),
				getFloat64FromMap(slo, "target_value"),
				getUint64FromMap(slo, "window_hours"),
			)
			if err := queryClient.Execute(query); err != nil {
				LogError(err, "ClickHouse insert error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("insert error: %v", err), 500)
				return
			}
			slo["id"] = sloID
			json.NewEncoder(w).Encode(slo)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	mux.HandleFunc("/api/slos/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 4 {
			http.Error(w, "bad request", 400)
			return
		}
		sloID := parts[3]
		
		if len(parts) >= 5 && parts[4] == "compliance" {
			// Get SLO compliance metrics
			query := fmt.Sprintf(`SELECT 
				actual_value, compliance_percentage, is_breach, window_start, window_end
				FROM opa.slo_metrics 
				WHERE slo_id = '%s' 
				ORDER BY window_start DESC LIMIT 30`,
				escapeSQL(sloID))
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			var metrics []map[string]interface{}
			for _, row := range rows {
				metrics = append(metrics, map[string]interface{}{
					"actual_value":          getFloat64(row, "actual_value"),
					"compliance_percentage": getFloat64(row, "compliance_percentage"),
					"is_breach":             getUint64(row, "is_breach") > 0,
					"window_start":          getString(row, "window_start"),
					"window_end":             getString(row, "window_end"),
				})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"metrics": metrics})
			return
		}
		
		switch r.Method {
		case "GET":
			query := fmt.Sprintf("SELECT * FROM opa.slos WHERE id = '%s'", escapeSQL(sloID))
			rows, err := queryClient.Query(query)
			if err != nil || len(rows) == 0 {
				http.Error(w, "slo not found", 404)
				return
			}
			row := rows[0]
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":           getString(row, "id"),
				"name":         getString(row, "name"),
				"description":  getString(row, "description"),
				"service":      getString(row, "service"),
				"slo_type":     getString(row, "slo_type"),
				"target_value": getFloat64(row, "target_value"),
				"window_hours": getUint64(row, "window_hours"),
			})
		case "PUT":
			var slo map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&slo); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			query := fmt.Sprintf(`ALTER TABLE opa.slos UPDATE 
				name = '%s', description = '%s', service = '%s', slo_type = '%s', 
				target_value = %f, window_hours = %d, updated_at = now()
				WHERE id = '%s'`,
				escapeSQL(getStringFromMap(slo, "name")),
				escapeSQL(getStringFromMap(slo, "description")),
				escapeSQL(getStringFromMap(slo, "service")),
				escapeSQL(getStringFromMap(slo, "slo_type")),
				getFloat64FromMap(slo, "target_value"),
				getUint64FromMap(slo, "window_hours"),
				escapeSQL(sloID),
			)
			if err := queryClient.Execute(query); err != nil {
				http.Error(w, fmt.Sprintf("update error: %v", err), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
		case "DELETE":
			query := fmt.Sprintf("ALTER TABLE opa.slos DELETE WHERE id = '%s'", escapeSQL(sloID))
			if err := queryClient.Execute(query); err != nil {
				http.Error(w, fmt.Sprintf("delete error: %v", err), 500)
				return
			}
			w.WriteHeader(204)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	// Dashboard endpoints
	mux.HandleFunc("/api/dashboards", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			userID := r.URL.Query().Get("user_id")
			query := "SELECT id, name, description, config, user_id, is_shared, created_at, updated_at FROM opa.dashboards WHERE 1=1"
			if userID != "" {
				query += fmt.Sprintf(" AND (user_id = '%s' OR is_shared = 1)", escapeSQL(userID))
			} else {
				query += " AND is_shared = 1"
			}
			query += " ORDER BY created_at DESC"
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			var dashboards []map[string]interface{}
			for _, row := range rows {
				var config map[string]interface{}
				configStr := getString(row, "config")
				json.Unmarshal([]byte(configStr), &config)
				dashboards = append(dashboards, map[string]interface{}{
					"id":          getString(row, "id"),
					"name":        getString(row, "name"),
					"description": getString(row, "description"),
					"config":      config,
					"user_id":     getStringPtr(row, "user_id"),
					"is_shared":   getUint64(row, "is_shared") > 0,
					"created_at":  getString(row, "created_at"),
					"updated_at":  getString(row, "updated_at"),
				})
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"dashboards": dashboards})
		case "POST":
			var dashboard map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&dashboard); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			dashboardID := fmt.Sprintf("dashboard-%d", time.Now().UnixNano())
			if id, ok := dashboard["id"].(string); ok && id != "" {
				dashboardID = id
			}
			configJSON, _ := json.Marshal(dashboard["config"])
			userID := getStringFromMap(dashboard, "user_id")
			isShared := 0
			if shared, ok := dashboard["is_shared"].(bool); ok && shared {
				isShared = 1
			}
			query := fmt.Sprintf(`INSERT INTO opa.dashboards (id, name, description, config, user_id, is_shared) VALUES ('%s', '%s', '%s', '%s', %s, %d)`,
				escapeSQL(dashboardID),
				escapeSQL(getStringFromMap(dashboard, "name")),
				escapeSQL(getStringFromMap(dashboard, "description")),
				escapeSQL(string(configJSON)),
				userID,
				isShared,
			)
			if err := queryClient.Execute(query); err != nil {
				LogError(err, "ClickHouse insert error", map[string]interface{}{
					"path": r.URL.Path,
					"method": r.Method,
				})
				http.Error(w, fmt.Sprintf("insert error: %v", err), 500)
				return
			}
			dashboard["id"] = dashboardID
			json.NewEncoder(w).Encode(dashboard)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	mux.HandleFunc("/api/dashboards/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 4 {
			http.Error(w, "bad request", 400)
			return
		}
		dashboardID := parts[3]
		
		switch r.Method {
		case "GET":
			query := fmt.Sprintf("SELECT * FROM opa.dashboards WHERE id = '%s'", escapeSQL(dashboardID))
			rows, err := queryClient.Query(query)
			if err != nil || len(rows) == 0 {
				http.Error(w, "dashboard not found", 404)
				return
			}
			row := rows[0]
			var config map[string]interface{}
			configStr := getString(row, "config")
			json.Unmarshal([]byte(configStr), &config)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":          getString(row, "id"),
				"name":        getString(row, "name"),
				"description": getString(row, "description"),
				"config":      config,
				"user_id":     getStringPtr(row, "user_id"),
				"is_shared":   getUint64(row, "is_shared") > 0,
			})
		case "PUT":
			var dashboard map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&dashboard); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			configJSON, _ := json.Marshal(dashboard["config"])
			isShared := 0
			if shared, ok := dashboard["is_shared"].(bool); ok && shared {
				isShared = 1
			}
			query := fmt.Sprintf(`ALTER TABLE opa.dashboards UPDATE 
				name = '%s', description = '%s', config = '%s', is_shared = %d, updated_at = now()
				WHERE id = '%s'`,
				escapeSQL(getStringFromMap(dashboard, "name")),
				escapeSQL(getStringFromMap(dashboard, "description")),
				escapeSQL(string(configJSON)),
				isShared,
				escapeSQL(dashboardID),
			)
			if err := queryClient.Execute(query); err != nil {
				http.Error(w, fmt.Sprintf("update error: %v", err), 500)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
		case "DELETE":
			query := fmt.Sprintf("ALTER TABLE opa.dashboards DELETE WHERE id = '%s'", escapeSQL(dashboardID))
			if err := queryClient.Execute(query); err != nil {
				http.Error(w, fmt.Sprintf("delete error: %v", err), 500)
				return
			}
			w.WriteHeader(204)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	// Alert endpoints
	mux.HandleFunc("/api/alerts", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			alerts := alertWorker.ListAlerts()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"alerts": alerts,
			})
		case "POST":
			var alert Alert
			if err := json.NewDecoder(r.Body).Decode(&alert); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			if alert.ID == "" {
				alert.ID = fmt.Sprintf("alert-%d", time.Now().UnixNano())
			}
			alertWorker.AddAlert(&alert)
			json.NewEncoder(w).Encode(alert)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	mux.HandleFunc("/api/alerts/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 4 {
			http.Error(w, "bad request", 400)
			return
		}
		alertID := parts[3]
		
		switch r.Method {
		case "GET":
			alert := alertWorker.GetAlert(alertID)
			if alert == nil {
				http.Error(w, "alert not found", 404)
				return
			}
			json.NewEncoder(w).Encode(alert)
		case "PUT":
			var alert Alert
			if err := json.NewDecoder(r.Body).Decode(&alert); err != nil {
				http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
				return
			}
			alert.ID = alertID
			alertWorker.AddAlert(&alert)
			json.NewEncoder(w).Encode(alert)
		case "DELETE":
			alertWorker.RemoveAlert(alertID)
			w.WriteHeader(204)
		case "POST":
			// Test alert
			alert := alertWorker.GetAlert(alertID)
			if alert == nil {
				http.Error(w, "alert not found", 404)
				return
			}
			// Manually trigger check
			go func() {
				alertWorker.checkAlert(alert)
			}()
			json.NewEncoder(w).Encode(map[string]string{"status": "checking"})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	
	// Error details
	mux.HandleFunc("/api/errors/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 4 {
			http.Error(w, "bad request", 400)
			return
		}
		errorId := strings.Join(parts[3:], "/")
		
		// Parse error ID (format: service:name)
		parts = strings.SplitN(errorId, ":", 2)
		if len(parts) != 2 {
			http.Error(w, "invalid error ID", 400)
			return
		}
		serviceName := strings.ReplaceAll(parts[0], "'", "''")
		errorName := strings.ReplaceAll(parts[1], "'", "''")
		
		// Get error stats
		query := fmt.Sprintf(`SELECT 
			count(*) as count,
			min(start_ts) as first_seen,
			max(start_ts) as last_seen
			FROM opa.spans_min WHERE (status = 'error' OR status = '0')
			AND service = '%s' AND name = '%s'`, serviceName, errorName)
		
		rows, err := queryClient.Query(query)
		if err != nil || len(rows) == 0 {
			http.Error(w, "error not found", 404)
			return
		}
		
		row := rows[0]
		
		// Get stack trace from spans_full
		stackTrace := []interface{}{}
		stackRows, _ := queryClient.Query(fmt.Sprintf(`SELECT stack FROM opa.spans_full 
			WHERE service = '%s' AND name = '%s' AND stack != '' LIMIT 1`, serviceName, errorName))
		if len(stackRows) > 0 {
			stackStr := getString(stackRows[0], "stack")
			if stackStr != "" {
				json.Unmarshal([]byte(stackStr), &stackTrace)
			}
		}
		
		// Get related traces
		traceQuery := fmt.Sprintf(`SELECT DISTINCT trace_id, min(start_ts) as start_ts, 
			sum(duration_ms) as duration_ms
			FROM opa.spans_min WHERE (status = 'error' OR status = '0')
			AND service = '%s' AND name = '%s'
			GROUP BY trace_id ORDER BY start_ts DESC LIMIT 10`, serviceName, errorName)
		
		traceRows, _ := queryClient.Query(traceQuery)
		var relatedTraces []map[string]interface{}
		for _, tRow := range traceRows {
			relatedTraces = append(relatedTraces, map[string]interface{}{
				"trace_id":   getString(tRow, "trace_id"),
				"start_ts":   getString(tRow, "start_ts"),
				"duration_ms": getFloat64(tRow, "duration_ms"),
			})
		}
		
		// Error trends
		trendsQuery := fmt.Sprintf(`SELECT 
			toStartOfHour(start_ts) as time,
			count(*) as count
			FROM opa.spans_min WHERE (status = 'error' OR status = '0')
			AND service = '%s' AND name = '%s'
			AND start_ts >= now() - INTERVAL 7 DAY
			GROUP BY time ORDER BY time`, serviceName, errorName)
		
		trendRows, _ := queryClient.Query(trendsQuery)
		var trends []map[string]interface{}
		for _, tRow := range trendRows {
			trends = append(trends, map[string]interface{}{
				"time":  getString(tRow, "time"),
				"count": getUint64(tRow, "count"),
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"error_id":      errorId,
			"error_message": errorName,
			"service":       parts[0],
			"count":         getUint64(row, "count"),
			"first_seen":    getString(row, "first_seen"),
			"last_seen":     getString(row, "last_seen"),
			"stack_trace":   stackTrace,
			"related_traces": relatedTraces,
			"trends":        trends,
		})
	})
	
	// Keep trace
	mux.HandleFunc("/api/control/keep", func(w http.ResponseWriter, r *http.Request) {
		type req struct{ TraceID string `json:"trace_id"` }
		var rr req
		if err := json.NewDecoder(r.Body).Decode(&rr); err != nil || rr.TraceID == "" {
			http.Error(w, "bad request", 400)
			return
		}
		tb.MarkKeep(rr.TraceID)
		w.WriteHeader(200)
	})
	
	// Sampling control
	mux.HandleFunc("/api/control/sampling", func(w http.ResponseWriter, r *http.Request) {
		type req struct{ Rate float64 `json:"rate"` }
		var rr req
		if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		atomic.StoreUint64(&currentSamplingRate, uint64(rr.Rate*1000))
		w.WriteHeader(200)
	})
	
	// Purge all traces
	mux.HandleFunc("/api/control/purge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" && r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		LogInfo("Purging all traces", nil)
		
		// Clear in-memory buffer
		tb.ClearAll()
		
		// Delete all data from ClickHouse tables
		errors := make([]string, 0)
		
		// Delete from spans_min
		if err := queryClient.Execute("ALTER TABLE opa.spans_min DELETE WHERE 1=1"); err != nil {
			LogError(err, "Error purging spans_min", nil)
			errors = append(errors, fmt.Sprintf("spans_min: %v", err))
		}
		
		// Delete from spans_full
		if err := queryClient.Execute("ALTER TABLE opa.spans_full DELETE WHERE 1=1"); err != nil {
			LogError(err, "Error purging spans_full", nil)
			errors = append(errors, fmt.Sprintf("spans_full: %v", err))
		}
		
		// Delete from traces_full
		if err := queryClient.Execute("ALTER TABLE opa.traces_full DELETE WHERE 1=1"); err != nil {
			LogError(err, "Error purging traces_full", nil)
			errors = append(errors, fmt.Sprintf("traces_full: %v", err))
		}
		
		// Delete from network_metrics
		if err := queryClient.Execute("ALTER TABLE opa.network_metrics DELETE WHERE 1=1"); err != nil {
			LogError(err, "Error purging network_metrics", nil)
			errors = append(errors, fmt.Sprintf("network_metrics: %v", err))
		}
		
		if len(errors) > 0 {
			http.Error(w, fmt.Sprintf("partial purge completed with errors: %v", errors), 500)
			return
		}
		
		LogInfo("All traces purged successfully", nil)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "purged",
			"message": "All traces have been purged from all tables",
		})
	})
	
	// Export endpoints
	mux.HandleFunc("/api/export/traces", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		format := r.URL.Query().Get("format")
		if format == "" {
			format = "json"
		}
		
		// Use same filters as /api/traces
		service := r.URL.Query().Get("service")
		status := r.URL.Query().Get("status")
		language := r.URL.Query().Get("language")
		framework := r.URL.Query().Get("framework")
		version := r.URL.Query().Get("version")
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		
		// Build query similar to handleListTraces but without pagination
		query := "SELECT trace_id, service, min(start_ts) as start_ts, max(end_ts) as end_ts, "
		query += "sum(duration_ms) as duration_ms, count(*) as span_count, "
		query += "any(status) as status FROM ("
		query += "SELECT trace_id, service, start_ts, end_ts, duration_ms, status "
		query += "FROM opa.spans_min WHERE 1=1"
		
		if service != "" {
			query += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
		}
		if status != "" && status != "all" {
			query += fmt.Sprintf(" AND status = '%s'", strings.ReplaceAll(status, "'", "''"))
		}
		if language != "" {
			query += fmt.Sprintf(" AND language = '%s'", strings.ReplaceAll(language, "'", "''"))
		}
		if framework != "" {
			query += fmt.Sprintf(" AND framework = '%s'", strings.ReplaceAll(framework, "'", "''"))
		}
		if version != "" {
			query += fmt.Sprintf(" AND language_version = '%s'", strings.ReplaceAll(version, "'", "''"))
		}
		if timeFrom != "" {
			query += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
		} else {
			query += " AND start_ts >= now() - INTERVAL 24 HOUR"
		}
		if timeTo != "" {
			query += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
		}
		
		query += ") GROUP BY trace_id, service ORDER BY start_ts DESC"
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		w.Header().Set("Content-Type", "application/octet-stream")
		filename := fmt.Sprintf("traces_export_%s.%s", time.Now().Format("20060102_150405"), format)
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", filename))
		
		switch format {
		case "csv":
			w.Header().Set("Content-Type", "text/csv")
			w.Write([]byte("trace_id,service,start_ts,end_ts,duration_ms,span_count,status\n"))
			for _, row := range rows {
				w.Write([]byte(fmt.Sprintf("%s,%s,%s,%s,%.2f,%d,%s\n",
					getString(row, "trace_id"),
					getString(row, "service"),
					getString(row, "start_ts"),
					getString(row, "end_ts"),
					getFloat64(row, "duration_ms"),
					getUint64(row, "span_count"),
					getString(row, "status"),
				)))
			}
		case "ndjson":
			w.Header().Set("Content-Type", "application/x-ndjson")
			for _, row := range rows {
				json.NewEncoder(w).Encode(row)
				w.Write([]byte("\n"))
			}
		default: // json
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"traces": rows,
				"total":  len(rows),
			})
		}
	})
	
	// WebSocket hub for real-time updates
	wsHub := NewWebSocketHub()
	go wsHub.Run()
	
	// Add WebSocket endpoint to main API
	mux.HandleFunc("/ws", handleWebSocket(wsHub))
	
	// Start separate WebSocket server on dedicated port
	go func() {
		wsMux := http.NewServeMux()
		wsMux.HandleFunc("/ws", handleWebSocket(wsHub))
		LogInfo("Starting WebSocket server", map[string]interface{}{
			"address": *wsAddr,
		})
		log.Fatal(http.ListenAndServe(*wsAddr, wsMux))
	}()
	
	// Wrap mux with logging middleware
	loggedMux := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		
		// Wrap response writer to capture status code
		lrw := &logResponseWriter{
			ResponseWriter: w,
			statusCode:     200, // Default status code
		}
		
		// Handle panics
		defer func() {
			if err := recover(); err != nil {
				LogError(nil, "HTTP handler panic", map[string]interface{}{
					"path":       r.URL.Path,
					"method":     r.Method,
					"remote_addr": r.RemoteAddr,
					"panic":      err,
				})
				if lrw.statusCode == 200 {
					lrw.statusCode = 500
				}
				http.Error(w, "Internal server error", 500)
			}
			
			// Log the response
			duration := time.Since(start)
			LogHTTPResponse(lrw.statusCode, r.Method, r.URL.Path, duration, map[string]interface{}{
				"remote_addr": r.RemoteAddr,
				"user_agent":  r.UserAgent(),
				"size":         lrw.size,
			})
		}()
		
		// Call the mux handler
		mux.ServeHTTP(lrw, r)
	})
	
	go func() {
		LogInfo("Starting admin API server", map[string]interface{}{
			"address": *apiAddr,
		})
		log.Fatal(http.ListenAndServe(*apiAddr, loggedMux))
	}()
	
	// Setup Unix socket listener
	var unixListener net.Listener
	if *socketPath != "" {
		// Setup socket - remove if exists, retry if busy
		for i := 0; i < 5; i++ {
			if _, err := os.Stat(*socketPath); err == nil {
				if err := os.Remove(*socketPath); err != nil {
					if i < 4 {
						LogWarn("Socket busy, retrying", map[string]interface{}{
							"attempt": i+1,
							"max_attempts": 5,
						})
						time.Sleep(1 * time.Second)
						continue
					} else {
						LogError(err, "CRITICAL: Could not remove existing socket after 5 attempts", nil)
						log.Fatalf("could not remove existing socket after 5 attempts: %v", err)
					}
				}
			}
			break
		}
		// Ensure directory exists
		dir := "/var/run"
		if lastSlash := strings.LastIndex(*socketPath, "/"); lastSlash > 0 {
			dir = (*socketPath)[:lastSlash]
		}
		if err := os.MkdirAll(dir, 0755); err != nil {
			LogError(err, "CRITICAL: Could not create socket directory", nil)
			log.Fatalf("could not create socket directory %s: %v", dir, err)
		}
		l, err := net.Listen("unix", *socketPath)
		if err != nil {
			LogError(err, "CRITICAL: Failed to listen on Unix socket", nil)
			log.Fatalf("listen unix: %v", err)
		}
		unixListener = l
		defer unixListener.Close()
		// Set socket permissions so PHP-FPM can connect
		if err := os.Chmod(*socketPath, 0666); err != nil {
			LogWarn("Could not set socket permissions", map[string]interface{}{
				"error": err.Error(),
			})
		}
		LogInfo("Listening on Unix socket", map[string]interface{}{
			"socket_path": *socketPath,
		})
	}
	
	// Setup TCP listener
	var tcpListener net.Listener
	if *tcpAddr != "" {
		l, err := net.Listen("tcp", *tcpAddr)
		if err != nil {
			LogError(err, "CRITICAL: Failed to listen on TCP", nil)
			log.Fatalf("listen tcp: %v", err)
		}
		tcpListener = l
		defer tcpListener.Close()
		LogInfo("Listening on TCP", map[string]interface{}{
			"address": *tcpAddr,
		})
	}
	
	// Ensure at least one listener is configured
	if unixListener == nil && tcpListener == nil {
		LogError(nil, "CRITICAL: No transport configured. Set -socket or -tcp flag, or TRANSPORT_TCP/SOCKET_PATH environment variables", nil)
		log.Fatal("no transport configured: set -socket or -tcp flag, or TRANSPORT_TCP/SOCKET_PATH environment variables")
	}
	
	tb = NewTailBuffer(2000, 30*time.Second)
	writer = NewClickHouseWriter(*clickhouseURL, *batchSize)
	queryClient = NewClickHouseQuery(*clickhouseURL)
	breaker = NewCircuitBreaker(10, 30*time.Second)
	
	// Initialize alert worker
	alertWorker = NewAlertWorker(queryClient, 1*time.Minute)
	alertWorker.Start()
	
	// Initialize anomaly detector
	anomalyDetector := NewAnomalyDetector(queryClient, 100)
	
	// Initialize log correlation
	logCorrelation = NewLogCorrelation(queryClient)
	
	// Anomaly detection endpoints
	mux.HandleFunc("/api/anomalies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		service := r.URL.Query().Get("service")
		severity := r.URL.Query().Get("severity")
		timeFrom := r.URL.Query().Get("from")
		
		query := "SELECT id, type, service, metric, value, expected, score, severity, detected_at, metadata FROM opa.anomalies WHERE 1=1"
		
		if service != "" {
			query += fmt.Sprintf(" AND service = '%s'", escapeSQL(service))
		}
		if severity != "" {
			query += fmt.Sprintf(" AND severity = '%s'", escapeSQL(severity))
		}
		if timeFrom != "" {
			query += fmt.Sprintf(" AND detected_at >= '%s'", timeFrom)
		} else {
			query += " AND detected_at >= now() - INTERVAL 24 HOUR"
		}
		
		query += " ORDER BY detected_at DESC LIMIT 100"
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var anomalies []map[string]interface{}
		for _, row := range rows {
			var metadata map[string]interface{}
			metadataStr := getString(row, "metadata")
			json.Unmarshal([]byte(metadataStr), &metadata)
			
			anomalies = append(anomalies, map[string]interface{}{
				"id":         getString(row, "id"),
				"type":       getString(row, "type"),
				"service":    getString(row, "service"),
				"metric":     getString(row, "metric"),
				"value":      getFloat64(row, "value"),
				"expected":   getFloat64(row, "expected"),
				"score":      getFloat64(row, "score"),
				"severity":   getString(row, "severity"),
				"detected_at": getString(row, "detected_at"),
				"metadata":   metadata,
			})
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{"anomalies": anomalies})
	})
	
	mux.HandleFunc("/api/anomalies/analyze", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		var req struct {
			Service    string `json:"service"`
			TimeWindow string `json:"time_window"` // e.g., "1h", "24h"
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
			return
		}
		
		timeWindow := 24 * time.Hour
		if req.TimeWindow != "" {
			switch req.TimeWindow {
			case "1h":
				timeWindow = 1 * time.Hour
			case "6h":
				timeWindow = 6 * time.Hour
			case "24h":
				timeWindow = 24 * time.Hour
			case "7d":
				timeWindow = 7 * 24 * time.Hour
			}
		}
		
		anomalies, err := anomalyDetector.DetectAnomalies(req.Service, timeWindow)
		if err != nil {
			http.Error(w, fmt.Sprintf("detection error: %v", err), 500)
			return
		}
		
		// Store detected anomalies
		for _, anomaly := range anomalies {
			anomalyDetector.StoreAnomaly(anomaly)
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"anomalies": anomalies,
			"count":     len(anomalies),
		})
	})
	
	// Periodic flush
	go func() {
		t := time.NewTicker(time.Duration(*batchInterval) * time.Millisecond)
		for range t.C {
			writer.Flush()
		}
	}()
	
	// Background job: Extract service dependencies from trace data
	go func() {
		t := time.NewTicker(5 * time.Minute) // Run every 5 minutes
		for range t.C {
			// Extract service dependencies from spans_min by analyzing parent-child relationships
			// This is a simplified approach - in production, you'd want to process this more efficiently
			query := `SELECT 
				parent.service as from_service,
				child.service as to_service,
				count(*) as call_count,
				avg(child.duration_ms) as avg_duration_ms,
				max(child.duration_ms) as max_duration_ms,
				sum(CASE WHEN child.status = 'error' OR child.status = '0' THEN 1 ELSE 0 END) as error_count
				FROM opa.spans_min as child
				INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id
				WHERE child.organization_id = parent.organization_id 
				AND child.project_id = parent.project_id
				AND child.service != parent.service
				AND child.start_ts >= now() - INTERVAL 1 HOUR
				GROUP BY parent.service, child.service, child.organization_id, child.project_id`
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to extract service dependencies", nil)
				continue
			}
			
			for _, row := range rows {
				orgID := getString(row, "organization_id")
				projID := getString(row, "project_id")
				fromService := getString(row, "from_service")
				toService := getString(row, "to_service")
				callCount := getUint64(row, "call_count")
				avgDuration := getFloat64(row, "avg_duration_ms")
				errorCount := getUint64(row, "error_count")
				errorRate := float64(errorCount) / float64(callCount) * 100.0
				
				healthStatus := "healthy"
				if errorRate > 10.0 || avgDuration > 1000.0 {
					healthStatus = "degraded"
				}
				if errorRate > 50.0 {
					healthStatus = "down"
				}
				
				// Update service_map_metadata
				updateQuery := fmt.Sprintf(`INSERT INTO opa.service_map_metadata 
					(organization_id, project_id, from_service, to_service, last_seen, avg_latency_ms, error_rate, call_count, health_status)
					VALUES ('%s', '%s', '%s', '%s', now(), %.2f, %.2f, %d, '%s')`,
					escapeSQL(orgID),
					escapeSQL(projID),
					escapeSQL(fromService),
					escapeSQL(toService),
					avgDuration,
					errorRate,
					callCount,
					healthStatus)
				
				queryClient.Execute(updateQuery)
			}
		}
	}()
	
	// Background job: Aggregate key transaction metrics
	go func() {
		t := time.NewTicker(5 * time.Minute) // Run every 5 minutes
		for range t.C {
			// Get all enabled key transactions
			query := "SELECT transaction_id, service, pattern FROM opa.key_transactions WHERE enabled = 1"
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to get key transactions", nil)
				continue
			}
			
			for _, row := range rows {
				txID := getString(row, "transaction_id")
				service := getString(row, "service")
				pattern := getString(row, "pattern")
				
				// Match spans against pattern and aggregate metrics
				// Pattern can be: service:name, URL pattern, or service only
				whereClause := fmt.Sprintf("service = '%s'", escapeSQL(service))
				if pattern != "" && pattern != service {
					// If pattern contains :, it's service:name format
					if strings.Contains(pattern, ":") {
						parts := strings.SplitN(pattern, ":", 2)
						whereClause += fmt.Sprintf(" AND service = '%s' AND name = '%s'", 
							escapeSQL(parts[0]), escapeSQL(parts[1]))
					} else if strings.Contains(pattern, "/") {
						// URL pattern
						whereClause += fmt.Sprintf(" AND url_path LIKE '%s'", escapeSQL(pattern))
					} else {
						// Service name pattern
						whereClause += fmt.Sprintf(" AND name = '%s'", escapeSQL(pattern))
					}
				}
				
				// Aggregate metrics for last hour
				metricsQuery := fmt.Sprintf(`SELECT 
					organization_id,
					project_id,
					count(*) as request_count,
					sum(duration_ms) as total_duration_ms,
					avg(duration_ms) as avg_duration_ms,
					quantile(0.50)(duration_ms) as p50_duration_ms,
					quantile(0.95)(duration_ms) as p95_duration_ms,
					quantile(0.99)(duration_ms) as p99_duration_ms,
					max(duration_ms) as max_duration_ms,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) as error_count
					FROM opa.spans_min 
					WHERE %s AND start_ts >= now() - INTERVAL 1 HOUR
					GROUP BY organization_id, project_id`,
					whereClause)
				
				metricsRows, err := queryClient.Query(metricsQuery)
				if err != nil {
					LogError(err, "Failed to aggregate metrics for transaction", map[string]interface{}{
						"transaction_id": txID,
					})
					continue
				}
				
				for _, mRow := range metricsRows {
					orgID := getString(mRow, "organization_id")
					projID := getString(mRow, "project_id")
					requestCount := getUint64(mRow, "request_count")
					errorCount := getUint64(mRow, "error_count")
					errorRate := float64(errorCount) / float64(requestCount) * 100.0
					avgDuration := getFloat64(mRow, "avg_duration_ms")
					
					// Calculate Apdex (simplified: satisfied < 500ms, tolerating < 2000ms)
					apdexQuery := fmt.Sprintf(`SELECT 
						count(*) as total,
						sum(CASE WHEN duration_ms < 500 THEN 1 ELSE 0 END) as satisfied,
						sum(CASE WHEN duration_ms >= 500 AND duration_ms < 2000 THEN 1 ELSE 0 END) as tolerating
						FROM opa.spans_min 
						WHERE %s AND organization_id = '%s' AND project_id = '%s' AND start_ts >= now() - INTERVAL 1 HOUR`,
						whereClause, escapeSQL(orgID), escapeSQL(projID))
					apdexRows, _ := queryClient.Query(apdexQuery)
					apdexScore := 0.0
					if len(apdexRows) > 0 {
						total := getUint64(apdexRows[0], "total")
						satisfied := getUint64(apdexRows[0], "satisfied")
						tolerating := getUint64(apdexRows[0], "tolerating")
						if total > 0 {
							apdexScore = (float64(satisfied) + float64(tolerating)/2.0) / float64(total)
						}
					}
					
					now := time.Now()
					date := now.Format("2006-01-02")
					hour := now.Format("2006-01-02 15:00:00")
					throughputRPM := float64(requestCount) / 60.0 // Requests per minute
					
					insertQuery := fmt.Sprintf(`INSERT INTO opa.key_transaction_metrics 
						(organization_id, project_id, transaction_id, date, hour, request_count, total_duration_ms,
						avg_duration_ms, p50_duration_ms, p95_duration_ms, p99_duration_ms, max_duration_ms,
						error_count, error_rate, throughput_rpm, apdex_score)
						VALUES ('%s', '%s', '%s', '%s', '%s', %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %d, %.2f, %.2f, %.2f)`,
						escapeSQL(orgID),
						escapeSQL(projID),
						escapeSQL(txID),
						date,
						hour,
						requestCount,
						getFloat64(mRow, "total_duration_ms"),
						avgDuration,
						getFloat64(mRow, "p50_duration_ms"),
						getFloat64(mRow, "p95_duration_ms"),
						getFloat64(mRow, "p99_duration_ms"),
						getFloat64(mRow, "max_duration_ms"),
						errorCount,
						errorRate,
						throughputRPM,
						apdexScore)
					
					queryClient.Execute(insertQuery)
				}
			}
		}
	}()
	
	// Background job: Calculate and store Apdex scores
	go func() {
		t := time.NewTicker(5 * time.Minute) // Run every 5 minutes
		for range t.C {
			// Calculate Apdex scores from spans_min for last hour
			query := `SELECT 
				organization_id,
				project_id,
				service,
				coalesce(url_path, name) as endpoint,
				sum(CASE WHEN duration_ms < 500 THEN 1 ELSE 0 END) as satisfied_count,
				sum(CASE WHEN duration_ms >= 500 AND duration_ms < 2000 THEN 1 ELSE 0 END) as tolerating_count,
				sum(CASE WHEN duration_ms >= 2000 THEN 1 ELSE 0 END) as frustrated_count,
				count(*) as total_count
				FROM opa.spans_min
				WHERE start_ts >= now() - INTERVAL 1 HOUR
				GROUP BY organization_id, project_id, service, endpoint`
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to calculate Apdex scores", nil)
				continue
			}
			
			now := time.Now()
			date := now.Format("2006-01-02")
			hour := now.Format("2006-01-02 15:00:00")
			
			for _, row := range rows {
				orgID := getString(row, "organization_id")
				projID := getString(row, "project_id")
				service := getString(row, "service")
				endpoint := getString(row, "endpoint")
				satisfied := getUint64(row, "satisfied_count")
				tolerating := getUint64(row, "tolerating_count")
				frustrated := getUint64(row, "frustrated_count")
				total := getUint64(row, "total_count")
				
				apdexScore := 0.0
				if total > 0 {
					apdexScore = (float64(satisfied) + float64(tolerating)/2.0) / float64(total)
				}
				
				insertQuery := fmt.Sprintf(`INSERT INTO opa.apdex_scores 
					(organization_id, project_id, service, endpoint, date, hour, 
					satisfied_count, tolerating_count, frustrated_count, total_count, apdex_score)
					VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %.3f)`,
					escapeSQL(orgID),
					escapeSQL(projID),
					escapeSQL(service),
					escapeSQL(endpoint),
					date,
					hour,
					satisfied,
					tolerating,
					frustrated,
					total,
					apdexScore)
				
				queryClient.Execute(insertQuery)
			}
		}
	}()
	
	// Background job: Calculate throughput metrics
	go func() {
		t := time.NewTicker(1 * time.Minute) // Run every minute
		for range t.C {
			// Calculate requests per minute and hour from spans_min
			query := `SELECT 
				organization_id,
				project_id,
				service,
				coalesce(url_path, name) as endpoint,
				toDate(start_ts) as date,
				toStartOfHour(start_ts) as hour,
				toStartOfMinute(start_ts) as minute,
				count(*) as request_count
				FROM opa.spans_min
				WHERE start_ts >= now() - INTERVAL 1 HOUR
				GROUP BY organization_id, project_id, service, endpoint, date, hour, minute`
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to calculate throughput", nil)
				continue
			}
			
			for _, row := range rows {
				orgID := getString(row, "organization_id")
				projID := getString(row, "project_id")
				service := getString(row, "endpoint")
				endpoint := getString(row, "endpoint")
				date := getString(row, "date")
				hour := getString(row, "hour")
				minute := getString(row, "minute")
				requestCount := getUint64(row, "request_count")
				
				// Calculate requests per hour (aggregate all minutes in the hour)
				hourQuery := fmt.Sprintf(`SELECT sum(request_count) as total 
					FROM (SELECT count(*) as request_count
					FROM opa.spans_min
					WHERE organization_id = '%s' AND project_id = '%s' 
					AND service = '%s' AND coalesce(url_path, name) = '%s'
					AND toStartOfHour(start_ts) = '%s'
					GROUP BY toStartOfMinute(start_ts))`,
					escapeSQL(orgID), escapeSQL(projID), escapeSQL(service), escapeSQL(endpoint), hour)
				hourRows, _ := queryClient.Query(hourQuery)
				requestsPerHour := requestCount
				if len(hourRows) > 0 {
					requestsPerHour = getUint64(hourRows[0], "total")
				}
				
				insertQuery := fmt.Sprintf(`INSERT INTO opa.throughput_metrics 
					(organization_id, project_id, service, endpoint, date, hour, minute, requests_per_minute, requests_per_hour)
					VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d)`,
					escapeSQL(orgID),
					escapeSQL(projID),
					escapeSQL(service),
					escapeSQL(endpoint),
					date,
					hour,
					minute,
					requestCount,
					requestsPerHour)
				
				queryClient.Execute(insertQuery)
			}
		}
	}()
	
	// Background job: Calculate response breakdown
	go func() {
		t := time.NewTicker(5 * time.Minute) // Run every 5 minutes
		for range t.C {
			// Calculate breakdown from spans_min and spans_full
			// Extract component times from SQL, HTTP, cache, redis operations
			query := `SELECT 
				organization_id,
				project_id,
				service,
				coalesce(url_path, name) as endpoint,
				toDate(start_ts) as date,
				toStartOfHour(start_ts) as hour,
				avg(duration_ms) as avg_duration,
				count(*) as request_count
				FROM opa.spans_min
				WHERE start_ts >= now() - INTERVAL 1 HOUR
				GROUP BY organization_id, project_id, service, endpoint, date, hour`
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to calculate response breakdown", nil)
				continue
			}
			
			for _, row := range rows {
				orgID := getString(row, "organization_id")
				projID := getString(row, "project_id")
				service := getString(row, "service")
				endpoint := getString(row, "endpoint")
				date := getString(row, "date")
				hour := getString(row, "hour")
				avgDuration := getFloat64(row, "avg_duration")
				
				// Get component times from spans_full (simplified - would need to parse JSON)
				// For now, estimate based on operation counts
				dbTime := avgDuration * 0.2 // Estimate 20% for DB
				externalTime := avgDuration * 0.1 // Estimate 10% for external
				cacheTime := avgDuration * 0.05 // Estimate 5% for cache
				redisTime := avgDuration * 0.05 // Estimate 5% for redis
				appTime := avgDuration * 0.6 // Rest is application time
				
				insertQuery := fmt.Sprintf(`INSERT INTO opa.response_breakdown 
					(organization_id, project_id, service, endpoint, date, hour,
					db_time_ms, external_time_ms, cache_time_ms, redis_time_ms, 
					application_time_ms, total_time_ms, request_count)
					VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %d)`,
					escapeSQL(orgID),
					escapeSQL(projID),
					escapeSQL(service),
					escapeSQL(endpoint),
					date,
					hour,
					dbTime,
					externalTime,
					cacheTime,
					redisTime,
					appTime,
					avgDuration,
					getUint64(row, "request_count"))
				
				queryClient.Execute(insertQuery)
			}
		}
	}()
	
	// Background job: Detect slow queries and aggregate performance
	go func() {
		t := time.NewTicker(5 * time.Minute) // Run every 5 minutes
		slowQueryThreshold := 100.0 // 100ms threshold
		
		for range t.C {
			// Detect slow queries from spans_min
			query := fmt.Sprintf(`SELECT 
				organization_id,
				project_id,
				trace_id,
				span_id,
				service,
				db_system,
				query_fingerprint,
				duration_ms
				FROM opa.spans_min
				WHERE query_fingerprint != '' 
				AND duration_ms >= %.2f
				AND start_ts >= now() - INTERVAL 1 HOUR`,
				slowQueryThreshold)
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to detect slow queries", nil)
				continue
			}
			
			for _, row := range rows {
				orgID := getString(row, "organization_id")
				projID := getString(row, "project_id")
				traceID := getString(row, "trace_id")
				spanID := getString(row, "span_id")
				service := getString(row, "service")
				dbSystem := getString(row, "db_system")
				fingerprint := getString(row, "query_fingerprint")
				duration := getFloat64(row, "duration_ms")
				
				queryID := generateID()
				
				// Store slow query
				insertQuery := fmt.Sprintf(`INSERT INTO opa.slow_queries 
					(organization_id, project_id, query_id, query_fingerprint, query_text, 
					db_system, trace_id, span_id, service, duration_ms, detected_at)
					VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %.2f, now())`,
					escapeSQL(orgID),
					escapeSQL(projID),
					escapeSQL(queryID),
					escapeSQL(fingerprint),
					escapeSQL(fingerprint), // Use fingerprint as query text for now
					escapeSQL(dbSystem),
					escapeSQL(traceID),
					escapeSQL(spanID),
					escapeSQL(service),
					duration)
				
				queryClient.Execute(insertQuery)
			}
			
			// Aggregate query performance
			perfQuery := `SELECT 
				organization_id,
				project_id,
				query_fingerprint,
				db_system,
				toDate(start_ts) as date,
				toStartOfHour(start_ts) as hour,
				count(*) as execution_count,
				sum(duration_ms) as total_duration_ms,
				avg(duration_ms) as avg_duration_ms,
				quantile(0.95)(duration_ms) as p95_duration_ms,
				quantile(0.99)(duration_ms) as p99_duration_ms,
				max(duration_ms) as max_duration_ms,
				min(duration_ms) as min_duration_ms
				FROM opa.spans_min
				WHERE query_fingerprint != ''
				AND start_ts >= now() - INTERVAL 1 HOUR
				GROUP BY organization_id, project_id, query_fingerprint, db_system, date, hour`
			
			perfRows, err := queryClient.Query(perfQuery)
			if err != nil {
				LogError(err, "Failed to aggregate query performance", nil)
				continue
			}
			
			for _, row := range perfRows {
				orgID := getString(row, "organization_id")
				projID := getString(row, "project_id")
				fingerprint := getString(row, "query_fingerprint")
				dbSystem := getString(row, "db_system")
				date := getString(row, "date")
				hour := getString(row, "hour")
				
				insertPerfQuery := fmt.Sprintf(`INSERT INTO opa.query_performance 
					(organization_id, project_id, query_fingerprint, db_system, date, hour,
					execution_count, total_duration_ms, avg_duration_ms, p95_duration_ms, 
					p99_duration_ms, max_duration_ms, min_duration_ms)
					VALUES ('%s', '%s', '%s', '%s', '%s', '%s', %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f)`,
					escapeSQL(orgID),
					escapeSQL(projID),
					escapeSQL(fingerprint),
					escapeSQL(dbSystem),
					date,
					hour,
					getUint64(row, "execution_count"),
					getFloat64(row, "total_duration_ms"),
					getFloat64(row, "avg_duration_ms"),
					getFloat64(row, "p95_duration_ms"),
					getFloat64(row, "p99_duration_ms"),
					getFloat64(row, "max_duration_ms"),
					getFloat64(row, "min_duration_ms"))
				
				queryClient.Execute(insertPerfQuery)
			}
		}
	}()
	
	inCh := make(chan json.RawMessage, MAX_QUEUE_SIZE)
	for i := 0; i < 8; i++ {
		go worker(inCh, tb, writer, wsHub)
	}
	
	// Accept connections from Unix socket
	if unixListener != nil {
		go func() {
			for {
				conn, err := unixListener.Accept()
				if err != nil {
					LogError(err, "Unix socket accept error", nil)
					continue
				}
				go handleConn(conn, inCh)
			}
		}()
	}
	
	// Accept connections from TCP
	if tcpListener != nil {
		go func() {
			for {
				conn, err := tcpListener.Accept()
				if err != nil {
					LogError(err, "TCP accept error", nil)
					continue
				}
				go handleConn(conn, inCh)
			}
		}()
	}
	
	// Keep main goroutine alive
	select {}
}

var (
	tb                *TailBuffer
	writer            *ClickHouseWriter
	queryClient       *ClickHouseQuery
	breaker           *CircuitBreaker
	logCorrelation    *LogCorrelation
	currentQueueSize  int64
	currentSamplingRate uint64 = 1000 // 1.0 * 1000
)

func handleConn(conn net.Conn, inCh chan<- json.RawMessage) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)
	
	for scanner.Scan() {
		line := scanner.Bytes()
		incomingCounter.Inc()
		
		// Decompress if needed
		decompressed, err := decompressLZ4(line)
		if err != nil {
			LogError(err, "Failed to decompress data", nil)
			continue
		}
		
		raw := make([]byte, len(decompressed))
		copy(raw, decompressed)
		
		queueSize := atomic.AddInt64(&currentQueueSize, 1)
		queueSizeGauge.Set(float64(queueSize))
		
		select {
		case inCh <- raw:
		default:
			droppedCounter.Inc()
			atomic.AddInt64(&currentQueueSize, -1)
			LogWarn("Queue overflow - message dropped", map[string]interface{}{
				"queue_size": queueSize,
				"max_queue_size": MAX_QUEUE_SIZE,
			})
		}
	}
	
	if err := scanner.Err(); err != nil {
		log.Printf("scanner err: %v", err)
	}
}

// Error message structure
type ErrorMessage struct {
	Type            string  `json:"type"`
	TraceID         string  `json:"trace_id"`
	SpanID          string  `json:"span_id"`
	InstanceID      string  `json:"instance_id"`
	GroupID         string  `json:"group_id"`
	Fingerprint     string  `json:"fingerprint"`
	ErrorType       string  `json:"error_type"`
	ErrorMessage    string  `json:"error_message"`
	File            string  `json:"file"`
	Line            int     `json:"line"`
	StackTrace      string  `json:"stack_trace,omitempty"`
	OrganizationID  string  `json:"organization_id"`
	ProjectID       string  `json:"project_id"`
	Service         string  `json:"service"`
	OccurredAtMs    int64   `json:"occurred_at_ms"`
}

// Handle error messages from PHP extension
func handleErrorMessage(raw json.RawMessage, writer *ClickHouseWriter) {
	var errMsg ErrorMessage
	if err := json.Unmarshal(raw, &errMsg); err != nil {
		log.Printf("Failed to unmarshal error message: %v", err)
		return
	}
	
	// Set defaults
	if errMsg.OrganizationID == "" {
		errMsg.OrganizationID = "default-org"
	}
	if errMsg.ProjectID == "" {
		errMsg.ProjectID = "default-project"
	}
	if errMsg.Service == "" {
		errMsg.Service = "php-fpm"
	}
	
	// Convert timestamp
	occurredAt := time.UnixMilli(errMsg.OccurredAtMs)
	if errMsg.OccurredAtMs == 0 {
		occurredAt = time.Now()
	}
	
	// Write error instance
	errorInstance := map[string]interface{}{
		"organization_id": errMsg.OrganizationID,
		"project_id":      errMsg.ProjectID,
		"instance_id":     errMsg.InstanceID,
		"group_id":        errMsg.GroupID,
		"trace_id":        errMsg.TraceID,
		"span_id":         errMsg.SpanID,
		"error_type":      errMsg.ErrorType,
		"error_message":  errMsg.ErrorMessage,
		"stack_trace":     errMsg.StackTrace,
		"occurred_at":     occurredAt.Format("2006-01-02 15:04:05"),
		"environment":     "production",
		"release":         "",
		"user_context":   "{}",
		"tags":            "{}",
	}
	
	// Write error group (using ReplacingMergeTree, so we can update counts)
	errorGroup := map[string]interface{}{
		"organization_id": errMsg.OrganizationID,
		"project_id":      errMsg.ProjectID,
		"group_id":        errMsg.GroupID,
		"fingerprint":     errMsg.Fingerprint,
		"error_type":      errMsg.ErrorType,
		"error_message":   errMsg.ErrorMessage,
		"first_seen":      occurredAt.Format("2006-01-02 15:04:05"),
		"last_seen":       occurredAt.Format("2006-01-02 15:04:05"),
		"count":           uint64(1),
		"user_count":      uint64(1),
		"status":          "unresolved",
		"assigned_to":     nil,
	}
	
	// Write to ClickHouse
	writer.AddError(errorInstance, errorGroup)
	
	log.Printf("Error tracked: type=%s, message=%.100s, file=%s:%d", 
		errMsg.ErrorType, errMsg.ErrorMessage, errMsg.File, errMsg.Line)
}

func worker(inCh <-chan json.RawMessage, tb *TailBuffer, writer *ClickHouseWriter, wsHub *WebSocketHub) {
	for raw := range inCh {
		start := time.Now()
		atomic.AddInt64(&currentQueueSize, -1)
		
		if !breaker.Allow() {
			droppedCounter.Inc()
			LogWarn("Message dropped - circuit breaker open", nil)
			continue
		}
		
		// Check message type first
		var msgType struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &msgType); err != nil {
			log.Printf("bad json: %v", err)
			breaker.RecordFailure()
			continue
		}
		
		// Handle error messages separately
		if msgType.Type == "error" {
			handleErrorMessage(raw, writer)
			processingDuration.Observe(time.Since(start).Seconds())
			continue
		}
		
		var inc Incoming
		if err := json.Unmarshal(raw, &inc); err != nil {
			log.Printf("bad json: %v", err)
			breaker.RecordFailure()
			continue
		}
		
		if len(inc.Tags) > 0 {
			hasHttpRequest := strings.Contains(string(inc.Tags), "\"http_request\"") || strings.Contains(string(inc.Tags), "http_request")
			
			if !hasHttpRequest {
				var tagsMap map[string]interface{}
				if err := json.Unmarshal(inc.Tags, &tagsMap); err == nil {
					if _, exists := tagsMap["http_request"]; !exists {
						tagsMap["http_request"] = map[string]interface{}{}
						if newTagsBytes, err := json.Marshal(tagsMap); err == nil {
							inc.Tags = json.RawMessage(newTagsBytes)
						}
					}
				}
			}
		}
		
		// Apply sampling (using proper random sampling, not time-based)
		rate := float64(atomic.LoadUint64(&currentSamplingRate)) / 1000.0
		if rate < 1.0 {
			// Use proper random sampling instead of time-based
			// This ensures uniform distribution across requests
			if rand.Float64() > rate {
				continue
			}
		}
		
		// Metrics
		spansTotal.WithLabelValues(inc.Service, inc.Status).Inc()
		durationHistogram.WithLabelValues(inc.Service, inc.Name).Observe(inc.Duration / 1000.0)
		
		// Network metrics
		if len(inc.Net) > 0 {
			var net map[string]interface{}
			if err := json.Unmarshal(inc.Net, &net); err == nil {
				if sent, ok := net["bytes_sent"].(float64); ok {
					networkBytesTotal.WithLabelValues("sent").Add(sent)
				}
				if recv, ok := net["bytes_received"].(float64); ok {
					networkBytesTotal.WithLabelValues("received").Add(recv)
				}
			}
		}
		
		// SQL metrics - check both direct SQL field and call stack
		if len(inc.Sql) > 0 {
			var sqlArray []interface{}
			if err := json.Unmarshal(inc.Sql, &sqlArray); err == nil {
				for _, sqlItem := range sqlArray {
					if sqlMap, ok := sqlItem.(map[string]interface{}); ok {
						if query, ok := sqlMap["query"].(string); ok {
							// Use query as fingerprint for now
							sqlQueriesTotal.WithLabelValues(query).Inc()
						}
					}
				}
			}
		}
		
		// Also check SQL queries in call stack
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			for _, callNode := range callStack {
				for _, sqlQuery := range callNode.SQLQueries {
					if sqlMap, ok := sqlQuery.(map[string]interface{}); ok {
						if query, ok := sqlMap["query"].(string); ok {
							// Use query as fingerprint for now
							sqlQueriesTotal.WithLabelValues(query).Inc()
						}
					}
				}
			}
		}
		
		// Extract language metadata
		language, langVersion, framework, frameworkVersion := extractLanguageMetadata(&inc)
		
		// Extract tenant context from tags
		orgID := "default-org"
		projectID := "default-project"
		if len(inc.Tags) > 0 {
			var tags map[string]interface{}
			if err := json.Unmarshal(inc.Tags, &tags); err == nil {
				if v, ok := tags["organization_id"].(string); ok && v != "" {
					orgID = v
				}
				if v, ok := tags["project_id"].(string); ok && v != "" {
					projectID = v
				}
			}
		}
		
		// Extract network bytes from inc.Net or aggregate from call stack
		var bytesSent uint64 = 0
		var bytesReceived uint64 = 0
		
		// First try to get from direct net field
		if len(inc.Net) > 0 {
			var net map[string]interface{}
			if err := json.Unmarshal(inc.Net, &net); err == nil {
				if sent, ok := net["bytes_sent"].(float64); ok {
					bytesSent = uint64(sent)
				}
				if recv, ok := net["bytes_received"].(float64); ok {
					bytesReceived = uint64(recv)
				}
			}
		}
		
		// If not found in net field, aggregate from call stack
		if bytesSent == 0 && bytesReceived == 0 && len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			for _, callNode := range callStack {
				if callNode.NetworkBytesSent > 0 {
					bytesSent += uint64(callNode.NetworkBytesSent)
				}
				if callNode.NetworkBytesReceived > 0 {
					bytesReceived += uint64(callNode.NetworkBytesReceived)
				}
			}
		}
		
		// Store minimal row
		min := map[string]interface{}{
			"organization_id": orgID,
			"project_id":      projectID,
			"trace_id":        inc.TraceID,
			"span_id":         inc.SpanID,
			"parent_id":       nil,
			"service":         inc.Service,
			"name":            inc.Name,
			"url_scheme":      inc.URLScheme,
			"url_host":        inc.URLHost,
			"url_path":        inc.URLPath,
			"start_ts":        time.UnixMilli(inc.StartTS).Format("2006-01-02 15:04:05.000"),
			"end_ts":          time.UnixMilli(inc.EndTS).Format("2006-01-02 15:04:05.000"),
			"duration_ms":     inc.Duration,
			"cpu_ms":          inc.CPUms,
			"status":          inc.Status,
			"language":        language,
			"bytes_sent":      bytesSent,
			"bytes_received":  bytesReceived,
		}
		
		// Add optional language metadata fields
		if langVersion != "" {
			min["language_version"] = langVersion
		}
		if framework != "" {
			min["framework"] = framework
		}
		if frameworkVersion != "" {
			min["framework_version"] = frameworkVersion
		}
		
		// Process SQL queries from call stack
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			for _, callNode := range callStack {
				if len(callNode.SQLQueries) > 0 {
					for _, sqlQuery := range callNode.SQLQueries {
						if sqlMap, ok := sqlQuery.(map[string]interface{}); ok {
							if v, ok := sqlMap["db_system"].(string); ok {
								min["db_system"] = v
							}
							// Use first query for fingerprint
							if query, ok := sqlMap["query"].(string); ok {
								min["query_fingerprint"] = query
							}
							break // Use first query for min row
						}
					}
				}
			}
		}
		
		// Also check direct SQL field (backward compatibility)
		if inc.Sql != nil {
			var sqlArray []interface{}
			if err := json.Unmarshal(inc.Sql, &sqlArray); err == nil {
				if len(sqlArray) > 0 {
					if sqlMap, ok := sqlArray[0].(map[string]interface{}); ok {
						if v, ok := sqlMap["db_system"].(string); ok {
							min["db_system"] = v
						}
						if query, ok := sqlMap["query"].(string); ok {
							min["query_fingerprint"] = query
						}
					}
				}
			}
		}
		
		writer.Add(min)
		
		// Store full span if trace should be kept OR if call stack is present
		// This ensures call stack is always stored when available
		hasCallStack := len(inc.Stack) > 0
		// Also write full data if dumps are present (dumps are important and should always be stored)
		hasDumps := len(inc.Dumps) > 0 && string(inc.Dumps) != "[]" && string(inc.Dumps) != "null"
		if tb.ShouldKeep(inc.TraceID) || inc.ChunkDone != nil && *inc.ChunkDone || hasCallStack || hasDumps {
			// Serialize call stack to JSON string
			stackJSON := "[]"
			if len(inc.Stack) > 0 {
				if stackBytes, err := json.Marshal(inc.Stack); err == nil {
					stackJSON = string(stackBytes)
				}
			}
			
			// Aggregate all SQL queries: from inc.Sql (direct field) and from call stack
			var allSQLQueries []interface{}
			
			// Collect from direct SQL field (backward compatibility)
			if len(inc.Sql) > 0 {
				var sqlArray []interface{}
				if err := json.Unmarshal(inc.Sql, &sqlArray); err == nil {
					allSQLQueries = append(allSQLQueries, sqlArray...)
				}
			}
			
			// Collect from call stack (main method)
			if len(inc.Stack) > 0 {
				callStack := parseCallStack(inc.Stack)
				stackQueries := collectSQLQueriesFromCallStack(callStack)
				if len(stackQueries) > 0 {
					allSQLQueries = append(allSQLQueries, stackQueries...)
				}
			}
			
			// Serialize aggregated SQL queries to JSON string
			sqlJSON := "[]"
			if len(allSQLQueries) > 0 {
				if sqlBytes, err := json.Marshal(allSQLQueries); err == nil {
					sqlJSON = string(sqlBytes)
				} else {
					LogError(err, "Failed to marshal SQL queries", nil)
				}
			} else {
			}
			
			// Aggregate HTTP requests from call stack
			var allHttpRequests []interface{}
			if len(inc.Stack) > 0 {
				callStack := parseCallStack(inc.Stack)
				allHttpRequests = collectHttpRequestsFromCallStack(callStack)
			}
			httpJSON := "[]"
			if len(allHttpRequests) > 0 {
				if httpBytes, err := json.Marshal(allHttpRequests); err == nil {
					httpJSON = string(httpBytes)
				}
			}
			
			// Aggregate cache operations from call stack
			var allCacheOps []interface{}
			if len(inc.Stack) > 0 {
				callStack := parseCallStack(inc.Stack)
				allCacheOps = collectCacheOperationsFromCallStack(callStack)
			}
			cacheJSON := "[]"
			if len(allCacheOps) > 0 {
				if cacheBytes, err := json.Marshal(allCacheOps); err == nil {
					cacheJSON = string(cacheBytes)
				}
			}
			
			// Aggregate Redis operations from call stack
			var allRedisOps []interface{}
			if len(inc.Stack) > 0 {
				callStack := parseCallStack(inc.Stack)
				allRedisOps = collectRedisOperationsFromCallStack(callStack)
			}
			redisJSON := "[]"
			if len(allRedisOps) > 0 {
				if redisBytes, err := json.Marshal(allRedisOps); err == nil {
					redisJSON = string(redisBytes)
				}
			}
			
			// Serialize dumps to JSON string
			dumpsJSON := "[]"
			dumpsStr := string(inc.Dumps)
			if len(inc.Dumps) > 0 && dumpsStr != "[]" && dumpsStr != "null" {
				// inc.Dumps is already a JSON string (json.RawMessage), use it directly
				dumpsJSON = dumpsStr
				previewLen := 200
				if len(dumpsJSON) < previewLen {
					previewLen = len(dumpsJSON)
				}
				
				// Broadcast dumps via WebSocket
				if wsHub != nil {
					// Parse dumps to send individual entries
					var dumpsArray []interface{}
					if err := json.Unmarshal(inc.Dumps, &dumpsArray); err == nil {
						broadcastData := map[string]interface{}{
							"trace_id":   inc.TraceID,
							"span_id":    inc.SpanID,
							"service":    inc.Service,
							"span_name":  inc.Name,
							"dumps":      dumpsArray,
							"timestamp":  time.Now().Unix(),
						}
						wsHub.Broadcast("dumps", broadcastData)
					}
				}
			} else {
			}
			
			// Store tags as-is (raw JSON string) to preserve all fields including empty objects
			// Ensure http_request is always present even if empty
			tagsStr := string(inc.Tags)
			if len(tagsStr) > 0 {
				hasHttpRequest := strings.Contains(tagsStr, "\"http_request\"") || strings.Contains(tagsStr, "http_request")
				
				// If http_request is missing, add it (even if empty) to ensure it's always present
				if !hasHttpRequest {
					// Parse tags to add http_request
					var tagsMap map[string]interface{}
					if err := json.Unmarshal(inc.Tags, &tagsMap); err == nil {
						// Add empty http_request if not present
						if _, exists := tagsMap["http_request"]; !exists {
							tagsMap["http_request"] = map[string]interface{}{}
							// Re-marshal tags with http_request
							if newTagsBytes, err := json.Marshal(tagsMap); err == nil {
								tagsStr = string(newTagsBytes)
							}
						}
					}
				}
			} else {
				LogWarn("tagsStr is empty", map[string]interface{}{
					"trace_id": inc.TraceID,
					"span_id":  inc.SpanID,
					"tags_len": len(inc.Tags),
				}) 
			}
			
			full := map[string]interface{}{
				"organization_id": orgID,
				"project_id":      projectID,
				"trace_id":        inc.TraceID,
				"span_id":         inc.SpanID,
				"parent_id":       inc.ParentID,
				"service":         inc.Service,
				"name":            inc.Name,
				"url_scheme":       inc.URLScheme,
				"url_host":        inc.URLHost,
				"url_path":        inc.URLPath,
				"start_ts":   time.UnixMilli(inc.StartTS).Format("2006-01-02 15:04:05.000"),
				"end_ts":     time.UnixMilli(inc.EndTS).Format("2006-01-02 15:04:05.000"),
				"duration_ms": inc.Duration,
				"cpu_ms":     inc.CPUms,
				"status":     inc.Status,
				"net":        string(inc.Net),
				"sql":        sqlJSON,
				"http":       httpJSON,
				"cache":      cacheJSON,
				"redis":      redisJSON,
				"stack":      stackJSON,
				"tags":       tagsStr,
				"dumps":      dumpsJSON,
				"language":   language,
			}
			
			// Add optional language metadata fields
			if langVersion != "" {
				full["language_version"] = langVersion
			}
			if framework != "" {
				full["framework"] = framework
			}
			if frameworkVersion != "" {
				full["framework_version"] = frameworkVersion
			}
			writer.AddFull(full)
		}
		
		if inc.TraceID != "" {
			tb.Add(inc.TraceID, raw)
		}
		
		breaker.RecordSuccess()
		processingDuration.Observe(time.Since(start).Seconds())
		
		// Track traces
		if inc.ParentID == nil {
			tracesTotal.WithLabelValues(inc.Service).Inc()
		}
	}
}

