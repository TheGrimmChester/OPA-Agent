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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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


// normalizeSQLQuery creates a fingerprint by normalizing SQL queries
// It replaces literals with placeholders to group similar queries together
func normalizeSQLQuery(query string) string {
	if query == "" {
		return ""
	}
	
	// Trim and normalize whitespace
	query = strings.TrimSpace(query)
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\t", " ")
	// Collapse multiple spaces
	for strings.Contains(query, "  ") {
		query = strings.ReplaceAll(query, "  ", " ")
	}
	
	// Replace string literals (single and double quotes) first
	query = replaceStringLiterals(query)
	
	// Replace numeric literals
	query = replaceNumericLiterals(query)
	
	// Normalize common patterns
	query = normalizeSQLPatterns(query)
	
	// Final whitespace cleanup
	query = strings.TrimSpace(query)
	
	return query
}

// Extract language metadata from tags or use defaults

// replaceStringLiterals replaces string literals with ?
// Handles SQL string literals with proper escaping:
// - Single quotes: 'text' or 'text''s' (escaped quote - two single quotes)
// - Double quotes: "text" or "text""s" (escaped quote in some dialects)
func replaceStringLiterals(query string) string {
	var result strings.Builder
	inSingleQuote := false
	inDoubleQuote := false
	
	runes := []rune(query)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		
		// Handle single quotes (SQL string literals)
		if r == '\'' && !inDoubleQuote {
			if inSingleQuote {
				// Check for escaped quote: '' (two single quotes in SQL)
				// This is the standard SQL way to escape a single quote
				if i+1 < len(runes) && runes[i+1] == '\'' {
					// This is an escaped quote inside the string, skip both quotes
					i++ // Skip the next quote as well
					continue
				}
				// End of string literal
				inSingleQuote = false
				result.WriteString("?")
			} else {
				// Start of string literal
				inSingleQuote = true
			}
		} else if r == '"' && !inSingleQuote {
			// Handle double quotes (identifier quotes in some SQL dialects, or string literals)
			if inDoubleQuote {
				// Check for escaped quote: "" (two double quotes)
				if i+1 < len(runes) && runes[i+1] == '"' {
					// This is an escaped quote inside the string, skip both quotes
					i++ // Skip the next quote as well
					continue
				}
				// End of string literal
				inDoubleQuote = false
				result.WriteString("?")
			} else {
				// Start of string literal
				inDoubleQuote = true
			}
		} else if inSingleQuote || inDoubleQuote {
			// Inside string literal, skip the character (it's part of the literal value)
			continue
		} else {
			// Outside string literal, keep the character
			result.WriteRune(r)
		}
	}
	
	// If we ended inside a quote (malformed query), still write the ? to close it
	if inSingleQuote || inDoubleQuote {
		result.WriteString("?")
	}
	
	return result.String()
}

// replaceNumericLiterals replaces numeric literals with ?
func replaceNumericLiterals(query string) string {
	// Use regexp to match numeric literals
	// Match: optional sign, digits, optional decimal point and more digits
	// But be careful not to match numbers that are part of identifiers
	re := regexp.MustCompile(`\b[+-]?\d+\.?\d*\b`)
	return re.ReplaceAllString(query, "?")
}

// normalizeSQLPatterns normalizes common SQL patterns
// Note: Numeric literals are already replaced by replaceNumericLiterals, so LIMIT/OFFSET are already normalized
func normalizeSQLPatterns(query string) string {
	// Normalize IN clauses: IN (?, ?, ?) -> IN (?)
	// This groups queries with different numbers of IN values
	// Match: IN ( followed by zero or more ? or commas/spaces, then )
	re := regexp.MustCompile(`(?i)\bIN\s*\([?,\s]*\)`)
	query = re.ReplaceAllString(query, "IN (?)")
	
	// Normalize VALUES clauses: VALUES (?, ?, ?) -> VALUES (?)
	// This groups INSERT statements with different numbers of values
	// Handle single VALUES clause
	re = regexp.MustCompile(`(?i)\bVALUES\s*\([?,\s]*\)`)
	query = re.ReplaceAllString(query, "VALUES (?)")
	
	// Handle multiple VALUES clauses: VALUES (?, ?), (?, ?) -> VALUES (?)
	// Replace all multiple value groups with a single normalized one
	re = regexp.MustCompile(`(?i)\bVALUES\s*(\([?,\s]*\)\s*,?\s*)+`)
	query = re.ReplaceAllString(query, "VALUES (?)")
	
	// Normalize whitespace around operators and keywords (but preserve single spaces)
	query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
	query = regexp.MustCompile(`\s*\(\s*`).ReplaceAllString(query, " (")
	query = regexp.MustCompile(`\s*\)\s*`).ReplaceAllString(query, ") ")
	query = regexp.MustCompile(`\s*,\s*`).ReplaceAllString(query, ", ")
	
	return query
}

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

// normalizeTimestamp converts a timestamp to milliseconds if it appears to be in seconds
// Year 2000 in milliseconds = 946684800000
// If timestamp is less than this threshold, it's likely in seconds and needs conversion
func normalizeTimestamp(ts int64) int64 {
	// Threshold: year 2000 in milliseconds
	const year2000Ms = 946684800000
	
	// If timestamp is less than year 2000 in milliseconds, it might be in seconds
	// But we need to be careful - very small values might be legitimate
	// Check if interpreting as milliseconds gives a date before year 2000
	if ts > 0 && ts < year2000Ms {
		// Check if it's a reasonable Unix timestamp in seconds (between 1970 and 2100)
		// Year 1970 = 0 seconds, Year 2100 = 4102444800 seconds
		if ts >= 0 && ts <= 4102444800 {
			// This looks like seconds, convert to milliseconds
			converted := ts * 1000
			// Verify the converted value makes sense (should be >= year 2000 in ms)
			if converted >= year2000Ms {
				log.Printf("[WARN] Timestamp appears to be in seconds, converting: %d -> %d", ts, converted)
				return converted
			}
		}
	}
	
	// Also check for timestamps that are clearly in seconds (year 2081 range)
	// Year 2081 in seconds ≈ 3500000000, in milliseconds ≈ 3500000000000
	// If timestamp is between 3000000000 and 5000000000, it's likely seconds
	if ts >= 3000000000 && ts <= 5000000000 {
		converted := ts * 1000
		// Verify converted value is reasonable (should be a future date but not too far)
		// Year 2100 in milliseconds = 4102444800000
		if converted <= 4102444800000 {
			log.Printf("[WARN] Timestamp appears to be in seconds (year 2081 range), converting: %d -> %d", ts, converted)
			return converted
		}
	}
	
	return ts
}

// Enrich spans with full details from spans_full
func enrichSpansWithFullDetails(spanList []*Span, traceID string, queryClient *ClickHouseQuery) {
	if len(spanList) == 0 {
		return
	}
	
	traceIDEscaped := strings.ReplaceAll(traceID, "'", "''")
	// Query for full details from spans_full
	query := fmt.Sprintf("SELECT span_id, net, sql, http, cache, redis, stack, tags, dumps FROM opa.spans_full WHERE trace_id = '%s'", traceIDEscaped)
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

// expandCallStackToSpans converts significant call stack nodes into child spans
// This creates multiple spans per trace instead of just one span with a call stack
func expandCallStackToSpans(rootSpan *Span) []*Span {
	if rootSpan == nil || len(rootSpan.Stack) == 0 {
		return []*Span{rootSpan}
	}
	
	var allSpans []*Span
	allSpans = append(allSpans, rootSpan)
	
	// Recursively convert call nodes to spans
	var convertNodeToSpan func(node *CallNode, parentSpanID string, baseStartTS int64) *Span
	convertNodeToSpan = func(node *CallNode, parentSpanID string, baseStartTS int64) *Span {
		// Only create a span for nodes that have significant operations or are important
		hasSignificantData := len(node.SQLQueries) > 0 || 
			len(node.HttpRequests) > 0 || 
			len(node.CacheOperations) > 0 || 
			len(node.RedisOperations) > 0 ||
			node.DurationMs > 10.0 // Only create spans for calls longer than 10ms
		
		if !hasSignificantData {
			return nil
		}
		
		// Create a new span from this call node
		spanID := node.CallID
		if spanID == "" {
			spanID = fmt.Sprintf("%s-%d", rootSpan.SpanID, len(allSpans))
		}
		
		parentID := parentSpanID
		if node.ParentID != "" {
			// Try to find the parent span by call_id
			for _, s := range allSpans {
				if s.SpanID == node.ParentID {
					parentID = s.SpanID
					break
				}
			}
		}
		
		// Calculate timestamps relative to root span
		// Since call nodes don't have absolute timestamps, we approximate based on depth and order
		// Start time is approximate: root start + some offset based on depth
		startTS := rootSpan.StartTS + int64(float64(node.Depth)*1000) // Approximate offset by depth
		endTS := startTS + int64(node.DurationMs*1000)
		
		// Build span name
		spanName := node.Function
		if node.Class != "" {
			spanName = node.Class + "::" + node.Function
		}
		if spanName == "" {
			spanName = "function_call"
		}
		
		childSpan := &Span{
			TraceID:  rootSpan.TraceID,
			SpanID:   spanID,
			ParentID: &parentID,
			Service:  rootSpan.Service,
			Name:     spanName,
			StartTS:  startTS,
			EndTS:    endTS,
			Duration: node.DurationMs,
			CPUms:    node.CPUMs,
			Status:   rootSpan.Status,
			Sql:      node.SQLQueries,
			Http:     node.HttpRequests,
			Cache:    node.CacheOperations,
			Redis:    node.RedisOperations,
			Language: rootSpan.Language,
			LanguageVersion: rootSpan.LanguageVersion,
			Framework: rootSpan.Framework,
			FrameworkVersion: rootSpan.FrameworkVersion,
		}
		
		// Add network data if present
		if node.NetworkBytesSent > 0 || node.NetworkBytesReceived > 0 {
			childSpan.Net = map[string]interface{}{
				"bytes_sent":     node.NetworkBytesSent,
				"bytes_received": node.NetworkBytesReceived,
			}
		}
		
		// Add tags with call information
		childSpan.Tags = map[string]interface{}{
			"call_id": node.CallID,
			"file":    node.File,
			"line":    node.Line,
			"depth":   node.Depth,
		}
		
		allSpans = append(allSpans, childSpan)
		
		// Recursively process children
		for _, childNode := range node.Children {
			childSpanNode := convertNodeToSpan(childNode, spanID, baseStartTS)
			if childSpanNode != nil {
				childSpan.Children = append(childSpan.Children, childSpanNode)
			}
		}
		
		return childSpan
	}
	
	// Process all root-level call nodes
	for _, rootNode := range rootSpan.Stack {
		childSpan := convertNodeToSpan(rootNode, rootSpan.SpanID, rootSpan.StartTS)
		if childSpan != nil {
			rootSpan.Children = append(rootSpan.Children, childSpan)
		}
	}
	
	return allSpans
}

// extractExpandSpansFlag extracts the expand_spans flag from span tags
// Returns true if expand_spans is true or not present (default to multiple spans mode)
// Returns false if expand_spans is explicitly set to false
// 
// Backward compatibility: Old spans without this flag will default to multiple spans mode (new behavior)
// This ensures existing traces work correctly while new traces get the improved visualization
func extractExpandSpansFlag(span *Span) bool {
	if span == nil || span.Tags == nil {
		return true // Default to multiple spans mode
	}
	
	if expandSpansVal, ok := span.Tags["expand_spans"]; ok {
		// Handle different types that might come from JSON
		switch v := expandSpansVal.(type) {
		case bool:
			return v
		case string:
			return v == "true" || v == "1"
		case float64:
			return v != 0
		case int:
			return v != 0
		}
	}
	
	return true // Default to multiple spans mode if flag not present
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
	
	// Check if child spans were already sent separately
	// If multiple spans exist and root has empty stack, spans were sent separately (no expansion needed)
	// If only root span exists with stack, expand for backward compatibility with old traces
	hasChildSpans := len(spans) > 1
	hasStackInRoot := root != nil && len(root.Stack) > 0
	
	// Only expand if:
	// 1. Root has a call stack (old format)
	// 2. Only one span exists (no child spans sent separately)
	// 3. expand_spans flag is true (or not set, default true)
	if root != nil && hasStackInRoot && !hasChildSpans {
		// Check expand_spans flag from root span's tags (defaults to true for backward compatibility)
		shouldExpand := extractExpandSpansFlag(root)
		
		if shouldExpand {
			// Backward compatibility: expand old traces that have embedded stack
			expandedSpans := expandCallStackToSpans(root)
			// Update the spans list with expanded spans
			spans = expandedSpans
			
			// Rebuild span map and tree with expanded spans
			spanMap = make(map[string]*Span)
			for _, span := range spans {
				spanMap[span.SpanID] = span
			}
			
			// Rebuild tree with expanded spans
			root = nil
			for _, span := range spans {
				if span.ParentID == nil {
					root = span
				} else {
					if parent, ok := spanMap[*span.ParentID]; ok {
						parent.Children = append(parent.Children, span)
					}
				}
			}
		}
		// If shouldExpand is false, keep original behavior: single span with call stack nested inside
	}
	// If hasChildSpans is true, child spans were already sent separately, no expansion needed
	
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
	if clickhouseEnv := os.Getenv("CLICKHOUSE_URL"); clickhouseEnv != "" {
		*clickhouseURL = clickhouseEnv
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
			// Get distinct organizations from actual data (spans_min) instead of organizations table
			// This ensures we show organizations that actually have data
			query := `SELECT DISTINCT 
				coalesce(nullif(organization_id, ''), 'default-org') as org_id,
				coalesce(nullif(organization_id, ''), 'default-org') as name
			FROM opa.spans_min 
			WHERE start_ts >= now() - INTERVAL 30 DAY
			ORDER BY org_id`
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error (spans_min)", map[string]interface{}{
					"path":  r.URL.Path,
					"method": r.Method,
					"query":  query,
				})
				// Fallback to organizations table if spans query fails
				query = "SELECT org_id, name, settings, created_at, updated_at FROM opa.organizations"
				if ctx.OrganizationID != "default-org" {
					query += fmt.Sprintf(" WHERE org_id = '%s'", escapeSQL(ctx.OrganizationID))
				}
				query += " ORDER BY created_at DESC"
				rows, err = queryClient.Query(query)
				if err != nil {
					LogError(err, "ClickHouse query error (organizations table)", map[string]interface{}{
						"path": r.URL.Path,
						"method": r.Method,
					})
					http.Error(w, fmt.Sprintf("query error: %v", err), 500)
					return
				}
			}
			
			orgs := make([]map[string]interface{}, 0)
			log.Printf("[DEBUG] Organizations query: row_count=%d, query=%s", len(rows), query)
			for i, row := range rows {
				orgID := getString(row, "org_id")
				orgName := getString(row, "name")
				log.Printf("[DEBUG] Row %d: org_id=%s, name=%s, row=%v", i, orgID, orgName, row)
				// If name is empty or same as org_id, use a formatted name
				if orgName == "" || orgName == orgID {
					orgName = orgID
					// Format: "default-org" -> "Default Org"
					if orgID == "default-org" {
						orgName = "Default Organization"
					} else {
						// Capitalize first letter and add spaces before capital letters
						orgName = strings.Title(strings.ReplaceAll(orgID, "-", " "))
					}
				}
				orgMap := map[string]interface{}{
					"org_id": orgID,
					"name":   orgName,
				}
				// Only add these if they exist (from organizations table fallback)
				if settings := getString(row, "settings"); settings != "" {
					orgMap["settings"] = settings
				}
				if createdAt := getString(row, "created_at"); createdAt != "" {
					orgMap["created_at"] = createdAt
				}
				if updatedAt := getString(row, "updated_at"); updatedAt != "" {
					orgMap["updated_at"] = updatedAt
				}
				orgs = append(orgs, orgMap)
			}
			// Always return an array, never null
			if orgs == nil {
				orgs = []map[string]interface{}{}
			}
			log.Printf("[DEBUG] Returning organizations: count=%d", len(orgs))
			w.Header().Set("Content-Type", "application/json")
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
			// Normalize orgID (handle "all" and empty)
			if orgID == "all" || orgID == "" {
				orgID = "default-org"
			}
			
			// Get distinct projects from actual data (spans_min) instead of projects table
			// This ensures we show projects that actually have data for the given organization
			query := fmt.Sprintf(`SELECT DISTINCT 
				coalesce(nullif(project_id, ''), 'default-project') as project_id,
				coalesce(nullif(project_id, ''), 'default-project') as name,
				coalesce(nullif(organization_id, ''), 'default-org') as org_id
			FROM opa.spans_min 
			WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s')
				AND start_ts >= now() - INTERVAL 30 DAY
			ORDER BY project_id`,
				escapeSQL(orgID))
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "ClickHouse query error (spans_min for projects)", map[string]interface{}{
					"path":  r.URL.Path,
					"method": r.Method,
					"query":  query,
				})
				// Fallback to projects table if spans query fails
				query = fmt.Sprintf("SELECT project_id, org_id, name, dsn, created_at, updated_at FROM opa.projects WHERE org_id = '%s' ORDER BY created_at DESC",
					escapeSQL(orgID))
				rows, err = queryClient.Query(query)
				if err != nil {
					LogError(err, "ClickHouse query error (projects table)", map[string]interface{}{
						"path": r.URL.Path,
						"method": r.Method,
					})
					http.Error(w, fmt.Sprintf("query error: %v", err), 500)
					return
				}
			}
			projects := make([]map[string]interface{}, 0)
			seenProjects := make(map[string]bool) // Deduplicate by project_id
			log.Printf("[DEBUG] Projects query: row_count=%d", len(rows))
			for i, row := range rows {
				projID := getString(row, "project_id")
				log.Printf("[DEBUG] Processing project row %d: project_id=%s", i, projID)
				if projID == "" || seenProjects[projID] {
					log.Printf("[DEBUG] Skipping duplicate or empty project_id: %s", projID)
					continue // Skip empty or duplicate project IDs
				}
				seenProjects[projID] = true
				
				projName := getString(row, "name")
				// If name is empty or same as project_id, use a formatted name
				if projName == "" || projName == projID {
					projName = projID
					// Format: "default-project" -> "Default Project"
					if projID == "default-project" {
						projName = "Default Project"
					} else {
						// Capitalize first letter and add spaces before capital letters
						projName = strings.Title(strings.ReplaceAll(projID, "-", " "))
					}
				}
				projMap := map[string]interface{}{
					"project_id": projID,
					"name":       projName,
					"org_id":     getString(row, "org_id"),
				}
				// Only add these if they exist (from projects table fallback)
				if dsn := getString(row, "dsn"); dsn != "" {
					projMap["dsn"] = dsn
				}
				if createdAt := getString(row, "created_at"); createdAt != "" {
					projMap["created_at"] = createdAt
				}
				if updatedAt := getString(row, "updated_at"); updatedAt != "" {
					projMap["updated_at"] = updatedAt
				}
				projects = append(projects, projMap)
			}
			// Always return an array, never null
			if projects == nil {
				projects = []map[string]interface{}{}
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
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		ctx, _ := ExtractTenantContext(r, queryClient)
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		
		// Build tenant filter
		var tenantFilter string
		if ctx.IsAllTenants() {
			tenantFilter = ""
		} else {
			tenantFilter = fmt.Sprintf("(coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		
		// Build time filter
		var timeFilter string
		if timeFrom != "" {
			timeFilter += fmt.Sprintf(" AND start_ts >= '%s'", escapeSQL(timeFrom))
		}
		if timeTo != "" {
			timeFilter += fmt.Sprintf(" AND start_ts <= '%s'", escapeSQL(timeTo))
		}
		
		// Build WHERE clause
		var whereClause string
		if tenantFilter != "" {
			whereClause = " WHERE " + tenantFilter + timeFilter
		} else if timeFilter != "" {
			whereClause = " WHERE 1=1" + timeFilter
		} else {
			whereClause = ""
		}
		
		// Get traces statistics
		tracesQuery := `SELECT 
			count(DISTINCT trace_id) as total_traces,
			count(*) as total_spans,
			sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / greatest(count(*), 1) as error_rate,
			avg(duration_ms) as avg_duration_ms,
			quantile(0.50)(duration_ms) as p50_duration_ms,
			quantile(0.95)(duration_ms) as p95_duration_ms,
			quantile(0.99)(duration_ms) as p99_duration_ms
			FROM opa.spans_min` + whereClause
		
		tracesRows, err := queryClient.Query(tracesQuery)
		if err != nil {
			LogError(err, "Failed to query traces stats", map[string]interface{}{
				"endpoint": "/api/stats",
			})
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var tracesStats map[string]interface{}
		if len(tracesRows) > 0 {
			row := tracesRows[0]
			tracesStats = map[string]interface{}{
				"total_traces":    getUint64(row, "total_traces"),
				"total_spans":     getUint64(row, "total_spans"),
				"error_rate":      getFloat64(row, "error_rate"),
				"avg_duration_ms": getFloat64(row, "avg_duration_ms"),
				"p50_duration_ms": getFloat64(row, "p50_duration_ms"),
				"p95_duration_ms": getFloat64(row, "p95_duration_ms"),
				"p99_duration_ms": getFloat64(row, "p99_duration_ms"),
			}
		} else {
			tracesStats = map[string]interface{}{
				"total_traces":    uint64(0),
				"total_spans":     uint64(0),
				"error_rate":      0.0,
				"avg_duration_ms": 0.0,
				"p50_duration_ms": 0.0,
				"p95_duration_ms": 0.0,
				"p99_duration_ms": 0.0,
			}
		}
		
		// Get traces by service
		var serviceFilter string
		if tenantFilter != "" {
			serviceFilter = " WHERE " + tenantFilter + timeFilter
		} else if timeFilter != "" {
			serviceFilter = " WHERE 1=1" + timeFilter
		} else {
			serviceFilter = ""
		}
		
		servicesQuery := `SELECT 
			service,
			count(DISTINCT trace_id) as traces,
			count(*) as spans,
			sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / greatest(count(*), 1) as error_rate
			FROM opa.spans_min` + serviceFilter + `
			GROUP BY service
			ORDER BY traces DESC
			LIMIT 20`
		
		servicesRows, _ := queryClient.Query(servicesQuery)
		byService := make([]map[string]interface{}, 0)
		for _, row := range servicesRows {
			byService = append(byService, map[string]interface{}{
				"service":    getString(row, "service"),
				"traces":     getUint64(row, "traces"),
				"spans":      getUint64(row, "spans"),
				"error_rate": getFloat64(row, "error_rate"),
			})
		}
		tracesStats["by_service"] = byService
		
		// Get agent internal stats
		// Collect Prometheus metrics
		gatherer := prometheus.DefaultGatherer
		metricFamilies, err := gatherer.Gather()
		var incomingTotal, droppedTotal float64
		if err == nil {
			for _, mf := range metricFamilies {
				switch *mf.Name {
				case "apm_agent_incoming_total":
					if len(mf.Metric) > 0 {
						incomingTotal = *mf.Metric[0].Counter.Value
					}
				case "apm_agent_dropped_total":
					if len(mf.Metric) > 0 {
						droppedTotal = *mf.Metric[0].Counter.Value
					}
				}
			}
		}
		
		agentStats := map[string]interface{}{
			"queue_size": atomic.LoadInt64(&currentQueueSize),
			"incoming_total": int64(incomingTotal),
			"dropped_total":   int64(droppedTotal),
		}
		
		// Get database size from ClickHouse system tables
		dbSizeQuery := `SELECT 
			database,
			table,
			sum(bytes) as size_bytes,
			sum(rows) as rows
			FROM system.parts
			WHERE database = 'opa' AND active
			GROUP BY database, table
			ORDER BY size_bytes DESC`
		
		dbRows, err := queryClient.Query(dbSizeQuery)
		var totalSizeBytes uint64 = 0
		tables := make([]map[string]interface{}, 0)
		
		if err == nil {
			for _, row := range dbRows {
				sizeBytes := getUint64(row, "size_bytes")
				rows := getUint64(row, "rows")
				tableName := getString(row, "table")
				
				totalSizeBytes += sizeBytes
				
				// Format readable size
				var sizeReadable string
				if sizeBytes < 1024 {
					sizeReadable = fmt.Sprintf("%d B", sizeBytes)
				} else if sizeBytes < 1024*1024 {
					sizeReadable = fmt.Sprintf("%.2f KB", float64(sizeBytes)/1024)
				} else if sizeBytes < 1024*1024*1024 {
					sizeReadable = fmt.Sprintf("%.2f MB", float64(sizeBytes)/(1024*1024))
				} else {
					sizeReadable = fmt.Sprintf("%.2f GB", float64(sizeBytes)/(1024*1024*1024))
				}
				
				tables = append(tables, map[string]interface{}{
					"name":          tableName,
					"size_bytes":    sizeBytes,
					"size_readable": sizeReadable,
					"rows":         rows,
				})
			}
		}
		
		// Format total size
		var totalSizeReadable string
		if totalSizeBytes < 1024 {
			totalSizeReadable = fmt.Sprintf("%d B", totalSizeBytes)
		} else if totalSizeBytes < 1024*1024 {
			totalSizeReadable = fmt.Sprintf("%.2f KB", float64(totalSizeBytes)/1024)
		} else if totalSizeBytes < 1024*1024*1024 {
			totalSizeReadable = fmt.Sprintf("%.2f MB", float64(totalSizeBytes)/(1024*1024))
		} else {
			totalSizeReadable = fmt.Sprintf("%.2f GB", float64(totalSizeBytes)/(1024*1024*1024))
		}
		
		databaseStats := map[string]interface{}{
			"total_size_bytes":    totalSizeBytes,
			"total_size_readable": totalSizeReadable,
			"tables":              tables,
		}
		
		// Combine all stats
		stats := map[string]interface{}{
			"traces":   tracesStats,
			"agent":    agentStats,
			"database": databaseStats,
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
	filterQuery := r.URL.Query().Get("filter")
	sortBy := r.URL.Query().Get("sort")
	if sortBy == "" {
		sortBy = "created_at" // Default to created_at instead of time
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
	
	// Parse filter query if provided
	var filterAST *FilterAST
	var filterErr error
	if filterQuery != "" {
		filterAST, filterErr = ParseFilterQuery(filterQuery)
		if filterErr != nil {
			LogWarn("Failed to parse filter query in traces endpoint", map[string]interface{}{
				"filter": filterQuery,
				"error":  filterErr.Error(),
			})
			http.Error(w, fmt.Sprintf("invalid filter query: %v", filterErr), 400)
			return
		}
	}
	
	// Build query - use subquery to filter before aggregation
	// If HTTP request filters or filter query are present, we need to join with spans_full to access tags
	hasHttpFilters := scheme != "" || host != "" || uri != "" || queryString != ""
	hasFilterQuery := filterAST != nil
	needsFullJoin := hasHttpFilters || hasFilterQuery
		
	var baseQuery string
	if needsFullJoin {
		// Join spans_min with spans_full to access tags for HTTP filtering and filter query
		baseQuery = "SELECT DISTINCT sm.trace_id, sm.service, sm.start_ts, sm.end_ts, sm.duration_ms, sm.status, sm.language, sm.language_version, sm.framework, sm.framework_version "
		baseQuery += "FROM opa.spans_min sm "
		baseQuery += "INNER JOIN opa.spans_full sf ON sm.trace_id = sf.trace_id AND sm.span_id = sf.span_id "
		
		// Build tenant filter
		var tenantFilter string
		if ctx.IsAllTenants() {
			tenantFilter = "WHERE 1=1"
		} else {
			tenantFilter = fmt.Sprintf("WHERE (coalesce(nullif(sm.organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(sm.project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		
		// Add filter query if provided
		if hasFilterQuery {
			var filterWhere string
			if ctx.IsAllTenants() {
				filterWhere, filterErr = BuildClickHouseWhere(filterAST, "")
			} else {
				filterWhere, filterErr = BuildClickHouseWhere(filterAST, tenantFilter)
			}
			if filterErr != nil {
				LogError(filterErr, "Failed to build filter WHERE clause in traces", map[string]interface{}{
					"filter": filterQuery,
				})
				http.Error(w, fmt.Sprintf("failed to build filter: %v", filterErr), 500)
				return
			}
			
			// Extract the WHERE clause part
			if strings.HasPrefix(filterWhere, " WHERE ") {
				filterWhere = strings.TrimPrefix(filterWhere, " WHERE ")
			}
			// Replace field references to use sf. prefix for spans_full fields
			filterWhere = adaptFilterForJoin(filterWhere, "sf")
			tenantFilter = tenantFilter + " AND (" + filterWhere + ")"
		}
		
		baseQuery += tenantFilter
		
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
		// No HTTP filters or filter query, use simpler query on spans_min only
		baseQuery = "SELECT trace_id, service, start_ts, end_ts, duration_ms, status, language, language_version, framework, framework_version "
		
		// Build tenant filter
		var tenantFilter string
		if ctx.IsAllTenants() {
			tenantFilter = "WHERE 1=1"
		} else {
			tenantFilter = fmt.Sprintf("WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		
		// Add filter query if provided (but no join needed - filter should work on spans_min fields)
		if hasFilterQuery {
			filterWhere, filterErr := BuildClickHouseWhere(filterAST, tenantFilter)
			if filterErr != nil {
				LogError(filterErr, "Failed to build filter WHERE clause in traces", map[string]interface{}{
					"filter": filterQuery,
				})
				http.Error(w, fmt.Sprintf("failed to build filter: %v", filterErr), 500)
				return
			}
			if strings.HasPrefix(filterWhere, " WHERE ") {
				filterWhere = strings.TrimPrefix(filterWhere, " WHERE ")
			}
			tenantFilter = filterWhere
		}
		
		baseQuery += "FROM opa.spans_min " + tenantFilter
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
		// span_count is simply COUNT(*) since all spans are pre-constructed and stored
		// No need for complex JOIN or stack calculation - each span is stored as a separate row
		var query string
		if needsFullJoin {
			// baseQuery uses sm. prefix for HTTP filters
			query = "SELECT sm.trace_id, sm.service, min(sm.start_ts) as start_ts, max(sm.end_ts) as end_ts, "
			query += "toUnixTimestamp64Milli(max(sm.end_ts)) - toUnixTimestamp64Milli(min(sm.start_ts)) as duration_ms, count(*) as span_count, "
			query += "any(sm.status) as status, any(sm.language) as language, any(sm.language_version) as language_version, "
			query += "any(sm.framework) as framework, any(sm.framework_version) as framework_version "
			query += "FROM (" + baseQuery + ") sm "
			query += "GROUP BY sm.trace_id, sm.service"
		} else {
			// baseQuery doesn't use prefix - use subquery to avoid nested aggregates
			query = "SELECT trace_id, service, start_ts, end_ts, "
			query += "toUnixTimestamp64Milli(end_ts) - toUnixTimestamp64Milli(start_ts) as duration_ms, span_count, "
			query += "status, language, language_version, framework, framework_version "
			query += "FROM ("
			query += "SELECT trace_id, service, min(start_ts) as start_ts, max(end_ts) as end_ts, "
			query += "count(*) as span_count, "
			query += "any(status) as status, any(language) as language, any(language_version) as language_version, "
			query += "any(framework) as framework, any(framework_version) as framework_version "
			query += "FROM (" + baseQuery + ") "
			query += "GROUP BY trace_id, service"
			query += ")"
		}
		
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
		if sortBy == "time" || sortBy == "created_at" {
			query += fmt.Sprintf(" ORDER BY start_ts %s", strings.ToUpper(sortOrder))
		} else if sortBy == "duration" {
			query += fmt.Sprintf(" ORDER BY duration_ms %s", strings.ToUpper(sortOrder))
		} else if sortBy == "service" {
			query += fmt.Sprintf(" ORDER BY service %s", strings.ToUpper(sortOrder))
		} else {
			// Default to created_at DESC
			query += " ORDER BY start_ts DESC"
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
				"created_at":  getString(row, "start_ts"), // Map start_ts to created_at for consistency
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
						// Enrich with HTTP requests
						if httpStr := getString(row, "http"); httpStr != "" && httpStr != "[]" {
							var httpArray []interface{}
							if err := json.Unmarshal([]byte(httpStr), &httpArray); err == nil {
								span.Http = httpArray
							}
						}
						// Enrich with cache operations
						if cacheStr := getString(row, "cache"); cacheStr != "" && cacheStr != "[]" {
							var cacheArray []interface{}
							if err := json.Unmarshal([]byte(cacheStr), &cacheArray); err == nil {
								span.Cache = cacheArray
							}
						}
						// Enrich with Redis operations
						if redisStr := getString(row, "redis"); redisStr != "" && redisStr != "[]" {
							var redisArray []interface{}
							if err := json.Unmarshal([]byte(redisStr), &redisArray); err == nil {
								span.Redis = redisArray
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
	
	// Service Map endpoint: build from spans directly
	mux.HandleFunc("/api/service-map", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}

		ctx, _ := ExtractTenantContext(r, queryClient)
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		
		// Build tenant filter - skip tenant filtering if "all" is selected
		var tenantFilter string
		var tenantFilterChild string
		var tenantFilterParent string
		if ctx.IsAllTenants() {
			// No tenant filtering - show all data
			tenantFilter = ""
			tenantFilterChild = ""
			tenantFilterParent = ""
		} else {
			// Handle NULL/empty tenant IDs by treating them as default tenant
			tenantFilter = fmt.Sprintf("(coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			tenantFilterChild = fmt.Sprintf("(coalesce(nullif(child.organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(child.project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			tenantFilterParent = fmt.Sprintf("(coalesce(nullif(parent.organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(parent.project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		
		// Default to last 24 hours if not specified
		// URL decode the parameters first
		if timeFrom == "" {
			timeFrom = "now() - INTERVAL 24 HOUR"
		} else {
			// URL decode
			if decoded, err := url.QueryUnescape(timeFrom); err == nil {
				timeFrom = decoded
			}
			// If provided as datetime string (not a function), convert to DateTime
			// But don't convert if it's a ClickHouse function like now() - INTERVAL
			if !strings.HasPrefix(timeFrom, "now()") && !strings.HasPrefix(timeFrom, "parseDateTime") {
				// Convert ISO 8601 or other datetime strings to DateTime
				// Remove any existing quotes first
				timeFromClean := strings.Trim(timeFrom, "'\"")
				// parseDateTimeBestEffort can handle both ISO format and space-separated format
				// No need to convert format, just use parseDateTimeBestEffort directly
				timeFrom = fmt.Sprintf("parseDateTimeBestEffort('%s')", timeFromClean)
				LogInfo("Service map: Converted timeFrom", map[string]interface{}{
					"original": r.URL.Query().Get("from"),
					"converted": timeFrom,
				})
			}
		}
		if timeTo == "" {
			timeTo = "now()"
		} else {
			// URL decode
			if decoded, err := url.QueryUnescape(timeTo); err == nil {
				timeTo = decoded
			}
			if !strings.HasPrefix(timeTo, "now()") && !strings.HasPrefix(timeTo, "parseDateTime") {
				// Convert ISO 8601 or other datetime strings to DateTime
				// Remove any existing quotes first
				timeToClean := strings.Trim(timeTo, "'\"")
				// parseDateTimeBestEffort can handle both ISO format and space-separated format
				// No need to convert format, just use parseDateTimeBestEffort directly
				timeTo = fmt.Sprintf("parseDateTimeBestEffort('%s')", timeToClean)
				LogInfo("Service map: Converted timeTo", map[string]interface{}{
					"original": r.URL.Query().Get("to"),
					"converted": timeTo,
				})
			}
		}

		// Get health thresholds (only if not "all")
		var thresholdsQuery string
		if ctx.IsAllTenants() {
			// Use default thresholds when "all" is selected
			thresholdsQuery = ""
		} else {
			thresholdsQuery = fmt.Sprintf(`SELECT 
				degraded_error_rate,
				down_error_rate,
				degraded_latency_ms,
				down_latency_ms
				FROM opa.service_map_thresholds
				WHERE organization_id = '%s' AND project_id = '%s'
				ORDER BY updated_at DESC
				LIMIT 1`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		
		var thresholdRows []map[string]interface{}
		if thresholdsQuery != "" {
			thresholdRows, _ = queryClient.Query(thresholdsQuery)
		}
		degradedErrorRate := 10.0
		downErrorRate := 50.0
		degradedLatency := 1000.0
		downLatency := 5000.0
		if len(thresholdRows) > 0 {
			if val, ok := thresholdRows[0]["degraded_error_rate"].(float64); ok {
				degradedErrorRate = val
			}
			if val, ok := thresholdRows[0]["down_error_rate"].(float64); ok {
				downErrorRate = val
			}
			if val, ok := thresholdRows[0]["degraded_latency_ms"].(float64); ok {
				degradedLatency = val
			}
			if val, ok := thresholdRows[0]["down_latency_ms"].(float64); ok {
				downLatency = val
			}
		}

		// Helper function to calculate health status
		calculateHealthStatus := func(errorRate, avgLatency float64) string {
			if errorRate >= downErrorRate || avgLatency >= downLatency {
				return "down"
			}
			if errorRate >= degradedErrorRate || avgLatency >= degradedLatency {
				return "degraded"
			}
			return "healthy"
		}

		var nodes []map[string]interface{}
		edges := make([]map[string]interface{}, 0)
		services := make(map[string]bool)
		externalDeps := make(map[string]bool) // Track external dependency nodes
		
		// Build service map directly from spans
		// 1. Service-to-service relationships from parent-child spans
		// 2. External dependencies from SQL/HTTP/cache/Redis data in spans
		
		// Step 1: Build service-to-service relationships from parent-child spans
		// Use coalesce(nullif(...)) pattern to handle empty/null tenant IDs
		// timeFrom and timeTo are already converted to DateTime or ClickHouse functions above
		var serviceToServiceQuery string
		if tenantFilterChild != "" {
			serviceToServiceQuery = fmt.Sprintf(`SELECT 
				parent.service as from_service,
				child.service as to_service,
				avg(child.duration_ms) as avg_latency_ms,
				quantile(0.95)(child.duration_ms) as p95_latency_ms,
				quantile(0.99)(child.duration_ms) as p99_latency_ms,
				sum(CASE WHEN child.status = 'error' OR child.status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate,
				count(*) as call_count,
				count(*) / greatest(toFloat64(dateDiff('second', %s, %s)), 1.0) as throughput,
				sum(coalesce(child.bytes_sent, 0)) as bytes_sent,
				sum(coalesce(child.bytes_received, 0)) as bytes_received
				FROM opa.spans_min as child
				INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id AND child.trace_id = parent.trace_id
				WHERE %s AND %s
					AND child.service != parent.service
					AND child.service != ''
					AND parent.service != ''
					AND child.start_ts >= %s AND child.start_ts <= %s
				GROUP BY parent.service, child.service
				ORDER BY call_count DESC`,
				timeFrom, timeTo, // These are already converted to parseDateTimeBestEffort(...) or now() functions
				tenantFilterChild, tenantFilterParent,
				timeFrom, timeTo) // These are already converted to parseDateTimeBestEffort(...) or now() functions
		} else {
			serviceToServiceQuery = fmt.Sprintf(`SELECT 
				parent.service as from_service,
				child.service as to_service,
				avg(child.duration_ms) as avg_latency_ms,
				quantile(0.95)(child.duration_ms) as p95_latency_ms,
				quantile(0.99)(child.duration_ms) as p99_latency_ms,
				sum(CASE WHEN child.status = 'error' OR child.status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate,
				count(*) as call_count,
				count(*) / greatest(toFloat64(dateDiff('second', %s, %s)), 1.0) as throughput,
				sum(coalesce(child.bytes_sent, 0)) as bytes_sent,
				sum(coalesce(child.bytes_received, 0)) as bytes_received
				FROM opa.spans_min as child
				INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id AND child.trace_id = parent.trace_id
				WHERE child.service != parent.service
					AND child.service != ''
					AND parent.service != ''
					AND child.start_ts >= %s AND child.start_ts <= %s
				GROUP BY parent.service, child.service
				ORDER BY call_count DESC`,
				timeFrom, timeTo, // These are already converted to parseDateTimeBestEffort(...) or now() functions
				timeFrom, timeTo) // These are already converted to parseDateTimeBestEffort(...) or now() functions
		}

		serviceRows, serviceErr := queryClient.Query(serviceToServiceQuery)
		if serviceErr == nil && len(serviceRows) > 0 {
			for _, row := range serviceRows {
				fromService := getString(row, "from_service")
				toService := getString(row, "to_service")
				
				if fromService == "" || toService == "" {
					continue
				}
				
				services[fromService] = true
				services[toService] = true

				avgLatency := getFloat64(row, "avg_latency_ms")
				p95Latency := getFloat64(row, "p95_latency_ms")
				p99Latency := getFloat64(row, "p99_latency_ms")
				errorRate := getFloat64(row, "error_rate")
				callCount := getUint64(row, "call_count")
				throughput := getFloat64(row, "throughput")
				bytesSent := getUint64(row, "bytes_sent")
				bytesReceived := getUint64(row, "bytes_received")

				healthStatus := calculateHealthStatus(errorRate, avgLatency)

				// Calculate min/max latency and success rate
				var minLatencyQuery string
				if tenantFilterChild != "" {
					minLatencyQuery = fmt.Sprintf(`SELECT min(duration_ms) as min_latency
						FROM opa.spans_min as child
						INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id AND child.trace_id = parent.trace_id
						WHERE %s
							AND parent.service = '%s' AND child.service = '%s'
							AND child.start_ts >= %s AND child.start_ts <= %s`,
						tenantFilterChild,
						escapeSQL(fromService), escapeSQL(toService), timeFrom, timeTo)
				} else {
					minLatencyQuery = fmt.Sprintf(`SELECT min(duration_ms) as min_latency
						FROM opa.spans_min as child
						INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id AND child.trace_id = parent.trace_id
						WHERE parent.service = '%s' AND child.service = '%s'
							AND child.start_ts >= %s AND child.start_ts <= %s`,
						escapeSQL(fromService), escapeSQL(toService), timeFrom, timeTo)
				}
				minLatencyRows, _ := queryClient.Query(minLatencyQuery)
				minLatency := avgLatency
				if len(minLatencyRows) > 0 {
					if val := getFloat64(minLatencyRows[0], "min_latency"); val > 0 {
						minLatency = val
					}
				}
				
				var maxLatencyQuery string
				if tenantFilterChild != "" {
					maxLatencyQuery = fmt.Sprintf(`SELECT max(duration_ms) as max_latency
						FROM opa.spans_min as child
						INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id AND child.trace_id = parent.trace_id
						WHERE %s
							AND parent.service = '%s' AND child.service = '%s'
							AND child.start_ts >= %s AND child.start_ts <= %s`,
						tenantFilterChild,
						escapeSQL(fromService), escapeSQL(toService), timeFrom, timeTo)
				} else {
					maxLatencyQuery = fmt.Sprintf(`SELECT max(duration_ms) as max_latency
						FROM opa.spans_min as child
						INNER JOIN opa.spans_min as parent ON child.parent_id = parent.span_id AND child.trace_id = parent.trace_id
						WHERE parent.service = '%s' AND child.service = '%s'
							AND child.start_ts >= %s AND child.start_ts <= %s`,
						escapeSQL(fromService), escapeSQL(toService), timeFrom, timeTo)
				}
				maxLatencyRows, _ := queryClient.Query(maxLatencyQuery)
				maxLatency := avgLatency
				if len(maxLatencyRows) > 0 {
					if val := getFloat64(maxLatencyRows[0], "max_latency"); val > 0 {
						maxLatency = val
					}
				}
				
				successRate := 100.0 - errorRate
				if successRate < 0 {
					successRate = 0
				}

				edges = append(edges, map[string]interface{}{
					"from":            fromService,
					"to":              toService,
					"avg_latency_ms":  avgLatency,
					"min_latency_ms":  minLatency,
					"max_latency_ms":  maxLatency,
					"p95_latency_ms":  p95Latency,
					"p99_latency_ms":  p99Latency,
					"error_rate":      errorRate,
					"success_rate":   successRate,
					"call_count":      callCount,
					"throughput":      throughput,
					"bytes_sent":      bytesSent,
					"bytes_received":  bytesReceived,
					"health_status":   healthStatus,
					"dependency_type": "service",
				})
			}
		}
		
		// Step 2: Extract external dependencies from spans_full (SQL, HTTP, cache, Redis)
		// Database dependencies from SQL data
		// timeFrom and timeTo are already converted to DateTime or ClickHouse functions above
		var dbQuery string
		if tenantFilter != "" {
			dbQuery = fmt.Sprintf(`SELECT 
				service,
				sql
				FROM opa.spans_full
				WHERE %s
					AND sql != '' AND sql != '[]' AND sql != 'null'
					AND start_ts >= %s AND start_ts <= %s
				LIMIT 10000`,
				tenantFilter, timeFrom, timeTo)
		} else {
			dbQuery = fmt.Sprintf(`SELECT 
				service,
				sql
				FROM opa.spans_full
				WHERE sql != '' AND sql != '[]' AND sql != 'null'
					AND start_ts >= %s AND start_ts <= %s
				LIMIT 10000`,
				timeFrom, timeTo)
		}
		
		dbRows, dbErr := queryClient.Query(dbQuery)
		if dbErr != nil {
			LogError(dbErr, "Failed to query database dependencies", map[string]interface{}{
				"query": dbQuery,
			})
		}
		dbDepsMap := make(map[string]map[string]interface{}) // key: "service->target"
		
		for _, row := range dbRows {
			fromService := getString(row, "service")
			sqlData := getString(row, "sql")
			if fromService == "" || sqlData == "" {
				continue
			}
			
			var sqlArray []map[string]interface{}
			if err := json.Unmarshal([]byte(sqlData), &sqlArray); err != nil {
				continue
			}
			
			for _, sqlItem := range sqlArray {
				dbSystem := ""
				if db, ok := sqlItem["db_system"].(string); ok && db != "" {
					dbSystem = db
				} else if db, ok := sqlItem["dbSystem"].(string); ok && db != "" {
					dbSystem = db
				}
				
				if dbSystem == "" {
					continue
				}
				
				// Extract host if available
				dbHost := ""
				if host, ok := sqlItem["db_host"].(string); ok && host != "" {
					dbHost = host
				}
				
				// Build target identifier
				target := ""
				if dbHost != "" {
					target = fmt.Sprintf("%s://%s", dbSystem, dbHost)
				} else {
					target = fmt.Sprintf("db:%s", dbSystem)
				}
				
				key := fmt.Sprintf("%s->%s", fromService, target)
				dep, exists := dbDepsMap[key]
				if !exists {
					dep = map[string]interface{}{
						"from_service": fromService,
						"target":       target,
						"call_count":   int64(0),
						"total_duration": 0.0,
						"error_count":  int64(0),
					}
					dbDepsMap[key] = dep
				}
				
				dep["call_count"] = dep["call_count"].(int64) + 1
				if duration, ok := sqlItem["duration_ms"].(float64); ok {
					dep["total_duration"] = dep["total_duration"].(float64) + duration
				} else if duration, ok := sqlItem["duration"].(float64); ok {
					dep["total_duration"] = dep["total_duration"].(float64) + duration*1000
				}
				if status, ok := sqlItem["status"].(string); ok && (status == "error" || status == "0") {
					dep["error_count"] = dep["error_count"].(int64) + 1
				}
			}
		}
		
		// Convert database dependencies to edges
		for _, dep := range dbDepsMap {
			fromService := dep["from_service"].(string)
			target := dep["target"].(string)
			callCount := dep["call_count"].(int64)
			if callCount == 0 {
				continue
			}
			
			services[fromService] = true
			externalDeps[target] = true
			
			avgLatency := dep["total_duration"].(float64) / float64(callCount)
			errorRate := float64(dep["error_count"].(int64)) / float64(callCount) * 100.0
			healthStatus := calculateHealthStatus(errorRate, avgLatency)
			
			successRate := 100.0 - errorRate
			if successRate < 0 {
				successRate = 0
			}

			edges = append(edges, map[string]interface{}{
				"from":            fromService,
				"to":              target,
				"avg_latency_ms":  avgLatency,
				"min_latency_ms":  avgLatency,
				"max_latency_ms":  avgLatency,
				"p95_latency_ms":  avgLatency,
				"p99_latency_ms":  avgLatency,
				"error_rate":      errorRate,
				"success_rate":   successRate,
				"call_count":      callCount,
				"throughput":      0.0,
				"bytes_sent":      0,
				"bytes_received":  0,
				"health_status":   healthStatus,
				"dependency_type": "database",
				"dependency_target": target,
			})
		}
		
		// HTTP dependencies from spans_full
		// timeFrom and timeTo are already converted to DateTime or ClickHouse functions above
		var httpQuery string
		if tenantFilter != "" {
			httpQuery = fmt.Sprintf(`SELECT 
				service,
				http
				FROM opa.spans_full
				WHERE %s
					AND http != '' AND http != '[]' AND http != 'null'
					AND start_ts >= %s AND start_ts <= %s
				LIMIT 10000`,
				tenantFilter, timeFrom, timeTo)
		} else {
			httpQuery = fmt.Sprintf(`SELECT 
				service,
				http
				FROM opa.spans_full
				WHERE http != '' AND http != '[]' AND http != 'null'
					AND start_ts >= %s AND start_ts <= %s
				LIMIT 10000`,
				timeFrom, timeTo)
		}
		
		httpRows, httpErr := queryClient.Query(httpQuery)
		if httpErr != nil {
			LogError(httpErr, "Failed to query HTTP dependencies", map[string]interface{}{
				"query": httpQuery,
			})
		}
		httpDepsMap := make(map[string]map[string]interface{}) // key: "service->url"
		curlDepsMap := make(map[string]map[string]interface{}) // key: "service->url" for curl calls
		
		for _, row := range httpRows {
			fromService := getString(row, "service")
			httpData := getString(row, "http")
			if fromService == "" || httpData == "" {
				continue
			}
			
			var httpArray []map[string]interface{}
			if err := json.Unmarshal([]byte(httpData), &httpArray); err != nil {
				continue
			}
			
			for _, req := range httpArray {
				// Check if this is a curl call
				isCurl := false
				if reqType, ok := req["type"].(string); ok && reqType == "curl" {
					isCurl = true
				}
				
				url := ""
				if u, ok := req["url"].(string); ok && u != "" {
					url = u
				} else if u, ok := req["URL"].(string); ok && u != "" {
					url = u
				} else if host, ok := req["host"].(string); ok && host != "" {
					scheme := "http"
					if s, ok := req["scheme"].(string); ok && s != "" {
						scheme = s
					}
					url = fmt.Sprintf("%s://%s", scheme, host)
				}
				
				if url == "" {
					continue
				}
				
				// Skip internal calls to the same service (but allow calls to other services)
				// Only skip if the URL host exactly matches the service name
				if url == fromService || strings.HasPrefix(url, fromService+":") || strings.HasPrefix(url, fromService+"/") {
					continue
				}
				
				// Extract just the base URL (host) for grouping
				baseURL := url
				if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
					parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://"), "/")
					if len(parts) > 0 {
						baseURL = strings.Split(url, "/")[0] + "//" + parts[0]
					}
				}
				
				// Use separate maps for curl vs regular HTTP
				var targetMap map[string]map[string]interface{}
				if isCurl {
					targetMap = curlDepsMap
				} else {
					targetMap = httpDepsMap
				}
				
				key := fmt.Sprintf("%s->%s", fromService, baseURL)
				dep, exists := targetMap[key]
				if !exists {
					dep = map[string]interface{}{
						"from_service": fromService,
						"target":       baseURL,
						"call_count":   int64(0),
						"total_duration": 0.0,
						"error_count":  int64(0),
						"total_bytes_sent": int64(0),
						"total_bytes_received": int64(0),
						"is_curl":      isCurl,
					}
					targetMap[key] = dep
				}
				
				dep["call_count"] = dep["call_count"].(int64) + 1
				if duration, ok := req["duration_ms"].(float64); ok {
					dep["total_duration"] = dep["total_duration"].(float64) + duration
				} else if duration, ok := req["duration"].(float64); ok {
					dep["total_duration"] = dep["total_duration"].(float64) + duration*1000
				}
				if status, ok := req["status"].(string); ok && (status == "error" || status == "0") {
					dep["error_count"] = dep["error_count"].(int64) + 1
				} else 				if statusCode, ok := req["status_code"].(float64); ok && statusCode >= 400 {
					dep["error_count"] = dep["error_count"].(int64) + 1
				}
				// Extract bytes sent - prefer bytes_sent, fallback to request_size
				if sent, ok := req["bytes_sent"].(float64); ok {
					dep["total_bytes_sent"] = dep["total_bytes_sent"].(int64) + int64(sent)
				} else if sent, ok := req["request_size"].(float64); ok {
					dep["total_bytes_sent"] = dep["total_bytes_sent"].(int64) + int64(sent)
				} else if sent, ok := req["curl_bytes_sent"].(float64); ok {
					dep["total_bytes_sent"] = dep["total_bytes_sent"].(int64) + int64(sent)
				}
				// Extract bytes received - prefer bytes_received, fallback to response_size
				if recv, ok := req["bytes_received"].(float64); ok {
					dep["total_bytes_received"] = dep["total_bytes_received"].(int64) + int64(recv)
				} else if recv, ok := req["response_size"].(float64); ok {
					dep["total_bytes_received"] = dep["total_bytes_received"].(int64) + int64(recv)
				} else if recv, ok := req["curl_bytes_received"].(float64); ok {
					dep["total_bytes_received"] = dep["total_bytes_received"].(int64) + int64(recv)
				}
			}
		}
		
		// Convert HTTP dependencies to edges
		for _, dep := range httpDepsMap {
			fromService := dep["from_service"].(string)
			target := dep["target"].(string)
			callCount := dep["call_count"].(int64)
			if callCount == 0 {
				continue
			}
			
			services[fromService] = true
			externalDeps[target] = true
			
			avgLatency := dep["total_duration"].(float64) / float64(callCount)
			errorRate := float64(dep["error_count"].(int64)) / float64(callCount) * 100.0
			healthStatus := calculateHealthStatus(errorRate, avgLatency)
			
			successRate := 100.0 - errorRate
			if successRate < 0 {
				successRate = 0
			}

			edges = append(edges, map[string]interface{}{
				"from":            fromService,
				"to":              target,
				"avg_latency_ms":  avgLatency,
				"min_latency_ms":  avgLatency,
				"max_latency_ms":  avgLatency,
				"p95_latency_ms":  avgLatency,
				"p99_latency_ms":  avgLatency,
				"error_rate":      errorRate,
				"success_rate":   successRate,
				"call_count":      callCount,
				"throughput":      0.0,
				"bytes_sent":      dep["total_bytes_sent"],
				"bytes_received":  dep["total_bytes_received"],
				"health_status":   healthStatus,
				"dependency_type": "http",
				"dependency_target": target,
			})
		}
		
		// Convert curl dependencies to edges (with curl type)
		for _, dep := range curlDepsMap {
			fromService := dep["from_service"].(string)
			target := dep["target"].(string)
			callCount := dep["call_count"].(int64)
			if callCount == 0 {
				continue
			}
			
			services[fromService] = true
			externalDeps[target] = true
			
			avgLatency := dep["total_duration"].(float64) / float64(callCount)
			errorRate := float64(dep["error_count"].(int64)) / float64(callCount) * 100.0
			healthStatus := calculateHealthStatus(errorRate, avgLatency)
			
			successRate := 100.0 - errorRate
			if successRate < 0 {
				successRate = 0
			}

			edges = append(edges, map[string]interface{}{
				"from":            fromService,
				"to":              target,
				"avg_latency_ms":  avgLatency,
				"min_latency_ms":  avgLatency,
				"max_latency_ms":  avgLatency,
				"p95_latency_ms":  avgLatency,
				"p99_latency_ms":  avgLatency,
				"error_rate":      errorRate,
				"success_rate":   successRate,
				"call_count":      callCount,
				"throughput":      0.0,
				"bytes_sent":      dep["total_bytes_sent"],
				"bytes_received":  dep["total_bytes_received"],
				"health_status":   healthStatus,
				"dependency_type": "curl",
				"dependency_target": target,
			})
		}
		
		// Always ensure services with external dependencies are included
		// Also add all services that have spans in the time range
		var allServicesQuery string
		if tenantFilter != "" {
			allServicesQuery = fmt.Sprintf(`SELECT DISTINCT service
				FROM opa.spans_min
				WHERE %s
					AND start_ts >= %s AND start_ts <= %s`,
				tenantFilter, timeFrom, timeTo)
		} else {
			allServicesQuery = fmt.Sprintf(`SELECT DISTINCT service
				FROM opa.spans_min
				WHERE start_ts >= %s AND start_ts <= %s`,
				timeFrom, timeTo)
		}
		
		serviceRows, _ = queryClient.Query(allServicesQuery)
		for _, row := range serviceRows {
			service := getString(row, "service")
			if service != "" {
				services[service] = true
			}
		}
		
		// Also add services that have external dependencies (from edges we just created)
		for _, edge := range edges {
			if from, ok := edge["from"].(string); ok && from != "" {
				services[from] = true
			}
		}

		// Create nodes for all services with complete metrics
		for service := range services {
			// Get comprehensive service stats
			// timeFrom and timeTo are already converted to DateTime or ClickHouse functions above
			var statsQuery string
			if tenantFilter != "" {
				statsQuery = fmt.Sprintf(`SELECT 
					count(*) as total_spans,
					avg(duration_ms) as avg_duration,
					min(duration_ms) as min_duration,
					max(duration_ms) as max_duration,
					quantile(0.95)(duration_ms) as p95_duration,
					quantile(0.99)(duration_ms) as p99_duration,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate,
					sum(coalesce(bytes_sent, 0)) + sum(coalesce(bytes_received, 0)) as total_traffic,
					count(*) / greatest(toFloat64(dateDiff('second', %s, %s)), 1.0) as throughput
					FROM opa.spans_min 
					WHERE %s AND service = '%s' AND start_ts >= %s AND start_ts <= %s`,
					timeFrom, timeTo, // These are already converted to parseDateTimeBestEffort(...) or now() functions
					tenantFilter, escapeSQL(service), timeFrom, timeTo) // These are already converted
			} else {
				statsQuery = fmt.Sprintf(`SELECT 
					count(*) as total_spans,
					avg(duration_ms) as avg_duration,
					min(duration_ms) as min_duration,
					max(duration_ms) as max_duration,
					quantile(0.95)(duration_ms) as p95_duration,
					quantile(0.99)(duration_ms) as p99_duration,
					sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate,
					sum(coalesce(bytes_sent, 0)) + sum(coalesce(bytes_received, 0)) as total_traffic,
					count(*) / greatest(toFloat64(dateDiff('second', %s, %s)), 1.0) as throughput
					FROM opa.spans_min 
					WHERE service = '%s' AND start_ts >= %s AND start_ts <= %s`,
					timeFrom, timeTo, // These are already converted to parseDateTimeBestEffort(...) or now() functions
					escapeSQL(service), timeFrom, timeTo) // These are already converted
			}
			
			statsRows, _ := queryClient.Query(statsQuery)
			healthStatus := "healthy"
			var avgDuration, errorRate, minDuration, maxDuration, p95Duration, p99Duration, throughput, totalTraffic float64
			var totalSpans uint64
			if len(statsRows) > 0 {
				errorRate = getFloat64(statsRows[0], "error_rate")
				avgDuration = getFloat64(statsRows[0], "avg_duration")
				minDuration = getFloat64(statsRows[0], "min_duration")
				maxDuration = getFloat64(statsRows[0], "max_duration")
				p95Duration = getFloat64(statsRows[0], "p95_duration")
				p99Duration = getFloat64(statsRows[0], "p99_duration")
				throughput = getFloat64(statsRows[0], "throughput")
				totalTraffic = getFloat64(statsRows[0], "total_traffic")
				totalSpans = getUint64(statsRows[0], "total_spans")
				healthStatus = calculateHealthStatus(errorRate, avgDuration)
			}

			// Count incoming and outgoing calls
			incomingCalls := uint64(0)
			outgoingCalls := uint64(0)
			for _, edge := range edges {
				if to, ok := edge["to"].(string); ok && to == service {
					if count, ok := edge["call_count"].(uint64); ok {
						incomingCalls += count
					} else if count, ok := edge["call_count"].(int64); ok {
						incomingCalls += uint64(count)
					}
				}
				if from, ok := edge["from"].(string); ok && from == service {
					if count, ok := edge["call_count"].(uint64); ok {
						outgoingCalls += count
					} else if count, ok := edge["call_count"].(int64); ok {
						outgoingCalls += uint64(count)
					}
				}
			}

			nodeData := map[string]interface{}{
				"id":              service,
				"service":         service,
				"health_status":   healthStatus,
				"avg_duration":    avgDuration,
				"error_rate":      errorRate,
				"total_spans":     totalSpans,
				"incoming_calls":  incomingCalls,
				"outgoing_calls":  outgoingCalls,
				"node_type":       "service",
			}
			
			// Add optional fields only if they have meaningful values
			if minDuration > 0 {
				nodeData["min_duration"] = minDuration
			}
			if maxDuration > 0 {
				nodeData["max_duration"] = maxDuration
			}
			if p95Duration > 0 {
				nodeData["p95_duration"] = p95Duration
			}
			if p99Duration > 0 {
				nodeData["p99_duration"] = p99Duration
			}
			if throughput > 0 {
				nodeData["throughput"] = throughput
			}
			if totalTraffic > 0 {
				nodeData["total_traffic"] = totalTraffic
			}
			
			nodes = append(nodes, nodeData)
		}

		// Create nodes for external dependencies
		// First, check edges to determine if a dependency is curl
		curlTargets := make(map[string]bool)
		for _, edge := range edges {
			if depType, ok := edge["dependency_type"].(string); ok && depType == "curl" {
				if to, ok := edge["to"].(string); ok {
					curlTargets[to] = true
				}
			}
		}
		
		for extDep := range externalDeps {
			// Determine dependency type from the target name and edge types
			depType := "external"
			if curlTargets[extDep] {
				depType = "curl"
			} else if strings.HasPrefix(extDep, "db:") {
				depType = "database"
			} else if strings.HasPrefix(extDep, "http://") || strings.HasPrefix(extDep, "https://") {
				depType = "http"
			} else if strings.HasPrefix(extDep, "redis://") || extDep == "redis" {
				depType = "redis"
			} else if strings.HasPrefix(extDep, "cache:") || strings.HasPrefix(extDep, "cache://") {
				depType = "cache"
			}

			// Calculate health status from edges
			var totalCalls uint64
			var totalErrors uint64
			var totalLatency float64
			for _, edge := range edges {
				if to, ok := edge["to"].(string); ok && to == extDep {
					if count, ok := edge["call_count"].(uint64); ok {
						totalCalls += count
					}
					if latency, ok := edge["avg_latency_ms"].(float64); ok {
						totalLatency += latency
					}
					if errRate, ok := edge["error_rate"].(float64); ok {
						totalErrors += uint64(float64(totalCalls) * errRate / 100.0)
					}
				}
			}

			errorRate := 0.0
			avgLatency := 0.0
			if totalCalls > 0 {
				errorRate = float64(totalErrors) / float64(totalCalls) * 100.0
				avgLatency = totalLatency / float64(totalCalls)
			}
			healthStatus := calculateHealthStatus(errorRate, avgLatency)

			// Count incoming calls for external dependencies
			incomingCalls := uint64(0)
			for _, edge := range edges {
				if to, ok := edge["to"].(string); ok && to == extDep {
					if count, ok := edge["call_count"].(uint64); ok {
						incomingCalls += count
					} else if count, ok := edge["call_count"].(int64); ok {
						incomingCalls += uint64(count)
					}
				}
			}

			nodes = append(nodes, map[string]interface{}{
				"id":            extDep,
				"service":       extDep,
				"health_status": healthStatus,
				"node_type":     depType,
				"avg_duration":  avgLatency,
				"error_rate":    errorRate,
				"incoming_calls": incomingCalls,
				"outgoing_calls": uint64(0), // External deps don't make outgoing calls
			})
		}

		// Ensure nodes is always an array, not null
		if nodes == nil {
			nodes = []map[string]interface{}{}
		}
		if edges == nil {
			edges = []map[string]interface{}{}
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"nodes": nodes,
			"edges": edges,
		})
	})

	// Service Map Health Thresholds endpoint
	mux.HandleFunc("/api/service-map/thresholds", func(w http.ResponseWriter, r *http.Request) {
		ctx, _ := ExtractTenantContext(r, queryClient)
		
		switch r.Method {
		case "GET":
			query := fmt.Sprintf(`SELECT 
				degraded_error_rate,
				down_error_rate,
				degraded_latency_ms,
				down_latency_ms,
				updated_at
				FROM opa.service_map_thresholds
				WHERE organization_id = '%s' AND project_id = '%s'
				ORDER BY updated_at DESC
				LIMIT 1`,
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to fetch health thresholds", map[string]interface{}{
					"org_id": ctx.OrganizationID,
					"project_id": ctx.ProjectID,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			
			if len(rows) > 0 {
				json.NewEncoder(w).Encode(rows[0])
			} else {
				// Return defaults if not found
				json.NewEncoder(w).Encode(map[string]interface{}{
					"degraded_error_rate": 10.0,
					"down_error_rate": 50.0,
					"degraded_latency_ms": 1000.0,
					"down_latency_ms": 5000.0,
				})
			}
			
		case "POST", "PUT":
			var thresholds map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&thresholds); err != nil {
				http.Error(w, fmt.Sprintf("invalid JSON: %v", err), 400)
				return
			}
			
			degradedErrorRate := 10.0
			downErrorRate := 50.0
			degradedLatency := 1000.0
			downLatency := 5000.0
			
			if val, ok := thresholds["degraded_error_rate"].(float64); ok {
				degradedErrorRate = val
			}
			if val, ok := thresholds["down_error_rate"].(float64); ok {
				downErrorRate = val
			}
			if val, ok := thresholds["degraded_latency_ms"].(float64); ok {
				degradedLatency = val
			}
			if val, ok := thresholds["down_latency_ms"].(float64); ok {
				downLatency = val
			}
			
			query := fmt.Sprintf(`INSERT INTO opa.service_map_thresholds 
				(organization_id, project_id, degraded_error_rate, down_error_rate, degraded_latency_ms, down_latency_ms, updated_at)
				VALUES ('%s', '%s', %.2f, %.2f, %.2f, %.2f, now())`,
				escapeSQL(ctx.OrganizationID),
				escapeSQL(ctx.ProjectID),
				degradedErrorRate,
				downErrorRate,
				degradedLatency,
				downLatency)
			
			if err := queryClient.Execute(query); err != nil {
				LogError(err, "Failed to update health thresholds", map[string]interface{}{
					"org_id": ctx.OrganizationID,
					"project_id": ctx.ProjectID,
				})
				http.Error(w, fmt.Sprintf("query error: %v", err), 500)
				return
			}
			
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"degraded_error_rate": degradedErrorRate,
				"down_error_rate": downErrorRate,
				"degraded_latency_ms": degradedLatency,
				"down_latency_ms": downLatency,
			})
			
		default:
			http.Error(w, "method not allowed", 405)
		}
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
		
		// Get HTTP/cURL calls for this service
		if len(parts) >= 5 && parts[4] == "http-calls" {
			ctx, _ := ExtractTenantContext(r, queryClient)
			timeFrom := r.URL.Query().Get("from")
			timeTo := r.URL.Query().Get("to")
			limit := r.URL.Query().Get("limit")
			if limit == "" {
				limit = "100"
			}
			limitInt, _ := strconv.Atoi(limit)
			if limitInt > 500 {
				limitInt = 500
			}
			
			// Query spans_full to get HTTP requests
			var query string
			if ctx.IsAllTenants() {
				query = fmt.Sprintf(`SELECT 
					http,
					start_ts,
					duration_ms,
					status,
					trace_id,
					span_id
					FROM opa.spans_full 
					WHERE service = '%s' AND http != '' AND http != '[]' AND http != 'null'`,
					escapeSQL(service))
			} else {
				query = fmt.Sprintf(`SELECT 
					http,
					start_ts,
					duration_ms,
					status,
					trace_id,
					span_id
					FROM opa.spans_full 
					WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s') 
						AND service = '%s' AND http != '' AND http != '[]' AND http != 'null'`,
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID), escapeSQL(service))
			}
			
			if timeFrom != "" {
				query += fmt.Sprintf(" AND start_ts >= '%s'", escapeSQL(timeFrom))
			} else {
				query += " AND start_ts >= now() - INTERVAL 24 HOUR"
			}
			if timeTo != "" {
				query += fmt.Sprintf(" AND start_ts <= '%s'", escapeSQL(timeTo))
			}
			query += fmt.Sprintf(" ORDER BY start_ts DESC LIMIT %d", limitInt)
			
			rows, err := queryClient.Query(query)
			if err != nil {
				LogError(err, "Failed to fetch HTTP calls for service", map[string]interface{}{
					"service": service,
				})
				http.Error(w, "failed to fetch HTTP calls", 500)
				return
			}
			
			// Aggregate HTTP calls by URL
			type HttpCallStats struct {
				URL              string
				Method           string
				CallCount        int64
				TotalDuration    float64
				ErrorCount       int64
				TotalBytesSent   int64
				TotalBytesRecv   int64
				MinDuration      float64
				MaxDuration      float64
			}
			httpCallsMap := make(map[string]*HttpCallStats)
			totalCalls := int64(0)
			
			for _, row := range rows {
				httpData := getString(row, "http")
				if httpData == "" {
					continue
				}
				
				// Parse HTTP requests JSON array
				var httpRequests []map[string]interface{}
				if err := json.Unmarshal([]byte(httpData), &httpRequests); err != nil {
					continue
				}
				
				for _, req := range httpRequests {
					url := ""
					if u, ok := req["url"].(string); ok && u != "" {
						url = u
					} else if u, ok := req["URL"].(string); ok && u != "" {
						url = u
					}
					if url == "" {
						continue
					}
					
					method := "GET"
					if m, ok := req["method"].(string); ok && m != "" {
						method = m
					}
					
					key := fmt.Sprintf("%s %s", method, url)
					
					call, exists := httpCallsMap[key]
					if !exists {
						call = &HttpCallStats{
							URL:           url,
							Method:        method,
							MinDuration:   999999.0,
							MaxDuration:   0.0,
						}
						httpCallsMap[key] = call
					}
					
					call.CallCount++
					totalCalls++
					
					var duration float64
					if d, ok := req["duration_ms"].(float64); ok {
						duration = d
					} else if d, ok := req["duration"].(float64); ok {
						duration = d * 1000.0
					}
					
					if duration > 0 {
						call.TotalDuration += duration
						if duration < call.MinDuration {
							call.MinDuration = duration
						}
						if duration > call.MaxDuration {
							call.MaxDuration = duration
						}
					}
					
					statusCode := 0
					if sc, ok := req["status_code"].(float64); ok {
						statusCode = int(sc)
					} else if sc, ok := req["statusCode"].(float64); ok {
						statusCode = int(sc)
					}
					
					if statusCode >= 400 {
						call.ErrorCount++
					} else if err, ok := req["error"].(string); ok && err != "" {
						call.ErrorCount++
					}
					
					// Extract bytes sent - prefer bytes_sent, fallback to request_size
					if sent, ok := req["bytes_sent"].(float64); ok {
						call.TotalBytesSent += int64(sent)
					} else if sent, ok := req["request_size"].(float64); ok {
						call.TotalBytesSent += int64(sent)
					} else if sent, ok := req["curl_bytes_sent"].(float64); ok {
						call.TotalBytesSent += int64(sent)
					}
					// Extract bytes received - prefer bytes_received, fallback to response_size
					if recv, ok := req["bytes_received"].(float64); ok {
						call.TotalBytesRecv += int64(recv)
					} else if recv, ok := req["response_size"].(float64); ok {
						call.TotalBytesRecv += int64(recv)
					} else if recv, ok := req["curl_bytes_received"].(float64); ok {
						call.TotalBytesRecv += int64(recv)
					}
				}
			}
			
			// Convert to array and calculate averages
			httpCalls := make([]map[string]interface{}, 0, len(httpCallsMap))
			for _, call := range httpCallsMap {
				avgDuration := 0.0
				errorRate := 0.0
				if call.CallCount > 0 {
					avgDuration = call.TotalDuration / float64(call.CallCount)
					errorRate = float64(call.ErrorCount) / float64(call.CallCount) * 100.0
					if call.MinDuration == 999999.0 {
						call.MinDuration = 0.0
					}
				}
				
				httpCalls = append(httpCalls, map[string]interface{}{
					"url":                call.URL,
					"method":            call.Method,
					"call_count":        call.CallCount,
					"avg_duration":     avgDuration,
					"min_duration":     call.MinDuration,
					"max_duration":     call.MaxDuration,
					"error_count":      call.ErrorCount,
					"error_rate":       errorRate,
					"total_bytes_sent": call.TotalBytesSent,
					"total_bytes_received": call.TotalBytesRecv,
				})
			}
			
			// Sort by call count descending
			sort.Slice(httpCalls, func(i, j int) bool {
				ci, _ := httpCalls[i]["call_count"].(int64)
				cj, _ := httpCalls[j]["call_count"].(int64)
				return ci > cj
			})
			
			json.NewEncoder(w).Encode(map[string]interface{}{
				"service":     service,
				"total_calls": totalCalls,
				"http_calls":  httpCalls,
			})
			return
		}
		
		http.Error(w, "not found", 404)
	})
	
	// Get HTTP calls across all services (or filtered by service)
	mux.HandleFunc("/api/http-calls", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
	ctx, _ := ExtractTenantContext(r, queryClient)
	timeFrom := r.URL.Query().Get("from")
	timeTo := r.URL.Query().Get("to")
	serviceFilter := r.URL.Query().Get("service")
	filterQuery := r.URL.Query().Get("filter")
	limit := r.URL.Query().Get("limit")
	offset := r.URL.Query().Get("offset")
	sortBy := r.URL.Query().Get("sort")
	if sortBy == "" {
		sortBy = "last_created_at" // Default to last_created_at for grouped items
	}
	sortOrder := r.URL.Query().Get("order")
	if sortOrder == "" {
		sortOrder = "desc"
	}
	
	if limit == "" {
		limit = "100"
	}
	limitInt, _ := strconv.Atoi(limit)
	if limitInt > 500 {
		limitInt = 500
	}
	if limitInt < 1 {
		limitInt = 100
	}
	
	offsetInt := 0
	if offset != "" {
		offsetInt, _ = strconv.Atoi(offset)
		if offsetInt < 0 {
			offsetInt = 0
		}
	}
	
	// Build base WHERE clause
	var baseWhere string
	if ctx.IsAllTenants() {
		baseWhere = "WHERE ((http != '' AND http != '[]' AND http != 'null') OR (tags != '' AND tags != '{}' AND tags LIKE '%%http_request%%'))"
	} else {
		baseWhere = fmt.Sprintf("WHERE (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s') AND ((http != '' AND http != '[]' AND http != 'null') OR (tags != '' AND tags != '{}' AND tags LIKE '%%http_request%%'))",
			escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
	}
	
		// Parse and apply filter query if provided
		if filterQuery != "" {
			filterAST, err := ParseFilterQuery(filterQuery)
			if err != nil {
				LogWarn("Failed to parse filter query", map[string]interface{}{
					"filter": filterQuery,
					"error":  err.Error(),
				})
				http.Error(w, fmt.Sprintf("invalid filter query: %v", err), 400)
				return
			}
			
			if filterAST != nil {
				// Build tenant filter for filter query builder
				var tenantFilterForFilter string
				if ctx.IsAllTenants() {
					tenantFilterForFilter = ""
				} else {
					orgID := ctx.OrganizationID
					if orgID == "" {
						orgID = "default-org"
					}
					projID := ctx.ProjectID
					if projID == "" {
						projID = "default-project"
					}
					tenantFilterForFilter = fmt.Sprintf("(coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
						escapeSQL(orgID), escapeSQL(projID))
				}
				
				filterWhere, err := BuildClickHouseWhere(filterAST, tenantFilterForFilter)
				if err != nil {
					LogError(err, "Failed to build filter WHERE clause", map[string]interface{}{
						"filter":         filterQuery,
						"tenantFilter":   tenantFilterForFilter,
						"organizationID": ctx.OrganizationID,
						"projectID":      ctx.ProjectID,
					})
					http.Error(w, fmt.Sprintf("failed to build filter: %v", err), 500)
					return
				}
				
				// Combine base WHERE with filter WHERE
				if strings.HasPrefix(filterWhere, " WHERE ") {
					filterWhere = strings.TrimPrefix(filterWhere, " WHERE ")
				}
				if filterWhere != "" {
					baseWhere = baseWhere + " AND (" + filterWhere + ")"
				}
			}
		}
	
	// Build query to get HTTP requests
	// Include both http field (outgoing requests) and tags field (incoming requests)
	query := fmt.Sprintf(`SELECT 
		http,
		tags,
		net,
		service,
		start_ts,
		duration_ms,
		status,
		trace_id,
		span_id
		FROM opa.spans_full 
		%s`, baseWhere)
	
	if serviceFilter != "" {
		query += fmt.Sprintf(" AND service = '%s'", escapeSQL(serviceFilter))
	}
	
	if timeFrom != "" {
		query += fmt.Sprintf(" AND start_ts >= '%s'", escapeSQL(timeFrom))
	}
	if timeTo != "" {
		query += fmt.Sprintf(" AND start_ts <= '%s'", escapeSQL(timeTo))
	}
		
		// Apply pagination - we'll fetch more rows than needed to aggregate properly
		// Since we're grouping, we need to fetch all matching rows, aggregate, then paginate
		query += " ORDER BY start_ts DESC"
		
		rows, err := queryClient.Query(query)
		if err != nil {
			LogError(err, "Failed to fetch HTTP calls", map[string]interface{}{
				"service": serviceFilter,
			})
			http.Error(w, "failed to fetch HTTP calls", 500)
			return
		}
		
		// Aggregate HTTP calls by URL + Method
		type HttpCallStats struct {
			URL              string
			URI              string
			Method           string
			Service          string
			CallCount        int64
			TotalDuration    float64
			ErrorCount       int64
			TotalBytesSent   int64
			TotalBytesRecv   int64
			MinDuration      float64
			MaxDuration      float64
			StatusCodeCounts map[int]int64 // Track status code frequencies
			MostCommonStatus int           // Most common status code
			LastCreatedAt    string        // Most recent start_ts in the group
		}
		httpCallsMap := make(map[string]*HttpCallStats)
		totalCalls := int64(0)
		
		// Helper function to process HTTP requests from either http field or tags
		processHttpRequestFromField := func(req map[string]interface{}, httpCallsMap map[string]*HttpCallStats, serviceName string, totalCalls *int64, startTs string) {
			requestURL := ""
			if u, ok := req["url"].(string); ok && u != "" {
				requestURL = u
			} else if u, ok := req["URL"].(string); ok && u != "" {
				requestURL = u
			}
			if requestURL == "" {
				return
			}
			
			method := "GET"
			if m, ok := req["method"].(string); ok && m != "" {
				method = m
			}
			
			// Extract URI from URL (remove scheme and host)
			uri := ""
			if u, ok := req["uri"].(string); ok && u != "" {
				uri = u
				// Add query string if present
				if qs, ok := req["query_string"].(string); ok && qs != "" {
					uri = uri + "?" + qs
				}
			} else {
				// Try to extract URI from URL
				if strings.HasPrefix(requestURL, "http://") || strings.HasPrefix(requestURL, "https://") {
					if parsedURL, err := url.Parse(requestURL); err == nil {
						uri = parsedURL.Path
						if parsedURL.RawQuery != "" {
							uri = uri + "?" + parsedURL.RawQuery
						}
					} else {
						uri = requestURL
					}
				} else {
					uri = requestURL
				}
			}
			
			// Group by method + URI (service is included in response but not in grouping key)
			key := fmt.Sprintf("%s %s", method, uri)
			
			call, exists := httpCallsMap[key]
			if !exists {
				call = &HttpCallStats{
					URL:             requestURL,
					URI:             uri,
					Method:          method,
					Service:         serviceName,
					MinDuration:     999999.0,
					MaxDuration:     0.0,
					StatusCodeCounts: make(map[int]int64),
					MostCommonStatus: 0,
					LastCreatedAt:    "",
				}
				httpCallsMap[key] = call
			}
			
			// Update last_created_at if this start_ts is more recent
			if startTs != "" {
				if call.LastCreatedAt == "" || startTs > call.LastCreatedAt {
					call.LastCreatedAt = startTs
				}
			}
			
			call.CallCount++
			*totalCalls++
			
			var duration float64
			if d, ok := req["duration_ms"].(float64); ok {
				duration = d
			} else if d, ok := req["duration"].(float64); ok {
				duration = d * 1000.0
			}
			
			if duration > 0 {
				call.TotalDuration += duration
				if duration < call.MinDuration {
					call.MinDuration = duration
				}
				if duration > call.MaxDuration {
					call.MaxDuration = duration
				}
			}
			
			statusCode := 0
			if sc, ok := req["status_code"].(float64); ok {
				statusCode = int(sc)
			} else if sc, ok := req["statusCode"].(float64); ok {
				statusCode = int(sc)
			}
			
			// Track status code frequency
			if statusCode > 0 {
				call.StatusCodeCounts[statusCode]++
				// Update most common status code
				if call.StatusCodeCounts[statusCode] > call.StatusCodeCounts[call.MostCommonStatus] {
					call.MostCommonStatus = statusCode
				}
			}
			
			if statusCode >= 400 {
				call.ErrorCount++
			} else if err, ok := req["error"].(string); ok && err != "" {
				call.ErrorCount++
			}
			
			// Extract bytes sent
			if sent, ok := req["bytes_sent"].(float64); ok {
				call.TotalBytesSent += int64(sent)
			} else if sent, ok := req["request_size"].(float64); ok {
				call.TotalBytesSent += int64(sent)
			} else if sent, ok := req["curl_bytes_sent"].(float64); ok {
				call.TotalBytesSent += int64(sent)
			}
			// Extract bytes received
			if recv, ok := req["bytes_received"].(float64); ok {
				call.TotalBytesRecv += int64(recv)
			} else if recv, ok := req["response_size"].(float64); ok {
				call.TotalBytesRecv += int64(recv)
			} else if recv, ok := req["curl_bytes_received"].(float64); ok {
				call.TotalBytesRecv += int64(recv)
			}
		}
		
		for _, row := range rows {
			serviceName := getString(row, "service")
			durationMs := getFloat64(row, "duration_ms")
			spanStatus := getString(row, "status")
			startTs := getString(row, "start_ts")
			
			// Extract net field for fallback bytes
			var netBytesSent, netBytesRecv int64
			netData := getString(row, "net")
			if netData != "" && netData != "{}" {
				var netMap map[string]interface{}
				if err := json.Unmarshal([]byte(netData), &netMap); err == nil {
					if sent, ok := netMap["bytes_sent"].(float64); ok {
						netBytesSent = int64(sent)
					}
					if recv, ok := netMap["bytes_received"].(float64); ok {
						netBytesRecv = int64(recv)
					}
				}
			}
			
			// First, process outgoing HTTP requests from http field
			httpData := getString(row, "http")
			if httpData != "" && httpData != "[]" && httpData != "null" {
				// Parse HTTP requests JSON array
				var httpRequests []map[string]interface{}
				if err := json.Unmarshal([]byte(httpData), &httpRequests); err == nil {
					for _, req := range httpRequests {
						processHttpRequestFromField(req, httpCallsMap, serviceName, &totalCalls, startTs)
					}
				}
			}
			
			// Second, process incoming HTTP requests from tags field
			tagsData := getString(row, "tags")
			if tagsData != "" && tagsData != "{}" {
				var tagsMap map[string]interface{}
				if err := json.Unmarshal([]byte(tagsData), &tagsMap); err == nil {
					if httpRequest, ok := tagsMap["http_request"].(map[string]interface{}); ok {
						// Build request object from tags
						req := make(map[string]interface{})
						
						// Build URL from http_request components
						scheme := ""
						if s, ok := httpRequest["scheme"].(string); ok {
							scheme = s
						}
						host := ""
						if h, ok := httpRequest["host"].(string); ok {
							host = h
						}
						// Prefer request_uri (actual path) over uri (which may contain /index.php)
						uri := ""
						if reqUri, ok := httpRequest["request_uri"].(string); ok && reqUri != "" {
							uri = reqUri
						} else if u, ok := httpRequest["uri"].(string); ok {
							uri = u
						}
						
						// Construct full URL
						url := ""
						if scheme != "" && host != "" {
							url = fmt.Sprintf("%s://%s%s", scheme, host, uri)
						} else if host != "" {
							url = fmt.Sprintf("%s%s", host, uri)
						} else if uri != "" {
							url = uri
						}
						
						if url != "" {
							req["url"] = url
							
							// Build URI with query string if present
							uriWithQuery := uri
							if qs, ok := httpRequest["query_string"].(string); ok && qs != "" {
								uriWithQuery = uri + "?" + qs
								req["url"] = url + "?" + qs
							}
							req["uri"] = uriWithQuery
							// Also include request_uri if available
							if reqUri, ok := httpRequest["request_uri"].(string); ok && reqUri != "" {
								req["request_uri"] = reqUri
							}
							
							// Get method
							if m, ok := httpRequest["method"].(string); ok {
								req["method"] = m
							}
							
							// Extract request_size as bytes_sent from http_request
							if reqSize, ok := httpRequest["request_size"].(float64); ok {
								req["bytes_sent"] = reqSize
								req["request_size"] = reqSize
							} else if netBytesSent > 0 {
								// Fallback to net bytes_sent if request_size not available
								req["bytes_sent"] = float64(netBytesSent)
							}
							
							// Get status code and response size from http_response
							if httpResponse, ok := tagsMap["http_response"].(map[string]interface{}); ok {
								if sc, ok := httpResponse["status_code"].(float64); ok {
									req["status_code"] = sc
								}
								// Extract response_size as bytes_received
								if respSize, ok := httpResponse["response_size"].(float64); ok {
									req["bytes_received"] = respSize
									req["response_size"] = respSize
								} else if netBytesRecv > 0 {
									// Fallback to net bytes_received if response_size not available
									req["bytes_received"] = float64(netBytesRecv)
								}
							} else if netBytesRecv > 0 {
								// Fallback to net bytes_received if http_response not available
								req["bytes_received"] = float64(netBytesRecv)
							}
							
							// Use span duration
							if durationMs > 0 {
								req["duration_ms"] = durationMs
							}
							
							// Check for errors
							if spanStatus == "error" || spanStatus == "0" {
								req["error"] = "span_error"
							}
							
							processHttpRequestFromField(req, httpCallsMap, serviceName, &totalCalls, startTs)
						}
					}
				}
			}
		}
		
		// Convert to array and calculate averages
		httpCalls := make([]map[string]interface{}, 0, len(httpCallsMap))
		for _, call := range httpCallsMap {
			avgDuration := 0.0
			errorRate := 0.0
			if call.CallCount > 0 {
				avgDuration = call.TotalDuration / float64(call.CallCount)
				errorRate = float64(call.ErrorCount) / float64(call.CallCount) * 100.0
				if call.MinDuration == 999999.0 {
					call.MinDuration = 0.0
				}
			}
			
			// Include status code in response (most common status code for this endpoint)
			statusCode := 0
			if call.MostCommonStatus > 0 {
				statusCode = call.MostCommonStatus
			}
			
			httpCalls = append(httpCalls, map[string]interface{}{
				"url":                 call.URL,
				"uri":                 call.URI,
				"request_uri":        call.URI, // Add request_uri for compatibility
				"method":             call.Method,
				"service":            call.Service,
				"call_count":         call.CallCount,
				"avg_duration":       avgDuration,
				"min_duration":       call.MinDuration,
				"max_duration":       call.MaxDuration,
				"error_count":       call.ErrorCount,
				"error_rate":        errorRate,
				"total_bytes_sent":   call.TotalBytesSent,
				"total_bytes_received": call.TotalBytesRecv,
				"status_code":       statusCode, // Include actual status code
				"last_created_at":    call.LastCreatedAt,
			})
		}
		
		// Sorting
		sort.Slice(httpCalls, func(i, j int) bool {
			if sortBy == "last_created_at" || sortBy == "created_at" {
				ti, _ := httpCalls[i]["last_created_at"].(string)
				tj, _ := httpCalls[j]["last_created_at"].(string)
				if sortOrder == "asc" {
					return ti < tj
				}
				return ti > tj
			} else if sortBy == "call_count" || sortBy == "count" {
				ci, _ := httpCalls[i]["call_count"].(int64)
				cj, _ := httpCalls[j]["call_count"].(int64)
				if sortOrder == "asc" {
					return ci < cj
				}
				return ci > cj
			} else if sortBy == "avg_duration" || sortBy == "duration" {
				di, _ := httpCalls[i]["avg_duration"].(float64)
				dj, _ := httpCalls[j]["avg_duration"].(float64)
				if sortOrder == "asc" {
					return di < dj
				}
				return di > dj
			} else if sortBy == "error_count" || sortBy == "errors" {
				ei, _ := httpCalls[i]["error_count"].(int64)
				ej, _ := httpCalls[j]["error_count"].(int64)
				if sortOrder == "asc" {
					return ei < ej
				}
				return ei > ej
			} else if sortBy == "error_rate" || sortBy == "rate" {
				ri, _ := httpCalls[i]["error_rate"].(float64)
				rj, _ := httpCalls[j]["error_rate"].(float64)
				if sortOrder == "asc" {
					return ri < rj
				}
				return ri > rj
			} else {
				// Default to last_created_at DESC
				ti, _ := httpCalls[i]["last_created_at"].(string)
				tj, _ := httpCalls[j]["last_created_at"].(string)
				return ti > tj
			}
		})
		
		// Apply pagination
		total := int64(len(httpCalls))
		startIdx := offsetInt
		endIdx := offsetInt + limitInt
		if startIdx > len(httpCalls) {
			startIdx = len(httpCalls)
		}
		if endIdx > len(httpCalls) {
			endIdx = len(httpCalls)
		}
		if startIdx < 0 {
			startIdx = 0
		}
		
		paginatedCalls := httpCalls[startIdx:endIdx]
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"http_calls":  paginatedCalls,
			"total":       total,
			"total_calls": totalCalls,
		})
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

// Filter suggestions endpoints
mux.HandleFunc("/api/filter-suggestions/keys", func(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", 405)
		return
	}
	
	prefix := r.URL.Query().Get("prefix")
	context := r.URL.Query().Get("context")
	
	// Define all available filter keys with categories
	allKeys := []map[string]interface{}{
		// Span-level fields
		{"key": "service", "category": "Span", "type": "string"},
		{"key": "name", "category": "Span", "type": "string"},
		{"key": "status", "category": "Span", "type": "string"},
		{"key": "trace_id", "category": "Span", "type": "string"},
		{"key": "span_id", "category": "Span", "type": "string"},
		{"key": "parent_id", "category": "Span", "type": "string"},
		{"key": "duration_ms", "category": "Span", "type": "number"},
		{"key": "cpu_ms", "category": "Span", "type": "number"},
		{"key": "start_ts", "category": "Span", "type": "datetime"},
		{"key": "end_ts", "category": "Span", "type": "datetime"},
		{"key": "url_scheme", "category": "Span", "type": "string"},
		{"key": "url_host", "category": "Span", "type": "string"},
		{"key": "url_path", "category": "Span", "type": "string"},
		{"key": "language", "category": "Span", "type": "string"},
		{"key": "language_version", "category": "Span", "type": "string"},
		{"key": "framework", "category": "Span", "type": "string"},
		{"key": "framework_version", "category": "Span", "type": "string"},
		
		// HTTP fields (from http array or tags.http_request)
		{"key": "http.method", "category": "HTTP", "type": "string"},
		{"key": "http.url", "category": "HTTP", "type": "string"},
		{"key": "http.uri", "category": "HTTP", "type": "string"},
		{"key": "http.request_uri", "category": "HTTP", "type": "string"},
		{"key": "http.status_code", "category": "HTTP", "type": "number"},
		{"key": "http.duration_ms", "category": "HTTP", "type": "number"},
		{"key": "http.bytes_sent", "category": "HTTP", "type": "number"},
		{"key": "http.bytes_received", "category": "HTTP", "type": "number"},
		{"key": "http.request_size", "category": "HTTP", "type": "number"},
		{"key": "http.response_size", "category": "HTTP", "type": "number"},
		{"key": "http.query_string", "category": "HTTP", "type": "string"},
		
		// Tags fields (nested JSON)
		{"key": "tags.http_request.method", "category": "Tags", "type": "string"},
		{"key": "tags.http_request.url", "category": "Tags", "type": "string"},
		{"key": "tags.http_request.uri", "category": "Tags", "type": "string"},
		{"key": "tags.http_request.request_uri", "category": "Tags", "type": "string"},
		{"key": "tags.http_request.query_string", "category": "Tags", "type": "string"},
		{"key": "tags.http_response.status_code", "category": "Tags", "type": "number"},
		{"key": "tags.http_response.response_size", "category": "Tags", "type": "number"},
		
		// SQL fields
		{"key": "sql.query", "category": "SQL", "type": "string"},
		{"key": "sql.duration_ms", "category": "SQL", "type": "number"},
		{"key": "sql.rows_affected", "category": "SQL", "type": "number"},
		{"key": "sql.error", "category": "SQL", "type": "string"},
		
		// Network fields
		{"key": "net.bytes_sent", "category": "Network", "type": "number"},
		{"key": "net.bytes_received", "category": "Network", "type": "number"},
	}
	
	// Filter by prefix if provided
	var filteredKeys []map[string]interface{}
	prefixLower := strings.ToLower(prefix)
	
	for _, key := range allKeys {
		keyStr := key["key"].(string)
		keyStrLower := strings.ToLower(keyStr)
		
		// If prefix is empty, show all keys
		if prefix == "" {
			// Filter by context if provided
			if context == "" || strings.HasPrefix(keyStr, context) {
				filteredKeys = append(filteredKeys, key)
			}
			continue
		}
		
		// Check if prefix matches from start (exact prefix match)
		if strings.HasPrefix(keyStrLower, prefixLower) {
			// Filter by context if provided
			if context == "" || strings.HasPrefix(keyStr, context) {
				filteredKeys = append(filteredKeys, key)
			}
			continue
		}
		
		// Check if prefix matches any part of the key (partial match)
		// For example, "ser" should match "service"
		// Split key by dots and check each segment
		keyParts := strings.Split(keyStrLower, ".")
		prefixParts := strings.Split(prefixLower, ".")
		
		// If prefix contains a dot, match the prefix parts
		if len(prefixParts) > 1 {
			// Prefix like "sql." - match keys starting with "sql."
			if len(keyParts) >= len(prefixParts) {
				matches := true
				for i := 0; i < len(prefixParts); i++ {
					if !strings.HasPrefix(keyParts[i], prefixParts[i]) {
						matches = false
						break
					}
				}
				if matches {
					// Filter by context if provided
					if context == "" || strings.HasPrefix(keyStr, context) {
						filteredKeys = append(filteredKeys, key)
					}
					continue
				}
			}
		} else {
			// Single word prefix like "ser" - check if it matches any part of the key
			// Check if prefix is contained in any part of the key
			for _, part := range keyParts {
				if strings.HasPrefix(part, prefixLower) {
					// Filter by context if provided
					if context == "" || strings.HasPrefix(keyStr, context) {
						filteredKeys = append(filteredKeys, key)
					}
					break
				}
			}
		}
	}
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"keys": filteredKeys,
	})
})

mux.HandleFunc("/api/filter-suggestions/values", func(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", 405)
		return
	}
	
	key := r.URL.Query().Get("key")
	prefix := r.URL.Query().Get("prefix")
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
			limit = l
		}
	}
	
	if key == "" {
		http.Error(w, "key parameter is required", 400)
		return
	}
	
	ctx, _ := ExtractTenantContext(r, queryClient)
	
	// Build tenant filter
	var tenantFilter string
	if ctx.IsAllTenants() {
		tenantFilter = ""
	} else {
		tenantFilter = fmt.Sprintf(" AND (coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
			escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
	}
	
	var query string
	var values []string
	
	// Handle different field types
	if key == "service" {
		query = fmt.Sprintf(`SELECT DISTINCT service as value FROM opa.spans_min WHERE service != ''%s ORDER BY service LIMIT %d`, tenantFilter, limit)
	} else if key == "status" {
		query = fmt.Sprintf(`SELECT DISTINCT status as value FROM opa.spans_min WHERE status != ''%s ORDER BY value LIMIT %d`, tenantFilter, limit)
	} else if key == "name" {
		query = fmt.Sprintf(`SELECT DISTINCT name as value FROM opa.spans_min WHERE name != ''%s ORDER BY value LIMIT %d`, tenantFilter, limit)
	} else if key == "language" {
		query = fmt.Sprintf(`SELECT DISTINCT language as value FROM opa.spans_min WHERE language != ''%s ORDER BY value LIMIT %d`, tenantFilter, limit)
	} else if key == "framework" {
		query = fmt.Sprintf(`SELECT DISTINCT framework as value FROM opa.spans_min WHERE framework != '' AND framework IS NOT NULL%s ORDER BY value LIMIT %d`, tenantFilter, limit)
	} else if strings.HasPrefix(key, "tags.http_request.") {
		// Extract nested field name
		fieldName := strings.TrimPrefix(key, "tags.http_request.")
		query = fmt.Sprintf(`SELECT DISTINCT JSONExtractString(tags, 'http_request', '%s') as value 
			FROM opa.spans_full 
			WHERE tags != '' AND tags != '{}' AND JSONExtractString(tags, 'http_request', '%s') != ''%s 
			ORDER BY value LIMIT %d`,
			escapeSQL(fieldName), escapeSQL(fieldName), tenantFilter, limit)
	} else if strings.HasPrefix(key, "tags.http_response.") {
		fieldName := strings.TrimPrefix(key, "tags.http_response.")
		query = fmt.Sprintf(`SELECT DISTINCT JSONExtractString(tags, 'http_response', '%s') as value 
			FROM opa.spans_full 
			WHERE tags != '' AND tags != '{}' AND JSONExtractString(tags, 'http_response', '%s') != ''%s 
			ORDER BY value LIMIT %d`,
			escapeSQL(fieldName), escapeSQL(fieldName), tenantFilter, limit)
	} else if strings.HasPrefix(key, "http.") {
		// For HTTP fields in the http array, we need to extract from JSON
		fieldName := strings.TrimPrefix(key, "http.")
		query = fmt.Sprintf(`SELECT DISTINCT JSONExtractString(http_item, '%s') as value 
			FROM opa.spans_full 
			ARRAY JOIN JSONExtractArrayRaw(http) as http_item
			WHERE http != '' AND http != '[]' AND http != 'null' AND JSONExtractString(http_item, '%s') != ''%s 
			ORDER BY value LIMIT %d`,
			escapeSQL(fieldName), escapeSQL(fieldName), tenantFilter, limit)
	} else {
		// Default: try to get from spans_min or spans_full
		query = fmt.Sprintf(`SELECT DISTINCT %s as value FROM opa.spans_min WHERE %s IS NOT NULL AND %s != ''%s ORDER BY value LIMIT %d`,
			escapeSQL(key), escapeSQL(key), escapeSQL(key), tenantFilter, limit)
	}
	
	rows, err := queryClient.Query(query)
	if err != nil {
		// If query fails, return empty array instead of error
		LogWarn("Filter value query failed", map[string]interface{}{
			"key": key,
			"error": err.Error(),
		})
		json.NewEncoder(w).Encode(map[string]interface{}{
			"values": []string{},
		})
		return
	}
	
	for _, row := range rows {
		value := getString(row, "value")
		if value != "" {
			// Filter by prefix if provided
			if prefix == "" || strings.HasPrefix(strings.ToLower(value), strings.ToLower(prefix)) {
				values = append(values, value)
			}
		}
	}
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"values": values,
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
		
	ctx, _ := ExtractTenantContext(r, queryClient)
	service := r.URL.Query().Get("service")
	timeFrom := r.URL.Query().Get("from")
	timeTo := r.URL.Query().Get("to")
	minDuration := r.URL.Query().Get("min_duration")
	filterQuery := r.URL.Query().Get("filter")
	limit := r.URL.Query().Get("limit")
	offset := r.URL.Query().Get("offset")
	sortBy := r.URL.Query().Get("sort")
	if sortBy == "" {
		sortBy = "last_created_at" // Default to last_created_at for grouped items
	}
	sortOrder := r.URL.Query().Get("order")
	if sortOrder == "" {
		sortOrder = "desc"
	}
	if limit == "" {
		limit = "100"
	}
	if offset == "" {
		offset = "0"
	}
	
	// Parse filter query if provided
	var filterAST *FilterAST
	var filterErr error
	if filterQuery != "" {
		filterAST, filterErr = ParseFilterQuery(filterQuery)
		if filterErr != nil {
			LogWarn("Failed to parse filter query in SQL queries endpoint", map[string]interface{}{
				"filter": filterQuery,
				"error":  filterErr.Error(),
			})
			http.Error(w, fmt.Sprintf("invalid filter query: %v", filterErr), 400)
			return
		}
	}
	
	// Build tenant filter - skip tenant filtering if "all" is selected
	var tenantFilter string
	if ctx.IsAllTenants() {
		// No tenant filtering - show all data
		tenantFilter = ""
	} else {
		// Handle NULL/empty tenant IDs by treating them as default tenant
		tenantFilter = fmt.Sprintf("(coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
			escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
	}
	
	// Build base WHERE clause
	baseWhere := "WHERE query_fingerprint IS NOT NULL AND query_fingerprint != ''"
	
	if tenantFilter != "" {
		baseWhere += " AND " + tenantFilter
	}
	
	// Add filter query if provided
	if filterAST != nil {
		var filterWhere string
		if ctx.IsAllTenants() {
			filterWhere, filterErr = BuildClickHouseWhere(filterAST, "")
		} else {
			filterWhere, filterErr = BuildClickHouseWhere(filterAST, tenantFilter)
		}
		if filterErr != nil {
			LogError(filterErr, "Failed to build filter WHERE clause in SQL queries", map[string]interface{}{
				"filter": filterQuery,
			})
			http.Error(w, fmt.Sprintf("failed to build filter: %v", filterErr), 500)
			return
		}
		
		if strings.HasPrefix(filterWhere, " WHERE ") {
			filterWhere = strings.TrimPrefix(filterWhere, " WHERE ")
		}
		baseWhere = baseWhere + " AND (" + filterWhere + ")"
	}
	
	if service != "" {
		baseWhere += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
	}
	if timeFrom != "" {
		baseWhere += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
	} else {
		baseWhere += " AND start_ts >= now() - INTERVAL 24 HOUR"
	}
	if timeTo != "" {
		baseWhere += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
	}
	
	query := fmt.Sprintf(`SELECT 
		query_fingerprint as fingerprint,
		count(*) as execution_count,
		avg(duration_ms) as avg_duration,
		quantile(0.95)(duration_ms) as p95_duration,
		quantile(0.99)(duration_ms) as p99_duration,
		max(duration_ms) as max_duration,
		max(start_ts) as last_created_at
		FROM opa.spans_min %s`, baseWhere)
	
	query += " GROUP BY query_fingerprint"
		
		if minDuration != "" {
			query += fmt.Sprintf(" HAVING avg(duration_ms) >= %s", minDuration)
		}
		
		// Sorting
		if sortBy == "last_created_at" || sortBy == "created_at" {
			query += fmt.Sprintf(" ORDER BY last_created_at %s", strings.ToUpper(sortOrder))
		} else if sortBy == "execution_count" || sortBy == "count" {
			query += fmt.Sprintf(" ORDER BY execution_count %s", strings.ToUpper(sortOrder))
		} else if sortBy == "avg_duration" || sortBy == "duration" {
			query += fmt.Sprintf(" ORDER BY avg_duration %s", strings.ToUpper(sortOrder))
		} else if sortBy == "max_duration" {
			query += fmt.Sprintf(" ORDER BY max_duration %s", strings.ToUpper(sortOrder))
		} else {
			// Default to last_created_at DESC
			query += " ORDER BY last_created_at DESC"
		}
		
		limitInt, _ := strconv.Atoi(limit)
		offsetInt, _ := strconv.Atoi(offset)
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", limitInt, offsetInt)
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var queries []map[string]interface{}
		for _, row := range rows {
			queries = append(queries, map[string]interface{}{
				"fingerprint":     getString(row, "fingerprint"),
				"execution_count":  getUint64(row, "execution_count"),
				"avg_duration":    getFloat64(row, "avg_duration"),
				"p95_duration":    getFloat64(row, "p95_duration"),
				"p99_duration":    getFloat64(row, "p99_duration"),
				"max_duration":    getFloat64(row, "max_duration"),
				"last_created_at": getString(row, "last_created_at"),
			})
		}
		
		// Get total count for pagination
		countQuery := fmt.Sprintf(`SELECT count(DISTINCT query_fingerprint) as total FROM opa.spans_min %s`, baseWhere)
		if minDuration != "" {
			countQuery = fmt.Sprintf(`SELECT count(*) as total FROM (
				SELECT query_fingerprint, avg(duration_ms) as avg_duration
				FROM opa.spans_min %s
				GROUP BY query_fingerprint
				HAVING avg(duration_ms) >= %s
			)`, baseWhere, minDuration)
		}
		countRows, _ := queryClient.Query(countQuery)
		total := int64(0)
		if len(countRows) > 0 {
			total = int64(getUint64(countRows[0], "total"))
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"queries": queries,
			"total":   total,
		})
	})
	
	// Redis operations endpoint
	mux.HandleFunc("/api/redis/operations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		ctx, _ := ExtractTenantContext(r, queryClient)
		service := r.URL.Query().Get("service")
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		minDuration := r.URL.Query().Get("min_duration")
		filterQuery := r.URL.Query().Get("filter")
		limit := r.URL.Query().Get("limit")
		offset := r.URL.Query().Get("offset")
		sortBy := r.URL.Query().Get("sort")
		if sortBy == "" {
			sortBy = "last_created_at"
		}
		sortOrder := r.URL.Query().Get("order")
		if sortOrder == "" {
			sortOrder = "desc"
		}
		if limit == "" {
			limit = "100"
		}
		if offset == "" {
			offset = "0"
		}
		
		// Parse filter query if provided
		var filterAST *FilterAST
		var filterErr error
		if filterQuery != "" {
			filterAST, filterErr = ParseFilterQuery(filterQuery)
			if filterErr != nil {
				LogWarn("Failed to parse filter query in Redis operations endpoint", map[string]interface{}{
					"filter": filterQuery,
					"error":  filterErr.Error(),
				})
				http.Error(w, fmt.Sprintf("invalid filter query: %v", filterErr), 400)
				return
			}
		}
		
		// Build tenant filter
		var tenantFilter string
		if ctx.IsAllTenants() {
			tenantFilter = ""
		} else {
			tenantFilter = fmt.Sprintf("(coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		
		// Build base WHERE clause - query spans_full for Redis operations
		baseWhere := "WHERE redis != '' AND redis != '[]' AND redis != 'null'"
		
		if tenantFilter != "" {
			baseWhere += " AND " + tenantFilter
		}
		
		// Add filter query if provided
		if filterAST != nil {
			var filterWhere string
			if ctx.IsAllTenants() {
				filterWhere, filterErr = BuildClickHouseWhere(filterAST, "")
			} else {
				filterWhere, filterErr = BuildClickHouseWhere(filterAST, tenantFilter)
			}
			if filterErr != nil {
				LogError(filterErr, "Failed to build filter WHERE clause in Redis operations", map[string]interface{}{
					"filter": filterQuery,
				})
				http.Error(w, fmt.Sprintf("failed to build filter: %v", filterErr), 500)
				return
			}
			
			if strings.HasPrefix(filterWhere, " WHERE ") {
				filterWhere = strings.TrimPrefix(filterWhere, " WHERE ")
			}
			baseWhere = baseWhere + " AND (" + filterWhere + ")"
		}
		
		if service != "" {
			baseWhere += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(service, "'", "''"))
		}
		if timeFrom != "" {
			baseWhere += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
		} else {
			baseWhere += " AND start_ts >= now() - INTERVAL 24 HOUR"
		}
		if timeTo != "" {
			baseWhere += fmt.Sprintf(" AND start_ts <= '%s'", timeTo)
		}
		
		// Query to extract Redis operations and aggregate by command+key
		// Redis is stored as JSON string array, need to parse it using JSONExtractArrayRaw
		query := fmt.Sprintf(`SELECT 
			JSONExtractString(redis_op, 'command') as command,
			coalesce(JSONExtractString(redis_op, 'key'), '') as key,
			coalesce(JSONExtractFloat(redis_op, 'duration_ms'), 0) as duration_ms,
			coalesce(JSONExtractBool(redis_op, 'hit'), 0) as hit,
			start_ts,
			service
			FROM opa.spans_full
			ARRAY JOIN JSONExtractArrayRaw(redis) as redis_op
			%s`, baseWhere)
		
		// Group by command and key to get aggregated stats
		groupQuery := fmt.Sprintf(`SELECT 
			command,
			key,
			count(*) as execution_count,
			avg(duration_ms) as avg_duration,
			quantile(0.95)(duration_ms) as p95_duration,
			quantile(0.99)(duration_ms) as p99_duration,
			max(duration_ms) as max_duration,
			sum(case when hit = 1 then 1 else 0 end) as hit_count,
			sum(case when hit = 0 then 1 else 0 end) as miss_count,
			max(start_ts) as last_created_at
			FROM (%s) WHERE command != '' AND command != 'null' GROUP BY command, key`, query)
		
		if minDuration != "" {
			groupQuery = fmt.Sprintf(`SELECT * FROM (%s) WHERE avg_duration >= %s`, groupQuery, minDuration)
		}
		
		// Sorting
		if sortBy == "last_created_at" || sortBy == "created_at" {
			groupQuery += fmt.Sprintf(" ORDER BY last_created_at %s", strings.ToUpper(sortOrder))
		} else if sortBy == "execution_count" || sortBy == "count" {
			groupQuery += fmt.Sprintf(" ORDER BY execution_count %s", strings.ToUpper(sortOrder))
		} else if sortBy == "avg_duration" || sortBy == "duration" {
			groupQuery += fmt.Sprintf(" ORDER BY avg_duration %s", strings.ToUpper(sortOrder))
		} else if sortBy == "max_duration" {
			groupQuery += fmt.Sprintf(" ORDER BY max_duration %s", strings.ToUpper(sortOrder))
		} else {
			groupQuery += " ORDER BY last_created_at DESC"
		}
		
		limitInt, _ := strconv.Atoi(limit)
		offsetInt, _ := strconv.Atoi(offset)
		groupQuery += fmt.Sprintf(" LIMIT %d OFFSET %d", limitInt, offsetInt)
		
		rows, err := queryClient.Query(groupQuery)
		if err != nil {
			LogError(err, "Redis operations query error", nil)
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var operations []map[string]interface{}
		for _, row := range rows {
			operations = append(operations, map[string]interface{}{
				"command":        getString(row, "command"),
				"key":            getString(row, "key"),
				"execution_count": getUint64(row, "execution_count"),
				"avg_duration":    getFloat64(row, "avg_duration"),
				"p95_duration":    getFloat64(row, "p95_duration"),
				"p99_duration":    getFloat64(row, "p99_duration"),
				"max_duration":    getFloat64(row, "max_duration"),
				"hit_count":       getUint64(row, "hit_count"),
				"miss_count":      getUint64(row, "miss_count"),
				"last_created_at": getString(row, "last_created_at"),
			})
		}
		
		// Get total count for pagination
		countQuery := fmt.Sprintf(`SELECT count(DISTINCT (command, key)) as total FROM (%s)`, query)
		if minDuration != "" {
			countQuery = fmt.Sprintf(`SELECT count(*) as total FROM (
				SELECT command, key, avg(duration_ms) as avg_duration
				FROM (%s)
				GROUP BY command, key
				HAVING avg(duration_ms) >= %s
			)`, query, minDuration)
		}
		countRows, _ := queryClient.Query(countQuery)
		total := int64(0)
		if len(countRows) > 0 {
			total = int64(getUint64(countRows[0], "total"))
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"operations": operations,
			"total":      total,
		})
	})
	
	// SQL query details
	mux.HandleFunc("/api/sql/queries/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		
		ctx, _ := ExtractTenantContext(r, queryClient)
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 5 {
			http.Error(w, "bad request", 400)
			return
		}
		fingerprint := strings.Join(parts[4:], "/")
		fingerprintEscaped := strings.ReplaceAll(fingerprint, "'", "''")
		
		// Build tenant filter - skip tenant filtering if "all" is selected
		var tenantFilter string
		if ctx.IsAllTenants() {
			// No tenant filtering - show all data
			tenantFilter = ""
		} else {
			// Handle NULL/empty tenant IDs by treating them as default tenant
			tenantFilter = fmt.Sprintf("(coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}
		
		// Get query stats
		var query string
		if tenantFilter != "" {
			query = fmt.Sprintf(`SELECT 
				count(*) as total_executions,
				avg(duration_ms) as avg_duration,
				quantile(0.95)(duration_ms) as p95_duration,
				quantile(0.99)(duration_ms) as p99_duration,
				max(duration_ms) as max_duration
				FROM opa.spans_min WHERE query_fingerprint = '%s' AND %s`, fingerprintEscaped, tenantFilter)
		} else {
			query = fmt.Sprintf(`SELECT 
				count(*) as total_executions,
				avg(duration_ms) as avg_duration,
				quantile(0.95)(duration_ms) as p95_duration,
				quantile(0.99)(duration_ms) as p99_duration,
				max(duration_ms) as max_duration
				FROM opa.spans_min WHERE query_fingerprint = '%s'`, fingerprintEscaped)
		}
		
		rows, err := queryClient.Query(query)
		if err != nil || len(rows) == 0 {
			http.Error(w, "query not found", 404)
			return
		}
		
		row := rows[0]
		
		// Get example query from spans_full by matching trace_id with spans_min
		exampleQuery := fingerprint // Use fingerprint as default
		var exampleQuerySQL string
		if tenantFilter != "" {
			exampleQuerySQL = fmt.Sprintf(`SELECT sql FROM opa.spans_full 
				WHERE trace_id IN (
					SELECT trace_id FROM opa.spans_min 
					WHERE query_fingerprint = '%s' AND %s LIMIT 1
				) AND %s AND sql != '' LIMIT 1`, fingerprintEscaped, tenantFilter, tenantFilter)
		} else {
			exampleQuerySQL = fmt.Sprintf(`SELECT sql FROM opa.spans_full 
				WHERE trace_id IN (
					SELECT trace_id FROM opa.spans_min 
					WHERE query_fingerprint = '%s' LIMIT 1
				) AND sql != '' LIMIT 1`, fingerprintEscaped)
		}
		exampleRows, _ := queryClient.Query(exampleQuerySQL)
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
		var trendsQuery string
		if tenantFilter != "" {
			trendsQuery = fmt.Sprintf(`SELECT 
				toStartOfHour(start_ts) as time,
				avg(duration_ms) as avg_duration,
				quantile(0.95)(duration_ms) as p95_duration
				FROM opa.spans_min WHERE query_fingerprint = '%s' AND %s
				AND start_ts >= now() - INTERVAL 7 DAY
				GROUP BY time ORDER BY time`, fingerprintEscaped, tenantFilter)
		} else {
			trendsQuery = fmt.Sprintf(`SELECT 
				toStartOfHour(start_ts) as time,
				avg(duration_ms) as avg_duration,
				quantile(0.95)(duration_ms) as p95_duration
				FROM opa.spans_min WHERE query_fingerprint = '%s' 
				AND start_ts >= now() - INTERVAL 7 DAY
				GROUP BY time ORDER BY time`, fingerprintEscaped)
		}
		
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
		filterQuery := r.URL.Query().Get("filter")
		timeFrom := r.URL.Query().Get("from")
		timeTo := r.URL.Query().Get("to")
		limit := r.URL.Query().Get("limit")
		offset := r.URL.Query().Get("offset")
		sortBy := r.URL.Query().Get("sort")
		if sortBy == "" {
			sortBy = "last_seen" // Default to last_seen (effectively last_created_at for groups)
		}
		sortOrder := r.URL.Query().Get("order")
		if sortOrder == "" {
			sortOrder = "desc"
		}
		if limit == "" {
			limit = "100"
		}
		if offset == "" {
			offset = "0"
		}
		
		// Query error_instances and join with spans_min to get service, then aggregate
		// This gives us service information which is not in error_groups
		baseWhere := "WHERE 1=1"
		
		// Parse and apply filter query if provided
		if filterQuery != "" {
			filterAST, err := ParseFilterQuery(filterQuery)
			if err != nil {
				LogWarn("Failed to parse filter query in errors endpoint", map[string]interface{}{
					"filter": filterQuery,
					"error":  err.Error(),
				})
				http.Error(w, fmt.Sprintf("invalid filter query: %v", err), 400)
				return
			}
			
			if filterAST != nil {
				// Build filter WHERE clause - note: filter fields need to map to ei.* or sm.* columns
				// For errors, available fields: service (from sm), error_type, error_message, count, first_seen, last_seen
				filterWhere, err := BuildClickHouseWhere(filterAST, "")
				if err != nil {
					LogError(err, "Failed to build filter WHERE clause for errors", map[string]interface{}{
						"filter": filterQuery,
					})
					http.Error(w, fmt.Sprintf("failed to build filter: %v", err), 500)
					return
				}
				
				if strings.HasPrefix(filterWhere, " WHERE ") {
					filterWhere = strings.TrimPrefix(filterWhere, " WHERE ")
				}
				// Map filter fields to actual columns: service -> sm.service, error_type -> ei.error_type, etc.
				// Replace field names in filterWhere to match actual column names
				filterWhere = strings.ReplaceAll(filterWhere, "service", "sm.service")
				filterWhere = strings.ReplaceAll(filterWhere, "error_type", "ei.error_type")
				filterWhere = strings.ReplaceAll(filterWhere, "error_message", "ei.error_message")
				baseWhere = baseWhere + " AND (" + filterWhere + ")"
			}
		}
		
		// Apply service filter for backward compatibility (only if filter query not provided)
		if filterQuery == "" && service != "" && service != "unknown" {
			baseWhere += fmt.Sprintf(" AND sm.service = '%s'", strings.ReplaceAll(service, "'", "''"))
		}
		
		query := fmt.Sprintf(`SELECT 
			ei.error_type,
			ei.error_message,
			COALESCE(sm.service, 'unknown') as service,
			count(*) as count,
			min(ei.occurred_at) as first_seen,
			max(ei.occurred_at) as last_seen
			FROM opa.error_instances ei
			LEFT JOIN opa.spans_min sm ON ei.trace_id = sm.trace_id AND sm.trace_id != ''
			%s`, baseWhere)
		
		if timeFrom != "" {
			// Convert ISO format to ClickHouse DateTime format
			timeFromFormatted := strings.ReplaceAll(timeFrom, "T", " ")
			timeFromFormatted = strings.ReplaceAll(timeFromFormatted, "Z", "")
			if len(timeFromFormatted) > 19 {
				timeFromFormatted = timeFromFormatted[:19]
			}
			query += fmt.Sprintf(" AND ei.occurred_at >= '%s'", timeFromFormatted)
		} else {
			// Default to last 7 days to catch older errors
			query += " AND ei.occurred_at >= now() - INTERVAL 7 DAY"
		}
		if timeTo != "" {
			// Convert ISO format to ClickHouse DateTime format
			timeToFormatted := strings.ReplaceAll(timeTo, "T", " ")
			timeToFormatted = strings.ReplaceAll(timeToFormatted, "Z", "")
			if len(timeToFormatted) > 19 {
				timeToFormatted = timeToFormatted[:19]
			}
			query += fmt.Sprintf(" AND ei.occurred_at <= '%s'", timeToFormatted)
		}
		
		query += " GROUP BY ei.error_type, ei.error_message, sm.service"
		
		// Sorting
		if sortBy == "last_seen" || sortBy == "created_at" || sortBy == "last_created_at" {
			query += fmt.Sprintf(" ORDER BY last_seen %s", strings.ToUpper(sortOrder))
		} else if sortBy == "first_seen" {
			query += fmt.Sprintf(" ORDER BY first_seen %s", strings.ToUpper(sortOrder))
		} else if sortBy == "count" {
			query += fmt.Sprintf(" ORDER BY count %s", strings.ToUpper(sortOrder))
		} else if sortBy == "error_message" || sortBy == "message" {
			query += fmt.Sprintf(" ORDER BY error_message %s", strings.ToUpper(sortOrder))
		} else if sortBy == "error_type" || sortBy == "type" {
			query += fmt.Sprintf(" ORDER BY error_type %s", strings.ToUpper(sortOrder))
		} else if sortBy == "service" {
			query += fmt.Sprintf(" ORDER BY service %s", strings.ToUpper(sortOrder))
		} else {
			// Default to last_seen DESC
			query += " ORDER BY last_seen DESC"
		}
		
		limitInt, _ := strconv.Atoi(limit)
		offsetInt, _ := strconv.Atoi(offset)
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", limitInt, offsetInt)
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var errors []map[string]interface{}
		for _, row := range rows {
			serviceName := getString(row, "service")
			errorType := getString(row, "error_type")
			errorMessage := getString(row, "error_message")
			
			// Create error_id from service and error message
			errorId := fmt.Sprintf("%s:%s", serviceName, errorMessage)
			if errorMessage == "" {
				errorId = fmt.Sprintf("%s:%s", serviceName, errorType)
			}
			
			errors = append(errors, map[string]interface{}{
				"error_id":      errorId,
				"error_message": errorMessage,
				"error_type":    errorType,
				"service":       serviceName,
				"count":         getUint64(row, "count"),
				"first_seen":    getString(row, "first_seen"),
				"last_seen":     getString(row, "last_seen"),
			})
		}
		
		// Get total count for pagination
		countQuery := fmt.Sprintf(`SELECT count(*) as total FROM (
			SELECT ei.error_type, ei.error_message, COALESCE(sm.service, 'unknown') as service
			FROM opa.error_instances ei
			LEFT JOIN opa.spans_min sm ON ei.trace_id = sm.trace_id AND sm.trace_id != ''
			%s
			GROUP BY ei.error_type, ei.error_message, sm.service
		)`, baseWhere)
		if timeFrom != "" {
			timeFromFormatted := strings.ReplaceAll(timeFrom, "T", " ")
			timeFromFormatted = strings.ReplaceAll(timeFromFormatted, "Z", "")
			if len(timeFromFormatted) > 19 {
				timeFromFormatted = timeFromFormatted[:19]
			}
			countQuery = strings.ReplaceAll(countQuery, baseWhere, baseWhere+fmt.Sprintf(" AND ei.occurred_at >= '%s'", timeFromFormatted))
		} else {
			countQuery = strings.ReplaceAll(countQuery, baseWhere, baseWhere+" AND ei.occurred_at >= now() - INTERVAL 7 DAY")
		}
		if timeTo != "" {
			timeToFormatted := strings.ReplaceAll(timeTo, "T", " ")
			timeToFormatted = strings.ReplaceAll(timeToFormatted, "Z", "")
			if len(timeToFormatted) > 19 {
				timeToFormatted = timeToFormatted[:19]
			}
			countQuery += fmt.Sprintf(" AND ei.occurred_at <= '%s'", timeToFormatted)
		}
		countRows, _ := queryClient.Query(countQuery)
		total := int64(0)
		if len(countRows) > 0 {
			total = int64(getUint64(countRows[0], "total"))
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"errors": errors,
			"total":   total,
		})
	})
	
	// GET /api/dumps - Get recent dumps from all traces
	mux.HandleFunc("/api/dumps", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}

		ctx, _ := ExtractTenantContext(r, queryClient)
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

		// Build tenant filter - skip tenant filtering if "all" is selected
		var tenantFilter string
		if ctx.IsAllTenants() {
			// No tenant filtering - show all data
			tenantFilter = ""
		} else {
			// Handle NULL/empty tenant IDs by treating them as default tenant
			tenantFilter = fmt.Sprintf("(coalesce(nullif(organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		}

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
		
		if tenantFilter != "" {
			query += " AND " + tenantFilter
		}
		
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
	
	// GET /api/logs - Get recent logs from all traces
	mux.HandleFunc("/api/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}

		ctx, _ := ExtractTenantContext(r, queryClient)
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
		level := r.URL.Query().Get("level")
		filterQuery := r.URL.Query().Get("filter")
		all := r.URL.Query().Get("all") // If "all" is set, fetch all historical logs
		cursor := r.URL.Query().Get("cursor") // Timestamp cursor for pagination

		// Build tenant filter - skip tenant filtering if "all" is selected
		// Logs table doesn't have organization_id/project_id, so we join with spans_min
		var tenantFilter string
		var useJoin bool
		if ctx.IsAllTenants() {
			// No tenant filtering - show all data
			tenantFilter = ""
			useJoin = false
		} else {
			// Handle NULL/empty tenant IDs by treating them as default tenant
			tenantFilter = fmt.Sprintf("(coalesce(nullif(spans_min.organization_id, ''), 'default-org') = '%s' AND coalesce(nullif(spans_min.project_id, ''), 'default-project') = '%s')",
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
			useJoin = true
		}

		// Parse and apply filter query if provided
		var filterWhere string
		if filterQuery != "" {
			filterAST, err := ParseFilterQuery(filterQuery)
			if err != nil {
				LogWarn("Failed to parse filter query in logs endpoint", map[string]interface{}{
					"filter": filterQuery,
					"error":  err.Error(),
				})
				http.Error(w, fmt.Sprintf("invalid filter query: %v", err), 400)
				return
			}
			
			if filterAST != nil {
				// Build tenant filter for filter query builder
				var tenantFilterForFilter string
				if !ctx.IsAllTenants() {
					tenantFilterForFilter = tenantFilter
				}
				
				builtFilterWhere, err := BuildClickHouseWhere(filterAST, tenantFilterForFilter)
				if err != nil {
					LogError(err, "Failed to build filter WHERE clause for logs", map[string]interface{}{
						"filter": filterQuery,
					})
					http.Error(w, fmt.Sprintf("failed to build filter: %v", err), 500)
					return
				}
				
				if strings.HasPrefix(builtFilterWhere, " WHERE ") {
					builtFilterWhere = strings.TrimPrefix(builtFilterWhere, " WHERE ")
				}
				// Map filter fields to actual columns: service -> logs.service (or service), level -> logs.level (or level), etc.
				if useJoin {
					builtFilterWhere = strings.ReplaceAll(builtFilterWhere, "service", "logs.service")
					builtFilterWhere = strings.ReplaceAll(builtFilterWhere, "level", "logs.level")
					builtFilterWhere = strings.ReplaceAll(builtFilterWhere, "message", "logs.message")
					builtFilterWhere = strings.ReplaceAll(builtFilterWhere, "trace_id", "logs.trace_id")
					builtFilterWhere = strings.ReplaceAll(builtFilterWhere, "span_id", "logs.span_id")
					builtFilterWhere = strings.ReplaceAll(builtFilterWhere, "timestamp", "logs.timestamp")
				}
				filterWhere = builtFilterWhere
			}
		}

		// Query opa.logs table
		var query string
		baseWhere := "WHERE 1=1"
		if useJoin {
			if tenantFilter != "" {
				baseWhere += " AND " + tenantFilter
			}
			if filterWhere != "" {
				baseWhere += " AND (" + filterWhere + ")"
			}
			query = fmt.Sprintf(`SELECT 
				logs.id,
				logs.trace_id,
				logs.span_id,
				logs.service,
				logs.level,
				logs.message,
				toUnixTimestamp64Milli(logs.timestamp) as timestamp_ms,
				logs.timestamp,
				logs.fields
			FROM opa.logs as logs
			INNER JOIN opa.spans_min as spans_min ON logs.trace_id = spans_min.trace_id
			%s`, baseWhere)
		} else {
			if filterWhere != "" {
				baseWhere += " AND (" + filterWhere + ")"
			}
			query = fmt.Sprintf(`SELECT 
				id,
				trace_id,
				span_id,
				service,
				level,
				message,
				toUnixTimestamp64Milli(timestamp) as timestamp_ms,
				timestamp,
				fields
			FROM opa.logs 
			%s`, baseWhere)
		}
		
		// Use table prefix when joining
		serviceColumn := "service"
		levelColumn := "level"
		if useJoin {
			serviceColumn = "logs.service"
			levelColumn = "logs.level"
		}
		
		// Apply service/level filters for backward compatibility (only if filter query not provided)
		if filterQuery == "" {
			if service != "" {
				query += fmt.Sprintf(" AND %s = '%s'", serviceColumn, strings.ReplaceAll(service, "'", "''"))
			}
			
			if level != "" {
				query += fmt.Sprintf(" AND %s = '%s'", levelColumn, strings.ReplaceAll(level, "'", "''"))
			}
		}
		
		// Cursor-based pagination: if cursor is provided, fetch logs older than cursor
		timestampColumn := "timestamp"
		if useJoin {
			timestampColumn = "logs.timestamp"
		}
		if cursor != "" {
			// Parse cursor as timestamp (milliseconds since epoch)
			if cursorInt, err := strconv.ParseInt(cursor, 10, 64); err == nil {
				// Convert to ClickHouse datetime format
				cursorTime := time.UnixMilli(cursorInt).Format("2006-01-02 15:04:05.000")
				query += fmt.Sprintf(" AND %s < '%s'", timestampColumn, cursorTime)
			}
		} else {
			// Only apply time restriction if "all" is not set and "since" is not provided
			if all == "" {
				if since != "" {
					query += fmt.Sprintf(" AND %s >= '%s'", timestampColumn, since)
				} else {
					// Default to last 7 days for better coverage while still being reasonable
					query += fmt.Sprintf(" AND %s >= now() - INTERVAL 7 DAY", timestampColumn)
				}
			} else if since != "" {
				// If "all" is set but "since" is also provided, use "since" as minimum
				query += fmt.Sprintf(" AND %s >= '%s'", timestampColumn, since)
			}
			// If "all" is set and no "since", no time restriction (fetch all historical)
		}
		
		query += fmt.Sprintf(" ORDER BY %s DESC LIMIT %s", timestampColumn, strconv.Itoa(limitInt))
		
		rows, err := queryClient.Query(query)
		if err != nil {
			http.Error(w, fmt.Sprintf("query error: %v", err), 500)
			return
		}
		
		var allLogs []map[string]interface{}
		
		for _, row := range rows {
			logID := getString(row, "id")
			traceID := getString(row, "trace_id")
			spanID := getString(row, "span_id")
			serviceName := getString(row, "service")
			levelName := getString(row, "level")
			message := getString(row, "message")
			fieldsStr := getString(row, "fields")
			
			// Get timestamp in milliseconds (preferred method)
			var timestampMs int64
			if tsMs, ok := row["timestamp_ms"].(float64); ok {
				timestampMs = int64(tsMs)
			} else if tsMs, ok := row["timestamp_ms"].(int64); ok {
				timestampMs = tsMs
			} else {
				// Fallback: parse timestamp string
				timestampStr := getString(row, "timestamp")
				if timestampStr != "" {
					timestamp, err := time.Parse("2006-01-02 15:04:05.000", timestampStr)
					if err != nil {
						// Try alternative formats
						timestamp, err = time.Parse("2006-01-02 15:04:05", timestampStr)
						if err != nil {
							timestamp, _ = time.Parse(time.RFC3339, timestampStr)
						}
					}
					timestampMs = timestamp.UnixMilli()
				}
			}
			
			// Parse fields JSON
			var fields map[string]interface{}
			if fieldsStr != "" && fieldsStr != "null" {
				json.Unmarshal([]byte(fieldsStr), &fields)
			}
			
			allLogs = append(allLogs, map[string]interface{}{
				"id":        logID,
				"trace_id":  traceID,
				"span_id":   spanID,
				"service":   serviceName,
				"level":     levelName,
				"message":   message,
				"timestamp": timestampMs,
				"fields":    fields,
			})
		}
		
		// Calculate next cursor (timestamp of the last log, or 0 if no more)
		nextCursor := int64(0)
		hasMore := false
		if len(allLogs) > 0 {
			lastLog := allLogs[len(allLogs)-1]
			if ts, ok := lastLog["timestamp"].(int64); ok && ts > 0 {
				nextCursor = ts
				hasMore = len(allLogs) == limitInt // If we got full batch, there might be more
			}
		}
		
		json.NewEncoder(w).Encode(map[string]interface{}{
			"logs":       allLogs,
			"total":      len(allLogs),
			"has_more":   hasMore,
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
		traceQuery := fmt.Sprintf(`SELECT trace_id, start_ts, 
			toUnixTimestamp64Milli(end_ts) - toUnixTimestamp64Milli(start_ts) as duration_ms
			FROM (
				SELECT DISTINCT trace_id, min(start_ts) as start_ts, max(end_ts) as end_ts
				FROM opa.spans_min WHERE (status = 'error' OR status = '0')
				AND service = '%s' AND name = '%s'
				GROUP BY trace_id
			) ORDER BY start_ts DESC LIMIT 10`, serviceName, errorName)
		
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
		
		// List of all data tables to purge (excluding configuration tables)
		dataTables := []string{
			"spans_min",
			"spans_full",
			"traces_full",
			"network_metrics",
			"logs",
			"slow_queries",
			"query_performance",
			"rum_events",
			"response_breakdown",
			"throughput_metrics",
			"apdex_scores",
			"key_transaction_metrics",
			"external_service_metrics",
			"custom_metrics",
			"custom_events",
			"error_groups",
			"error_instances",
			"user_sessions",
			"user_journeys",
			"browser_metrics",
			"infrastructure_metrics",
			"deployment_impact",
			"service_dependencies",
			"service_map_metadata",
			"service_map_thresholds",
			"slo_metrics",
			"anomalies",
			"alert_history",
		}
		
		// Delete all data from ClickHouse tables
		errors := make([]string, 0)
		
		for _, table := range dataTables {
			query := fmt.Sprintf("ALTER TABLE opa.%s DELETE WHERE 1=1", table)
			if err := queryClient.Execute(query); err != nil {
				LogError(err, fmt.Sprintf("Error purging %s", table), nil)
				errors = append(errors, fmt.Sprintf("%s: %v", table, err))
			} else {
				LogInfo(fmt.Sprintf("Successfully purged %s", table), nil)
			}
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
		query += "dateDiff('millisecond', min(start_ts), max(end_ts)) as duration_ms, count(*) as span_count, "
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
	serviceMapProcessors = make(map[string]*ServiceMapProcessor)
	
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
		filterQuery := r.URL.Query().Get("filter")
		timeFrom := r.URL.Query().Get("from")
		
		baseWhere := "WHERE 1=1"
		
		// Parse and apply filter query if provided
		if filterQuery != "" {
			filterAST, err := ParseFilterQuery(filterQuery)
			if err != nil {
				LogWarn("Failed to parse filter query in anomalies endpoint", map[string]interface{}{
					"filter": filterQuery,
					"error":  err.Error(),
				})
				http.Error(w, fmt.Sprintf("invalid filter query: %v", err), 400)
				return
			}
			
			if filterAST != nil {
				// Build filter WHERE clause
				filterWhere, err := BuildClickHouseWhere(filterAST, "")
				if err != nil {
					LogError(err, "Failed to build filter WHERE clause for anomalies", map[string]interface{}{
						"filter": filterQuery,
					})
					http.Error(w, fmt.Sprintf("failed to build filter: %v", err), 500)
					return
				}
				
				if strings.HasPrefix(filterWhere, " WHERE ") {
					filterWhere = strings.TrimPrefix(filterWhere, " WHERE ")
				}
				// Map filter fields to actual columns: service, severity, type, metric, value, detected_at
				// Fields already match column names, so no mapping needed
				baseWhere = baseWhere + " AND (" + filterWhere + ")"
			}
		}
		
		query := fmt.Sprintf("SELECT id, type, service, metric, value, expected, score, severity, detected_at, metadata FROM opa.anomalies %s", baseWhere)
		
		// Apply service/severity filters for backward compatibility (only if filter query not provided)
		if filterQuery == "" {
			if service != "" {
				query += fmt.Sprintf(" AND service = '%s'", escapeSQL(service))
			}
			if severity != "" {
				query += fmt.Sprintf(" AND severity = '%s'", escapeSQL(severity))
			}
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
	
	// Periodic flush and metadata update for ServiceMapProcessor
	go func() {
		t := time.NewTicker(30 * time.Second) // Flush every 30 seconds
		for range t.C {
			serviceMapMu.RLock()
			processors := make([]*ServiceMapProcessor, 0, len(serviceMapProcessors))
			for _, processor := range serviceMapProcessors {
				processors = append(processors, processor)
			}
			serviceMapMu.RUnlock()
			
			for _, processor := range processors {
				// Update metadata BEFORE flushing (since Flush clears dependencies)
				processor.UpdateMetadata()
				processor.Flush()
			}
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
				log.Printf("[DEBUG] TCP connection accepted from: %s", conn.RemoteAddr())
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
	serviceMapProcessors map[string]*ServiceMapProcessor // key: "orgID:projectID"
	serviceMapMu         sync.RWMutex
)

func handleConn(conn net.Conn, inCh chan<- json.RawMessage) {
	defer func() {
		log.Printf("[DEBUG] Connection closed: %s", conn.RemoteAddr())
		conn.Close()
	}()
	log.Printf("[DEBUG] Handling connection from: %s", conn.RemoteAddr())
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)
	
	log.Printf("[DEBUG] Starting to scan for data from: %s", conn.RemoteAddr())
	for scanner.Scan() {
		line := scanner.Bytes()
		log.Printf("[DEBUG] Received line from %s: %d bytes", conn.RemoteAddr(), len(line))
		incomingCounter.Inc()
		
		// Decompress if needed
		var raw []byte
		decompressed, err := decompressLZ4(line)
		if err != nil {
			log.Printf("[DEBUG] Decompression check failed (may not be compressed): %v, trying as raw JSON", err)
			// Try as raw JSON if decompression fails (data might not be compressed)
			raw = make([]byte, len(line))
			copy(raw, line)
		} else {
			log.Printf("[DEBUG] Successfully decompressed: %d bytes -> %d bytes", len(line), len(decompressed))
			raw = make([]byte, len(decompressed))
			copy(raw, decompressed)
		}
		
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
		log.Printf("[DEBUG] Scanner error from %s: %v", conn.RemoteAddr(), err)
	} else {
		log.Printf("[DEBUG] Scanner finished normally from %s (no more data)", conn.RemoteAddr())
	}
}

// Error message structure
type ErrorMessage struct {
	Type            string          `json:"type"`
	TraceID         string          `json:"trace_id"`
	SpanID          string          `json:"span_id"`
	InstanceID      string          `json:"instance_id"`
	GroupID         string          `json:"group_id"`
	Fingerprint     string          `json:"fingerprint"`
	ErrorType       string          `json:"error_type"`
	ErrorMessage    string          `json:"error_message"`
	File            string          `json:"file"`
	Line            int             `json:"line"`
	StackTrace      json.RawMessage `json:"stack_trace,omitempty"` // Can be array or string
	HttpRequest     json.RawMessage `json:"http_request,omitempty"`
	Tags            json.RawMessage `json:"tags,omitempty"`
	SqlQueries      json.RawMessage `json:"sql_queries,omitempty"`
	HttpRequests    json.RawMessage `json:"http_requests,omitempty"`
	ExceptionCode   *int            `json:"exception_code,omitempty"`
	Environment     string          `json:"environment,omitempty"`
	Release         string          `json:"release,omitempty"`
	UserContext     json.RawMessage `json:"user_context,omitempty"`
	OrganizationID  string          `json:"organization_id"`
	ProjectID       string          `json:"project_id"`
	Service         string          `json:"service"`
	OccurredAtMs    int64           `json:"occurred_at_ms"`
}

// Log message structure
type LogMessage struct {
	Type        string                 `json:"type"`
	ID          string                 `json:"id"`
	TraceID     string                 `json:"trace_id"`
	SpanID      *string                `json:"span_id"`
	Level       string                 `json:"level"`
	Message     string                 `json:"message"`
	Service     string                 `json:"service"`
	TimestampMs int64                  `json:"timestamp_ms"`
	Fields      map[string]interface{} `json:"fields"`
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
	
	// Convert stack_trace to string if it's JSON
	stackTraceStr := ""
	if len(errMsg.StackTrace) > 0 {
		// If it's already a string (wrapped in quotes), use it directly
		// Otherwise, it's a JSON array/object, marshal it to string
		if errMsg.StackTrace[0] == '"' {
			// It's a JSON string, unmarshal it
			json.Unmarshal(errMsg.StackTrace, &stackTraceStr)
		} else {
			// It's a JSON array/object, convert to string
			stackTraceStr = string(errMsg.StackTrace)
		}
	}
	
	// Convert context fields to strings
	userContextStr := "{}"
	if len(errMsg.UserContext) > 0 {
		if errMsg.UserContext[0] == '"' {
			// It's a JSON string, unmarshal it
			json.Unmarshal(errMsg.UserContext, &userContextStr)
		} else {
			// It's a JSON object, convert to string
			userContextStr = string(errMsg.UserContext)
		}
	}
	
	tagsStr := "{}"
	if len(errMsg.Tags) > 0 {
		if errMsg.Tags[0] == '"' {
			// It's a JSON string, unmarshal it
			json.Unmarshal(errMsg.Tags, &tagsStr)
		} else {
			// It's a JSON object, convert to string
			tagsStr = string(errMsg.Tags)
		}
	}
	
	// Set environment and release with defaults
	environment := errMsg.Environment
	if environment == "" {
		environment = "production"
	}
	release := errMsg.Release
	
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
		"stack_trace":     stackTraceStr,
		"occurred_at":     occurredAt.Format("2006-01-02 15:04:05"),
		"environment":     environment,
		"release":         release,
		"user_context":   userContextStr,
		"tags":            tagsStr,
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

// Handle log messages from PHP extension
func handleLogMessage(raw json.RawMessage, writer *ClickHouseWriter, wsHub *WebSocketHub) {
	var logMsg LogMessage
	if err := json.Unmarshal(raw, &logMsg); err != nil {
		log.Printf("Failed to unmarshal log message: %v", err)
		return
	}
	
	// Set defaults
	if logMsg.Service == "" {
		logMsg.Service = "php-fpm"
	}
	if logMsg.ID == "" {
		logMsg.ID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	
	// Convert timestamp from milliseconds
	var timestamp time.Time
	if logMsg.TimestampMs > 0 {
		timestamp = time.UnixMilli(logMsg.TimestampMs)
	} else {
		timestamp = time.Now()
	}
	
	// Serialize fields to JSON string
	fieldsJSON := "{}"
	var fieldsMap map[string]interface{}
	if logMsg.Fields != nil && len(logMsg.Fields) > 0 {
		if fieldsBytes, err := json.Marshal(logMsg.Fields); err == nil {
			fieldsJSON = string(fieldsBytes)
			json.Unmarshal(fieldsBytes, &fieldsMap)
		}
	}
	
	// Prepare span_id (nullable - use nil for empty string)
	var spanID interface{} = nil
	spanIDStr := ""
	if logMsg.SpanID != nil && *logMsg.SpanID != "" {
		spanID = *logMsg.SpanID
		spanIDStr = *logMsg.SpanID
	}
	
	// Write log to ClickHouse
	logEntry := map[string]interface{}{
		"id":        logMsg.ID,
		"trace_id":  logMsg.TraceID,
		"span_id":   spanID,
		"service":   logMsg.Service,
		"level":     logMsg.Level,
		"message":   logMsg.Message,
		"timestamp": timestamp.Format("2006-01-02 15:04:05.000"),
		"fields":    fieldsJSON,
	}
	
	writer.AddLog(logEntry)
	
	// Broadcast log via WebSocket
	if wsHub != nil {
		broadcastData := map[string]interface{}{
			"trace_id":  logMsg.TraceID,
			"span_id":   spanIDStr,
			"service":   logMsg.Service,
			"level":     logMsg.Level,
			"message":   logMsg.Message,
			"timestamp": timestamp.Unix(),
			"fields":    fieldsMap,
		}
		wsHub.Broadcast("logs", broadcastData)
	}
	
	log.Printf("Log tracked: level=%s, message=%.100s, trace_id=%s", 
		logMsg.Level, logMsg.Message, logMsg.TraceID)
}

// calculateSpanStatus determines the span status based on HTTP response codes and error indicators
// Extension provides data, agent calculates status
// Returns "error" for errors, "ok" for success, or empty string to keep original status
func calculateSpanStatus(inc *Incoming) string {
	// Check HTTP response status code from tags (highest priority - authoritative source)
	if len(inc.Tags) > 0 {
		var tagsMap map[string]interface{}
		if err := json.Unmarshal(inc.Tags, &tagsMap); err == nil {
			// Check for http_response.status_code
			if httpResponse, ok := tagsMap["http_response"].(map[string]interface{}); ok {
				if statusCodeVal, exists := httpResponse["status_code"]; exists {
					var statusCode int
					switch v := statusCodeVal.(type) {
					case float64:
						statusCode = int(v)
					case int:
						statusCode = v
					case int64:
						statusCode = int(v)
					case string:
						if parsed, err := strconv.Atoi(v); err == nil {
							statusCode = parsed
						}
					}
					
					// HTTP status codes >= 400 indicate errors (client or server errors)
					if statusCode >= 400 {
						log.Printf("[DEBUG] Status set to error due to HTTP status code %d", statusCode)
						return "error"
					}
					// HTTP status codes 200-399 are success
					if statusCode >= 200 && statusCode < 400 {
						return "ok"
					}
				}
			}
		}
	}
	
	// Check for exceptions/errors in dumps (second priority)
	if len(inc.Dumps) > 0 {
		var dumpsArray []interface{}
		if err := json.Unmarshal(inc.Dumps, &dumpsArray); err == nil {
			for _, dump := range dumpsArray {
				if dumpMap, ok := dump.(map[string]interface{}); ok {
					// Check for error indicators in dump
					if errorType, hasError := dumpMap["type"].(string); hasError {
						errorTypeLower := strings.ToLower(errorType)
						if strings.Contains(errorTypeLower, "error") || 
						   strings.Contains(errorTypeLower, "exception") ||
						   strings.Contains(errorTypeLower, "fatal") {
							log.Printf("[DEBUG] Status set to error due to error type in dumps: %s", errorType)
							return "error"
						}
					}
				}
			}
		}
	}
	
	// If status is already explicitly set to error, keep it
	if inc.Status == "error" || inc.Status == "0" {
		return "" // Keep existing error status
	}
	
	// If no errors detected and status is not explicitly set, default to "ok"
	// This handles the case where extension sends status=-1 (not set)
	if inc.Status == "" || inc.Status == "-1" {
		return "ok"
	}
	
	// Keep existing status if it's already set to "ok" or "1"
	return ""
}

// getOrCreateServiceMapProcessor gets or creates a ServiceMapProcessor for the given org/project
func getOrCreateServiceMapProcessor(orgID, projectID string) *ServiceMapProcessor {
	key := fmt.Sprintf("%s:%s", orgID, projectID)
	
	serviceMapMu.RLock()
	processor, exists := serviceMapProcessors[key]
	serviceMapMu.RUnlock()
	
	if exists {
		return processor
	}
	
	// Create new processor
	serviceMapMu.Lock()
	defer serviceMapMu.Unlock()
	
	// Double-check after acquiring write lock
	if processor, exists := serviceMapProcessors[key]; exists {
		return processor
	}
	
	processor = NewServiceMapProcessor(queryClient, writer, orgID, projectID)
	processor.SetURL(*clickhouseURL)
	serviceMapProcessors[key] = processor
	return processor
}

func worker(inCh <-chan json.RawMessage, tb *TailBuffer, writer *ClickHouseWriter, wsHub *WebSocketHub) {
	for raw := range inCh {
		start := time.Now()
		atomic.AddInt64(&currentQueueSize, -1)
		log.Printf("[DEBUG] Worker received message: %d bytes", len(raw))
		
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
			previewLen := 200
			if len(raw) < previewLen {
				previewLen = len(raw)
			}
			log.Printf("bad json: %v, first %d chars: %s", err, previewLen, string(raw[:previewLen]))
			breaker.RecordFailure()
			continue
		}
		
		// Handle error messages separately
		if msgType.Type == "error" {
			handleErrorMessage(raw, writer)
			processingDuration.Observe(time.Since(start).Seconds())
			continue
		}
		
		// Handle log messages separately
		if msgType.Type == "log" {
			handleLogMessage(raw, writer, wsHub)
			processingDuration.Observe(time.Since(start).Seconds())
			continue
		}
		
		var inc Incoming
		if err := json.Unmarshal(raw, &inc); err != nil {
			log.Printf("bad json: %v", err)
			breaker.RecordFailure()
			continue
		}
		log.Printf("[DEBUG] Parsed incoming span: trace_id=%s, span_id=%s, service=%s, stack_len=%d, sql_len=%d", 
			inc.TraceID, inc.SpanID, inc.Service, len(inc.Stack), len(inc.Sql))
		
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
		
		// Calculate status based on HTTP response code and error indicators
		// Extension provides data, agent calculates status for better performance
		calculatedStatus := calculateSpanStatus(&inc)
		if calculatedStatus != "" {
			originalStatus := inc.Status
			inc.Status = calculatedStatus
			log.Printf("[DEBUG] Calculated status for span %s: %s (was: %s)", inc.SpanID, calculatedStatus, originalStatus)
		}
		
		// Apply sampling (using proper random sampling, not time-based)
		rate := float64(atomic.LoadUint64(&currentSamplingRate)) / 1000.0
		if rate < 1.0 {
			// Use proper random sampling instead of time-based
			// This ensures uniform distribution across requests
			if rand.Float64() > rate {
				log.Printf("[DEBUG] Message dropped due to sampling: rate=%.3f", rate)
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
		// Set creation timestamp once per span (used for both min and full)
		createdAt := time.Now()
		
		// Normalize timestamps - convert from seconds to milliseconds if needed
		normalizedStartTS := normalizeTimestamp(inc.StartTS)
		normalizedEndTS := normalizeTimestamp(inc.EndTS)
		
		min := map[string]interface{}{
			"organization_id": orgID,
			"project_id":      projectID,
			"trace_id":        inc.TraceID,
			"span_id":         inc.SpanID,
			"parent_id":       inc.ParentID, // Use actual parent_id, not nil
			"service":         inc.Service,
			"name":            inc.Name,
			"url_scheme":      inc.URLScheme,
			"url_host":        inc.URLHost,
			"url_path":        inc.URLPath,
			"start_ts":        time.UnixMilli(normalizedStartTS).Format("2006-01-02 15:04:05.000"),
			"end_ts":          time.UnixMilli(normalizedEndTS).Format("2006-01-02 15:04:05.000"),
			"duration_ms":     inc.Duration,
			"cpu_ms":          inc.CPUms,
			"status":          inc.Status,
			"language":        language,
			"bytes_sent":      bytesSent,
			"bytes_received":  bytesReceived,
			"created_at":      createdAt.Format("2006-01-02 15:04:05.000"),
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
		// Collect all SQL queries and use the first valid one for fingerprinting
		// This ensures we don't miss queries in later call nodes
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			for _, callNode := range callStack {
				if len(callNode.SQLQueries) > 0 {
					for _, sqlQuery := range callNode.SQLQueries {
						if sqlMap, ok := sqlQuery.(map[string]interface{}); ok {
							// Set db_system if not already set
							if _, exists := min["db_system"]; !exists {
								if v, ok := sqlMap["db_system"].(string); ok && v != "" {
									min["db_system"] = v
								}
							}
							// Use first valid query for fingerprint (only if not already set)
							if _, exists := min["query_fingerprint"]; !exists {
								if query, ok := sqlMap["query"].(string); ok && query != "" {
									fingerprint := normalizeSQLQuery(query)
									if fingerprint != "" {
										min["query_fingerprint"] = fingerprint
										// Found a valid fingerprint, but continue to check for db_system
									}
								}
							}
							// If we have both db_system and fingerprint, we can break
							if _, hasDB := min["db_system"]; hasDB {
								if _, hasFP := min["query_fingerprint"]; hasFP {
									break
								}
							}
						}
					}
					// If we found a fingerprint, we can stop processing call nodes
					if _, exists := min["query_fingerprint"]; exists {
						if _, hasDB := min["db_system"]; hasDB {
							break
						}
					}
				}
			}
		}
		
		// Also check direct SQL field (backward compatibility)
		// Only use if we didn't find a fingerprint from call stack
		if _, exists := min["query_fingerprint"]; !exists && inc.Sql != nil {
			var sqlArray []interface{}
			if err := json.Unmarshal(inc.Sql, &sqlArray); err == nil {
				for _, sqlItem := range sqlArray {
					if sqlMap, ok := sqlItem.(map[string]interface{}); ok {
						// Set db_system if not already set
						if _, exists := min["db_system"]; !exists {
							if v, ok := sqlMap["db_system"].(string); ok && v != "" {
								min["db_system"] = v
							}
						}
						// Use first valid query for fingerprint
						if query, ok := sqlMap["query"].(string); ok && query != "" {
							fingerprint := normalizeSQLQuery(query)
							if fingerprint != "" {
								min["query_fingerprint"] = fingerprint
								break // Found valid fingerprint
							}
						}
					}
				}
			}
		}
		
		writer.Add(min)
		
		// Store full span if trace should be kept OR if call stack is present OR if there's any profiling data
		// This ensures call stack is always stored when available, and also stores spans with SQL/HTTP/cache/Redis data even without call stack
		// IMPORTANT: Always store full spans to ensure profiling data is captured even when call stack is empty
		hasCallStack := len(inc.Stack) > 0
		// Also write full data if dumps are present (dumps are important and should always be stored)
		hasDumps := len(inc.Dumps) > 0 && string(inc.Dumps) != "[]" && string(inc.Dumps) != "null"
		// Check for profiling data in direct fields (backward compatibility - SQL/HTTP/cache/Redis can be in direct fields even without call stack)
		hasSqlData := len(inc.Sql) > 0 && string(inc.Sql) != "[]" && string(inc.Sql) != "null"
		hasHttpData := len(inc.Http) > 0 && string(inc.Http) != "[]" && string(inc.Http) != "null"
		hasCacheData := len(inc.Cache) > 0 && string(inc.Cache) != "[]" && string(inc.Cache) != "null"
		hasRedisData := len(inc.Redis) > 0 && string(inc.Redis) != "[]" && string(inc.Redis) != "null"
		
		// Also check if call stack contains profiling data (SQL/HTTP/cache/Redis in call nodes)
		hasProfilingDataInStack := false
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			// Check if any call node has SQL queries, HTTP requests, cache operations, or Redis operations
			for _, node := range callStack {
				if len(node.SQLQueries) > 0 || len(node.HttpRequests) > 0 || len(node.CacheOperations) > 0 || len(node.RedisOperations) > 0 {
					hasProfilingDataInStack = true
					break
				}
			}
		}
		
		hasProfilingData := hasSqlData || hasHttpData || hasCacheData || hasRedisData || hasProfilingDataInStack
		
		// Aggregate all SQL queries, HTTP requests, Redis operations, and cache operations
		// Each span comes separately with its own data, no need to aggregate from call stack
		// However, for backward compatibility, we still check call stack if present (old format)
		var allSQLQueries []interface{}
		var allHttpRequests []interface{}
		var allCacheOps []interface{}
		var allRedisOps []interface{}
			
		// Aggregate SQL queries: from inc.Sql (direct field) - each span has its own SQL
		if len(inc.Sql) > 0 {
			var sqlArray []interface{}
			if err := json.Unmarshal(inc.Sql, &sqlArray); err == nil {
				allSQLQueries = append(allSQLQueries, sqlArray...)
			}
		}
		// Backward compatibility: also check call stack if present (old format with embedded stack)
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			stackQueries := collectSQLQueriesFromCallStack(callStack)
			if len(stackQueries) > 0 {
				allSQLQueries = append(allSQLQueries, stackQueries...)
			}
		}
			
		// Aggregate HTTP requests: from direct field - each span has its own HTTP
		if len(inc.Http) > 0 {
			var httpArray []interface{}
			if err := json.Unmarshal(inc.Http, &httpArray); err == nil {
				allHttpRequests = append(allHttpRequests, httpArray...)
				log.Printf("[DEBUG] Service map: found %d HTTP requests in inc.Http for service %s", len(httpArray), inc.Service)
			}
		}
		// Backward compatibility: also check call stack if present
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			stackRequests := collectHttpRequestsFromCallStack(callStack)
			if len(stackRequests) > 0 {
				allHttpRequests = append(allHttpRequests, stackRequests...)
				log.Printf("[DEBUG] Service map: found %d HTTP requests in call stack for service %s", len(stackRequests), inc.Service)
			}
		}
			
		// Aggregate cache operations: from direct field - each span has its own cache
		if len(inc.Cache) > 0 {
			var cacheArray []interface{}
			if err := json.Unmarshal(inc.Cache, &cacheArray); err == nil {
				allCacheOps = append(allCacheOps, cacheArray...)
			}
		}
		// Backward compatibility: also check call stack if present
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			stackCacheOps := collectCacheOperationsFromCallStack(callStack)
			if len(stackCacheOps) > 0 {
				allCacheOps = append(allCacheOps, stackCacheOps...)
			}
		}
			
		// Aggregate Redis operations: from direct field - each span has its own Redis
		if len(inc.Redis) > 0 {
			var redisArray []interface{}
			if err := json.Unmarshal(inc.Redis, &redisArray); err == nil {
				allRedisOps = append(allRedisOps, redisArray...)
				log.Printf("[DEBUG] Service map: found %d Redis operations in inc.Redis for service %s", len(redisArray), inc.Service)
			}
		}
		// Backward compatibility: also check call stack if present
		if len(inc.Stack) > 0 {
			callStack := parseCallStack(inc.Stack)
			stackRedisOps := collectRedisOperationsFromCallStack(callStack)
			if len(stackRedisOps) > 0 {
				allRedisOps = append(allRedisOps, stackRedisOps...)
				log.Printf("[DEBUG] Service map: found %d Redis operations in call stack for service %s", len(stackRedisOps), inc.Service)
			}
		}
		
		// Store full span if: trace should be kept, chunk is done, call stack present, dumps present, any profiling data present, OR always (to capture events even with empty call stack)
		// Always storing ensures we capture all profiling data regardless of call stack state
		shouldStoreFull := tb.ShouldKeep(inc.TraceID) || (inc.ChunkDone != nil && *inc.ChunkDone) || hasCallStack || hasDumps || hasProfilingData || true
		if shouldStoreFull {
			// Serialize call stack to JSON string
			stackJSON := "[]"
			if len(inc.Stack) > 0 {
				if stackBytes, err := json.Marshal(inc.Stack); err == nil {
					stackJSON = string(stackBytes)
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
			}
			
			// Serialize aggregated HTTP requests to JSON string
			httpJSON := "[]"
			if len(allHttpRequests) > 0 {
				if httpBytes, err := json.Marshal(allHttpRequests); err == nil {
					httpJSON = string(httpBytes)
					
					// Broadcast HTTP requests via WebSocket
					if wsHub != nil {
						// Send individual HTTP requests
						for _, httpReq := range allHttpRequests {
							if reqMap, ok := httpReq.(map[string]interface{}); ok {
								// Extract request data
								broadcastData := map[string]interface{}{
									"trace_id":   inc.TraceID,
									"span_id":    inc.SpanID,
									"service":    inc.Service,
									"span_name":  inc.Name,
									"timestamp":  time.Now().Unix(),
								}
								
								// Copy HTTP request fields
								if method, ok := reqMap["method"].(string); ok {
									broadcastData["method"] = method
								}
								if url, ok := reqMap["url"].(string); ok {
									broadcastData["url"] = url
								} else if url, ok := reqMap["URL"].(string); ok {
									broadcastData["url"] = url
								}
								if uri, ok := reqMap["uri"].(string); ok {
									broadcastData["uri"] = uri
								}
								if statusCode, ok := reqMap["status_code"].(float64); ok {
									broadcastData["status_code"] = int(statusCode)
								} else if statusCode, ok := reqMap["statusCode"].(float64); ok {
									broadcastData["status_code"] = int(statusCode)
								}
								if duration, ok := reqMap["duration_ms"].(float64); ok {
									broadcastData["duration_ms"] = duration
								} else if duration, ok := reqMap["duration"].(float64); ok {
									broadcastData["duration_ms"] = duration * 1000.0
								}
								if bytesSent, ok := reqMap["bytes_sent"].(float64); ok {
									broadcastData["bytes_sent"] = int64(bytesSent)
								} else if bytesSent, ok := reqMap["request_size"].(float64); ok {
									broadcastData["bytes_sent"] = int64(bytesSent)
								}
								if bytesRecv, ok := reqMap["bytes_received"].(float64); ok {
									broadcastData["bytes_received"] = int64(bytesRecv)
								} else if bytesRecv, ok := reqMap["response_size"].(float64); ok {
									broadcastData["bytes_received"] = int64(bytesRecv)
								}
								if queryString, ok := reqMap["query_string"].(string); ok {
									broadcastData["query_string"] = queryString
								}
								if reqHeaders, ok := reqMap["request_headers"].(map[string]interface{}); ok {
									broadcastData["request_headers"] = reqHeaders
								} else if reqHeadersRaw, ok := reqMap["request_headers_raw"].(string); ok && reqHeadersRaw != "" {
									// Try to parse raw headers
									var headersMap map[string]interface{}
									if err := json.Unmarshal([]byte(reqHeadersRaw), &headersMap); err == nil {
										broadcastData["request_headers"] = headersMap
									}
								}
								if respHeaders, ok := reqMap["response_headers"].(map[string]interface{}); ok {
									broadcastData["response_headers"] = respHeaders
								} else if respHeadersRaw, ok := reqMap["response_headers_raw"].(string); ok && respHeadersRaw != "" {
									// Try to parse raw headers
									var headersMap map[string]interface{}
									if err := json.Unmarshal([]byte(respHeadersRaw), &headersMap); err == nil {
										broadcastData["response_headers"] = headersMap
									}
								}
								if reqBody, ok := reqMap["request_body"]; ok {
									broadcastData["request_body"] = reqBody
								}
								if respBody, ok := reqMap["response_body"]; ok {
									broadcastData["response_body"] = respBody
								}
								
								wsHub.Broadcast("http", broadcastData)
							}
						}
					}
				}
			}
			
			// Serialize aggregated cache operations to JSON string
			cacheJSON := "[]"
			if len(allCacheOps) > 0 {
				if cacheBytes, err := json.Marshal(allCacheOps); err == nil {
					cacheJSON = string(cacheBytes)
				}
			}
			
			// Serialize aggregated Redis operations to JSON string
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
				
				// Parse tags to check for HTTP request data
				var tagsMap map[string]interface{}
				if err := json.Unmarshal(inc.Tags, &tagsMap); err == nil {
					// Broadcast HTTP request from tags if present
					if httpRequest, ok := tagsMap["http_request"].(map[string]interface{}); ok && wsHub != nil {
						// Get http_response if available
						var httpResponse map[string]interface{}
						if resp, ok := tagsMap["http_response"].(map[string]interface{}); ok {
							httpResponse = resp
						}
						
						// Build request object from tags
						broadcastData := map[string]interface{}{
							"trace_id":   inc.TraceID,
							"span_id":    inc.SpanID,
							"service":    inc.Service,
							"span_name":  inc.Name,
							"timestamp":  time.Now().Unix(),
						}
						
						// Build URL from http_request components
						scheme := ""
						if s, ok := httpRequest["scheme"].(string); ok {
							scheme = s
						}
						host := ""
						if h, ok := httpRequest["host"].(string); ok {
							host = h
						}
						uri := ""
						if u, ok := httpRequest["uri"].(string); ok {
							uri = u
						}
						
						// Construct full URL
						url := ""
						if scheme != "" && host != "" {
							url = fmt.Sprintf("%s://%s%s", scheme, host, uri)
						} else if host != "" {
							url = fmt.Sprintf("%s%s", host, uri)
						} else if uri != "" {
							url = uri
						}
						
						if url != "" {
							broadcastData["url"] = url
							broadcastData["uri"] = uri
							
							// Forward request_uri if available (contains actual path, not cleaned uri)
							if reqUri, ok := httpRequest["request_uri"].(string); ok && reqUri != "" {
								broadcastData["request_uri"] = reqUri
							}
							
							// Get method
							if m, ok := httpRequest["method"].(string); ok {
								broadcastData["method"] = m
							}
							
							// Get query string
							if qs, ok := httpRequest["query_string"].(string); ok && qs != "" {
								broadcastData["query_string"] = qs
								broadcastData["url"] = url + "?" + qs
							}
							
							// Get status code from http_response
							if httpResponse != nil {
								if sc, ok := httpResponse["status_code"].(float64); ok {
									broadcastData["status_code"] = int(sc)
								}
							}
							
							// Use span duration
							if inc.Duration > 0 {
								broadcastData["duration_ms"] = inc.Duration
							}
							
							// Get request/response headers and body if available
							if reqHeaders, ok := httpRequest["request_headers"].(map[string]interface{}); ok {
								broadcastData["request_headers"] = reqHeaders
							}
							if reqBody, ok := httpRequest["request_body"]; ok {
								broadcastData["request_body"] = reqBody
							}
							if httpResponse != nil {
								if respHeaders, ok := httpResponse["response_headers"].(map[string]interface{}); ok {
									broadcastData["response_headers"] = respHeaders
								}
								if respBody, ok := httpResponse["response_body"]; ok {
									broadcastData["response_body"] = respBody
								}
								// Get bytes sent/received
								if respSize, ok := httpResponse["response_size"].(float64); ok {
									broadcastData["bytes_received"] = int64(respSize)
								}
							}
							
							// Get bytes sent
							if reqSize, ok := httpRequest["request_size"].(float64); ok {
								broadcastData["bytes_sent"] = int64(reqSize)
							}
							
							wsHub.Broadcast("http", broadcastData)
						}
					}
					
					// If http_request is missing, add it (even if empty) to ensure it's always present
					if !hasHttpRequest {
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
				"start_ts":   time.UnixMilli(normalizedStartTS).Format("2006-01-02 15:04:05.000"),
				"end_ts":     time.UnixMilli(normalizedEndTS).Format("2006-01-02 15:04:05.000"),
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
				"created_at": createdAt.Format("2006-01-02 15:04:05.000"),
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
			// Debug: Log that we're storing full span
			log.Printf("[DEBUG] Storing full span: trace_id=%s, span_id=%s, sql_len=%d, stack_len=%d", 
				inc.TraceID, inc.SpanID, len(sqlJSON), len(stackJSON))
		}
		
		if inc.TraceID != "" {
			tb.Add(inc.TraceID, raw)
		}
		
		// Process span for service map
		if inc.Service != "" {
			processor := getOrCreateServiceMapProcessor(orgID, projectID)
			
			// Create a Span object for processing
			span := &Span{
				TraceID:  inc.TraceID,
				SpanID:   inc.SpanID,
				ParentID: inc.ParentID,
				Service:  inc.Service,
				Name:     inc.Name,
				StartTS:  inc.StartTS,
				EndTS:    inc.EndTS,
				Duration: inc.Duration,
				CPUms:    inc.CPUms,
				Status:   inc.Status,
			}
			
			// Parse network data if available
			if len(inc.Net) > 0 {
				var net map[string]interface{}
				if err := json.Unmarshal(inc.Net, &net); err == nil {
					span.Net = net
				}
			}
			
			// Populate HTTP requests for service map processing
			if len(allHttpRequests) > 0 {
				span.Http = allHttpRequests
				log.Printf("[DEBUG] Service map: populated %d HTTP requests for service %s", len(allHttpRequests), span.Service)
			}
			
			// Populate Redis operations for service map processing
			if len(allRedisOps) > 0 {
				span.Redis = allRedisOps
				log.Printf("[DEBUG] Service map: populated %d Redis operations for service %s", len(allRedisOps), span.Service)
			}
			
			// Populate SQL queries for service map processing
			if len(allSQLQueries) > 0 {
				span.Sql = allSQLQueries
				log.Printf("[DEBUG] Service map: populated %d SQL queries for service %s", len(allSQLQueries), span.Service)
			}
			
			// Populate cache operations for service map processing
			if len(allCacheOps) > 0 {
				span.Cache = allCacheOps
				log.Printf("[DEBUG] Service map: populated %d cache operations for service %s", len(allCacheOps), span.Service)
			}
			
			// Try to find parent span from TailBuffer
			var parentSpan *Span
			if inc.ParentID != nil && inc.TraceID != "" {
				// Get all spans for this trace from buffer
				traceSpans := tb.Get(inc.TraceID)
				for _, rawSpan := range traceSpans {
					var parentInc Incoming
					if err := json.Unmarshal(rawSpan, &parentInc); err == nil {
						if parentInc.SpanID == *inc.ParentID {
							parentSpan = &Span{
								TraceID:  parentInc.TraceID,
								SpanID:   parentInc.SpanID,
								ParentID: parentInc.ParentID,
								Service:  parentInc.Service,
								Name:     parentInc.Name,
								StartTS:  parentInc.StartTS,
								EndTS:    parentInc.EndTS,
								Duration: parentInc.Duration,
								CPUms:    parentInc.CPUms,
								Status:   parentInc.Status,
							}
							break
						}
					}
				}
			}
			
			// Process the span with its parent
			processor.ProcessSpan(span, parentSpan)
		}
		
		breaker.RecordSuccess()
		processingDuration.Observe(time.Since(start).Seconds())
		
		// Track traces
		if inc.ParentID == nil {
			tracesTotal.WithLabelValues(inc.Service).Inc()
		}
	}
}
