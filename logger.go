package main

import (
	"encoding/json"
	"os"
	"time"
)

// LogLevel represents the severity level of a log entry
type LogLevel string

const (
	LevelError LogLevel = "ERROR"
	LevelWarn  LogLevel = "WARN"
	LevelInfo  LogLevel = "INFO"
)

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp  string                 `json:"timestamp"`
	Level      LogLevel               `json:"level"`
	Message    string                 `json:"message"`
	Error      string                 `json:"error,omitempty"`
	Path       string                 `json:"path,omitempty"`
	Method     string                 `json:"method,omitempty"`
	StatusCode int                    `json:"status_code,omitempty"`
	DurationMs float64                `json:"duration_ms,omitempty"`
	RemoteAddr string                 `json:"remote_addr,omitempty"`
	UserAgent  string                 `json:"user_agent,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

// logEntry writes a structured log entry to stderr as JSON
func logEntry(entry LogEntry) {
	entry.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	
	jsonData, err := json.Marshal(entry)
	if err != nil {
		// Fallback to simple log if JSON marshaling fails
		os.Stderr.WriteString(entry.Timestamp + " [" + string(entry.Level) + "] " + entry.Message + "\n")
		return
	}
	
	os.Stderr.Write(jsonData)
	os.Stderr.WriteString("\n")
}

// LogError logs an error with structured JSON format
func LogError(err error, msg string, fields ...map[string]interface{}) {
	entry := LogEntry{
		Level:   LevelError,
		Message: msg,
	}
	
	if err != nil {
		entry.Error = err.Error()
	}
	
	if len(fields) > 0 {
		entry.Fields = fields[0]
	}
	
	logEntry(entry)
}

// LogWarn logs a warning with structured JSON format
func LogWarn(msg string, fields ...map[string]interface{}) {
	entry := LogEntry{
		Level:   LevelWarn,
		Message: msg,
	}
	
	if len(fields) > 0 {
		entry.Fields = fields[0]
	}
	
	logEntry(entry)
}

// LogInfo logs an info message with structured JSON format
func LogInfo(msg string, fields ...map[string]interface{}) {
	entry := LogEntry{
		Level:   LevelInfo,
		Message: msg,
	}
	
	if len(fields) > 0 {
		entry.Fields = fields[0]
	}
	
	logEntry(entry)
}

// LogHTTPResponse logs an HTTP response with structured JSON format
func LogHTTPResponse(statusCode int, method, path string, duration time.Duration, fields ...map[string]interface{}) {
	entry := LogEntry{
		Level:      getLogLevelForStatusCode(statusCode),
		Message:    "HTTP request completed",
		Path:       path,
		Method:     method,
		StatusCode: statusCode,
		DurationMs: float64(duration.Nanoseconds()) / 1e6, // Convert to milliseconds
	}
	
	if len(fields) > 0 {
		entry.Fields = fields[0]
		if remoteAddr, ok := fields[0]["remote_addr"].(string); ok {
			entry.RemoteAddr = remoteAddr
		}
		if userAgent, ok := fields[0]["user_agent"].(string); ok {
			entry.UserAgent = userAgent
		}
	}
	
	logEntry(entry)
}

// getLogLevelForStatusCode returns the appropriate log level for an HTTP status code
func getLogLevelForStatusCode(statusCode int) LogLevel {
	if statusCode >= 500 {
		return LevelError
	} else if statusCode >= 400 {
		return LevelWarn
	}
	return LevelInfo
}

