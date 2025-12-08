package main

import (
	"fmt"
	"strconv"
	"strings"
)

// Helper functions for extracting values from maps

func getString(row map[string]interface{}, key string) string {
	if v, ok := row[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func getStringPtr(row map[string]interface{}, key string) *string {
	if v, ok := row[key]; ok {
		if s, ok := v.(string); ok {
			if s == "" {
				return nil
			}
			return &s
		}
	}
	return nil
}

func getFloat64(row map[string]interface{}, key string) float64 {
	if v, ok := row[key]; ok {
		switch val := v.(type) {
		case float64:
			return val
		case float32:
			return float64(val)
		case int:
			return float64(val)
		case int64:
			return float64(val)
		case uint64:
			return float64(val)
		case string:
			// ClickHouse may return numbers as strings in JSONEachRow format
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				return f
			}
		}
	}
	return 0
}

func getUint64(row map[string]interface{}, key string) uint64 {
	if v, ok := row[key]; ok {
		switch val := v.(type) {
		case uint64:
			return val
		case int64:
			return uint64(val)
		case int:
			return uint64(val)
		case float64:
			return uint64(val)
		case string:
			// ClickHouse may return numbers as strings in JSONEachRow format
			if u, err := strconv.ParseUint(val, 10, 64); err == nil {
				return u
			}
			// Try parsing as float first, then convert (handles scientific notation)
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				return uint64(f)
			}
		}
	}
	return 0
}

// escapeSQL escapes SQL strings (simple version, should use parameterized queries in production)
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func getFloat64FromMap(m map[string]interface{}, key string) float64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case float64:
			return val
		case float32:
			return float64(val)
		case int:
			return float64(val)
		case int64:
			return float64(val)
		case uint64:
			return float64(val)
		}
	}
	return 0
}

func getUint64FromMap(m map[string]interface{}, key string) uint64 {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case uint64:
			return val
		case int:
			return uint64(val)
		case int64:
			return uint64(val)
		case float64:
			return uint64(val)
		}
	}
	return 0
}

func getStringPtrFromMap(m map[string]interface{}, key string) *string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			if s == "" {
				return nil
			}
			return &s
		}
	}
	return nil
}

// getMapKeys returns all keys from a map[string]interface{} as a slice of strings
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

