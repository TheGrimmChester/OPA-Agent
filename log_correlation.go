package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type CorrelatedLogEntry struct {
	ID        string                 `json:"id"`
	TraceID   string                 `json:"trace_id"`
	SpanID    string                 `json:"span_id,omitempty"`
	Service   string                 `json:"service"`
	Level     string                 `json:"level"` // debug, info, warn, error
	Message   string                 `json:"message"`
	Timestamp time.Time              `json:"timestamp"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

type LogCorrelation struct {
	queryClient *ClickHouseQuery
}

func NewLogCorrelation(queryClient *ClickHouseQuery) *LogCorrelation {
	return &LogCorrelation{
		queryClient: queryClient,
	}
}

func (lc *LogCorrelation) StoreLog(log *CorrelatedLogEntry) error {
	fieldsJSON, _ := json.Marshal(log.Fields)
	spanID := "NULL"
	if log.SpanID != "" {
		spanID = fmt.Sprintf("'%s'", strings.ReplaceAll(log.SpanID, "'", "''"))
	}
	query := fmt.Sprintf(`INSERT INTO opa.logs 
		(id, trace_id, span_id, service, level, message, timestamp, fields)
		VALUES ('%s', '%s', %s, '%s', '%s', '%s', '%s', '%s')`,
		strings.ReplaceAll(log.ID, "'", "''"),
		strings.ReplaceAll(log.TraceID, "'", "''"),
		spanID,
		strings.ReplaceAll(log.Service, "'", "''"),
		strings.ReplaceAll(log.Level, "'", "''"),
		strings.ReplaceAll(log.Message, "'", "''"),
		log.Timestamp.Format("2006-01-02 15:04:05.000"),
		strings.ReplaceAll(string(fieldsJSON), "'", "''"),
	)
	return lc.queryClient.Execute(query)
}

func (lc *LogCorrelation) GetLogsForTrace(traceID string, limit int) ([]*CorrelatedLogEntry, error) {
	query := fmt.Sprintf(`SELECT id, trace_id, span_id, service, level, message, timestamp, fields
		FROM opa.logs 
		WHERE trace_id = '%s'
		ORDER BY timestamp DESC
		LIMIT %d`,
		strings.ReplaceAll(traceID, "'", "''"), limit)
	
	rows, err := lc.queryClient.Query(query)
	if err != nil {
		return nil, err
	}
	
	var logs []*CorrelatedLogEntry
	for _, row := range rows {
		var fields map[string]interface{}
		fieldsStr := getString(row, "fields")
		json.Unmarshal([]byte(fieldsStr), &fields)
		
		timestamp, _ := time.Parse("2006-01-02 15:04:05.000", getString(row, "timestamp"))
		
		logs = append(logs, &CorrelatedLogEntry{
			ID:        getString(row, "id"),
			TraceID:   getString(row, "trace_id"),
			SpanID:    getString(row, "span_id"),
			Service:   getString(row, "service"),
			Level:     getString(row, "level"),
			Message:   getString(row, "message"),
			Timestamp: timestamp,
			Fields:    fields,
		})
	}
	
	return logs, nil
}

func (lc *LogCorrelation) GetLogsForSpan(traceID, spanID string, limit int) ([]*CorrelatedLogEntry, error) {
	query := fmt.Sprintf(`SELECT id, trace_id, span_id, service, level, message, timestamp, fields
		FROM opa.logs 
		WHERE trace_id = '%s' AND span_id = '%s'
		ORDER BY timestamp DESC
		LIMIT %d`,
		strings.ReplaceAll(traceID, "'", "''"), strings.ReplaceAll(spanID, "'", "''"), limit)
	
	rows, err := lc.queryClient.Query(query)
	if err != nil {
		return nil, err
	}
	
	var logs []*CorrelatedLogEntry
	for _, row := range rows {
		var fields map[string]interface{}
		fieldsStr := getString(row, "fields")
		json.Unmarshal([]byte(fieldsStr), &fields)
		
		timestamp, _ := time.Parse("2006-01-02 15:04:05.000", getString(row, "timestamp"))
		
		logs = append(logs, &CorrelatedLogEntry{
			ID:        getString(row, "id"),
			TraceID:   getString(row, "trace_id"),
			SpanID:    getString(row, "span_id"),
			Service:   getString(row, "service"),
			Level:     getString(row, "level"),
			Message:   getString(row, "message"),
			Timestamp: timestamp,
			Fields:    fields,
		})
	}
	
	return logs, nil
}

