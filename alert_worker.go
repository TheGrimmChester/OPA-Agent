package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Alert struct {
	ID            string                 `json:"id"`
	Name          string                 `json:"name"`
	Description   string                 `json:"description"`
	Enabled       bool                   `json:"enabled"`
	ConditionType string                 `json:"condition_type"` // 'duration', 'error_rate', 'throughput', 'custom'
	ConditionConfig map[string]interface{} `json:"condition_config"`
	ActionType    string                 `json:"action_type"`    // 'email', 'webhook', 'slack'
	ActionConfig  map[string]interface{} `json:"action_config"`
	Service       *string                `json:"service,omitempty"`
	Language      *string                `json:"language,omitempty"`
	Framework     *string                `json:"framework,omitempty"`
}

type AlertWorker struct {
	mu          sync.RWMutex
	alerts      map[string]*Alert
	queryClient *ClickHouseQuery
	checkInterval time.Duration
	lastCheck    time.Time
}

func NewAlertWorker(queryClient *ClickHouseQuery, checkInterval time.Duration) *AlertWorker {
	return &AlertWorker{
		alerts:        make(map[string]*Alert),
		queryClient:    queryClient,
		checkInterval: checkInterval,
	}
}

func (aw *AlertWorker) AddAlert(alert *Alert) {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	aw.alerts[alert.ID] = alert
}

func (aw *AlertWorker) RemoveAlert(id string) {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	delete(aw.alerts, id)
}

func (aw *AlertWorker) GetAlert(id string) *Alert {
	aw.mu.RLock()
	defer aw.mu.RUnlock()
	return aw.alerts[id]
}

func (aw *AlertWorker) ListAlerts() []*Alert {
	aw.mu.RLock()
	defer aw.mu.RUnlock()
	alerts := make([]*Alert, 0, len(aw.alerts))
	for _, alert := range aw.alerts {
		alerts = append(alerts, alert)
	}
	return alerts
}

func (aw *AlertWorker) Start() {
	go aw.run()
}

func (aw *AlertWorker) run() {
	ticker := time.NewTicker(aw.checkInterval)
	defer ticker.Stop()
	
	for range ticker.C {
		aw.checkAlerts()
	}
}

func (aw *AlertWorker) checkAlerts() {
	aw.mu.RLock()
	alerts := make([]*Alert, 0, len(aw.alerts))
	for _, alert := range aw.alerts {
		if alert.Enabled {
			alerts = append(alerts, alert)
		}
	}
	aw.mu.RUnlock()
	
	for _, alert := range alerts {
		aw.checkAlert(alert)
	}
	
	aw.lastCheck = time.Now()
}

func (aw *AlertWorker) checkAlert(alert *Alert) {
	// Build query based on condition type
	query := aw.buildQuery(alert)
	if query == "" {
		return
	}
	
	rows, err := aw.queryClient.Query(query)
	if err != nil {
		LogError(err, "Error checking alert", map[string]interface{}{
			"alert_id": alert.ID,
			"alert_name": alert.Name,
		})
		return
	}
	
	// Evaluate condition
	triggered, value := aw.evaluateCondition(alert, rows)
	if triggered {
		log.Printf("Alert %s triggered with value: %v", alert.Name, value)
		aw.triggerAlert(alert, value)
	}
}

func (aw *AlertWorker) buildQuery(alert *Alert) string {
	timeFrom := time.Now().Add(-1 * time.Hour).Format("2006-01-02 15:04:05")
	
	query := "SELECT "
	
	switch alert.ConditionType {
	case "duration":
		query += "avg(duration_ms) as value, count(*) as count FROM opa.spans_min WHERE 1=1"
	case "error_rate":
		query += "sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as value, count(*) as count FROM opa.spans_min WHERE 1=1"
	case "throughput":
		query += "count(DISTINCT trace_id) as value, count(*) as count FROM opa.spans_min WHERE 1=1"
	default:
		return ""
	}
	
	query += fmt.Sprintf(" AND start_ts >= '%s'", timeFrom)
	
	if alert.Service != nil && *alert.Service != "" {
		query += fmt.Sprintf(" AND service = '%s'", strings.ReplaceAll(*alert.Service, "'", "''"))
	}
	if alert.Language != nil && *alert.Language != "" {
		query += fmt.Sprintf(" AND language = '%s'", strings.ReplaceAll(*alert.Language, "'", "''"))
	}
	if alert.Framework != nil && *alert.Framework != "" {
		query += fmt.Sprintf(" AND framework = '%s'", strings.ReplaceAll(*alert.Framework, "'", "''"))
	}
	
	return query
}

func (aw *AlertWorker) evaluateCondition(alert *Alert, rows []map[string]interface{}) (bool, interface{}) {
	if len(rows) == 0 {
		return false, nil
	}
	
	row := rows[0]
	value := getFloat64(row, "value")
	
	threshold, ok := alert.ConditionConfig["threshold"].(float64)
	if !ok {
		return false, nil
	}
	
	operator, _ := alert.ConditionConfig["operator"].(string)
	if operator == "" {
		operator = "gt" // default to greater than
	}
	
	triggered := false
	switch operator {
	case "gt", ">":
		triggered = value > threshold
	case "gte", ">=":
		triggered = value >= threshold
	case "lt", "<":
		triggered = value < threshold
	case "lte", "<=":
		triggered = value <= threshold
	case "eq", "==":
		triggered = value == threshold
	}
	
	return triggered, value
}

func (aw *AlertWorker) triggerAlert(alert *Alert, value interface{}) {
	LogWarn("Alert triggered", map[string]interface{}{
		"alert_id":   alert.ID,
		"alert_name": alert.Name,
		"value":      value,
	})
	
	// Format alert message
	message := fmt.Sprintf("Alert '%s' triggered: %v", alert.Name, value)
	
	// Send notification based on action type
	switch alert.ActionType {
	case "webhook":
		aw.sendWebhook(alert, message, value)
	case "email":
		aw.sendEmail(alert, message, value)
	case "slack":
		aw.sendSlack(alert, message, value)
	default:
		// Log only
		log.Printf("Alert '%s' triggered with value: %v (no action configured)", alert.Name, value)
	}
	
	// Record in alert_history
	aw.recordAlertHistory(alert, value)
}

func (aw *AlertWorker) sendWebhook(alert *Alert, message string, value interface{}) {
	webhookURL, ok := alert.ActionConfig["url"].(string)
	if !ok || webhookURL == "" {
		LogError(nil, "Webhook URL not configured", map[string]interface{}{
			"alert_id": alert.ID,
		})
		return
	}
	
	payload := map[string]interface{}{
		"alert_id":   alert.ID,
		"alert_name": alert.Name,
		"message":    message,
		"value":      value,
		"timestamp":  time.Now().Format(time.RFC3339),
	}
	
	jsonData, err := json.Marshal(payload)
	if err != nil {
		LogError(err, "Failed to marshal webhook payload", nil)
		return
	}
	
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		LogError(err, "Failed to send webhook", map[string]interface{}{
			"alert_id": alert.ID,
			"url":      webhookURL,
		})
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode >= 400 {
		LogWarn("Webhook returned error status", map[string]interface{}{
			"alert_id":    alert.ID,
			"status_code": resp.StatusCode,
		})
	}
}

func (aw *AlertWorker) sendEmail(alert *Alert, message string, value interface{}) {
	// Email sending would require SMTP configuration
	// For now, just log it
	emailTo, ok := alert.ActionConfig["to"].(string)
	if !ok || emailTo == "" {
		LogError(nil, "Email recipient not configured", map[string]interface{}{
			"alert_id": alert.ID,
		})
		return
	}
	
	log.Printf("Email alert (to: %s): %s", emailTo, message)
}

func (aw *AlertWorker) sendSlack(alert *Alert, message string, value interface{}) {
	webhookURL, ok := alert.ActionConfig["webhook_url"].(string)
	if !ok || webhookURL == "" {
		LogError(nil, "Slack webhook URL not configured", map[string]interface{}{
			"alert_id": alert.ID,
		})
		return
	}
	
	payload := map[string]interface{}{
		"text": fmt.Sprintf("🚨 *Alert: %s*\n%s\nValue: %v", alert.Name, message, value),
	}
	
	jsonData, err := json.Marshal(payload)
	if err != nil {
		LogError(err, "Failed to marshal Slack payload", nil)
		return
	}
	
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		LogError(err, "Failed to send Slack notification", map[string]interface{}{
			"alert_id": alert.ID,
		})
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode >= 400 {
		LogWarn("Slack webhook returned error status", map[string]interface{}{
			"alert_id":    alert.ID,
			"status_code": resp.StatusCode,
		})
	}
}

func (aw *AlertWorker) recordAlertHistory(alert *Alert, value interface{}) {
	// Store in ClickHouse alert_history table
	// This would be done via the ClickHouseWriter
	// For now, just log it
	log.Printf("Alert history: %s triggered at %s with value: %v", 
		alert.ID, time.Now().Format(time.RFC3339), value)
}

