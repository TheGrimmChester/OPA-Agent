package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

type Anomaly struct {
	ID          string                 `json:"id"`
	Type        string                 `json:"type"` // 'duration', 'error_rate', 'throughput'
	Service     string                 `json:"service"`
	Metric      string                 `json:"metric"`
	Value       float64                `json:"value"`
	Expected    float64                `json:"expected"`
	Score       float64                `json:"score"` // 0-1, higher = more anomalous
	Severity    string                 `json:"severity"` // 'low', 'medium', 'high', 'critical'
	DetectedAt  time.Time              `json:"detected_at"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

type AnomalyDetector struct {
	mu          sync.RWMutex
	queryClient *ClickHouseQuery
	history     map[string][]float64 // service:metric -> values
	windowSize  int
}

func NewAnomalyDetector(queryClient *ClickHouseQuery, windowSize int) *AnomalyDetector {
	return &AnomalyDetector{
		queryClient: queryClient,
		history:     make(map[string][]float64),
		windowSize:  windowSize,
	}
}

func (ad *AnomalyDetector) DetectAnomalies(service string, timeWindow time.Duration) ([]*Anomaly, error) {
	timeFrom := time.Now().Add(-timeWindow).Format("2006-01-02 15:04:05")
	
	// Get metrics for the service
	query := fmt.Sprintf(`SELECT 
		avg(duration_ms) as avg_duration,
		sum(CASE WHEN status = 'error' OR status = '0' THEN 1 ELSE 0 END) * 100.0 / count(*) as error_rate,
		count(DISTINCT trace_id) as throughput
		FROM opa.spans_min 
		WHERE service = '%s' AND start_ts >= '%s'
		GROUP BY toStartOfHour(start_ts)
		ORDER BY toStartOfHour(start_ts)`,
		strings.ReplaceAll(service, "'", "''"), timeFrom)
	
	rows, err := ad.queryClient.Query(query)
	if err != nil {
		return nil, err
	}
	
	var anomalies []*Anomaly
	
	// Collect values for statistical analysis
	var durations, errorRates, throughputs []float64
	for _, row := range rows {
		if d := getFloat64(row, "avg_duration"); d > 0 {
			durations = append(durations, d)
		}
		if e := getFloat64(row, "error_rate"); e >= 0 {
			errorRates = append(errorRates, e)
		}
		if t := getFloat64(row, "throughput"); t > 0 {
			throughputs = append(throughputs, t)
		}
	}
	
	// Detect anomalies in each metric
	if len(durations) > 0 {
		anoms := ad.detectMetricAnomalies(service, "duration", durations, "avg_duration")
		anomalies = append(anomalies, anoms...)
	}
	
	if len(errorRates) > 0 {
		anoms := ad.detectMetricAnomalies(service, "error_rate", errorRates, "error_rate")
		anomalies = append(anomalies, anoms...)
	}
	
	if len(throughputs) > 0 {
		anoms := ad.detectMetricAnomalies(service, "throughput", throughputs, "throughput")
		anomalies = append(anomalies, anoms...)
	}
	
	return anomalies, nil
}

func (ad *AnomalyDetector) detectMetricAnomalies(service, metricType string, values []float64, metricName string) []*Anomaly {
	if len(values) < 3 {
		return nil // Need at least 3 data points
	}
	
	// Calculate mean and standard deviation
	mean := ad.mean(values)
	stdDev := ad.stdDev(values, mean)
	
	if stdDev == 0 {
		return nil // No variation
	}
	
	var anomalies []*Anomaly
	
	// Check each value using z-score
	for i, value := range values {
		zScore := math.Abs((value - mean) / stdDev)
		
		// Threshold: z-score > 2.5 is anomalous
		if zScore > 2.5 {
			score := math.Min(zScore/5.0, 1.0) // Normalize to 0-1
			severity := ad.determineSeverity(zScore)
			
			anomaly := &Anomaly{
				ID:         fmt.Sprintf("anomaly-%s-%s-%d", service, metricType, time.Now().UnixNano()+int64(i)),
				Type:       metricType,
				Service:    service,
				Metric:     metricName,
				Value:      value,
				Expected:   mean,
				Score:      score,
				Severity:   severity,
				DetectedAt: time.Now(),
				Metadata: map[string]interface{}{
					"z_score":    zScore,
					"mean":       mean,
					"std_dev":    stdDev,
					"deviation":  value - mean,
					"deviation_pct": ((value - mean) / mean) * 100,
				},
			}
			anomalies = append(anomalies, anomaly)
		}
	}
	
	return anomalies
}

func (ad *AnomalyDetector) mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func (ad *AnomalyDetector) stdDev(values []float64, mean float64) float64 {
	if len(values) < 2 {
		return 0
	}
	sumSqDiff := 0.0
	for _, v := range values {
		diff := v - mean
		sumSqDiff += diff * diff
	}
	variance := sumSqDiff / float64(len(values)-1)
	return math.Sqrt(variance)
}

func (ad *AnomalyDetector) determineSeverity(zScore float64) string {
	if zScore >= 4.0 {
		return "critical"
	} else if zScore >= 3.0 {
		return "high"
	} else if zScore >= 2.5 {
		return "medium"
	}
	return "low"
}

func (ad *AnomalyDetector) StoreAnomaly(anomaly *Anomaly) error {
	metadataJSON, _ := json.Marshal(anomaly.Metadata)
	query := fmt.Sprintf(`INSERT INTO opa.anomalies 
		(id, type, service, metric, value, expected, score, severity, detected_at, metadata)
		VALUES ('%s', '%s', '%s', '%s', %f, %f, %f, '%s', '%s', '%s')`,
		strings.ReplaceAll(anomaly.ID, "'", "''"),
		strings.ReplaceAll(anomaly.Type, "'", "''"),
		strings.ReplaceAll(anomaly.Service, "'", "''"),
		strings.ReplaceAll(anomaly.Metric, "'", "''"),
		anomaly.Value,
		anomaly.Expected,
		anomaly.Score,
		strings.ReplaceAll(anomaly.Severity, "'", "''"),
		anomaly.DetectedAt.Format("2006-01-02 15:04:05"),
		strings.ReplaceAll(string(metadataJSON), "'", "''"),
	)
	return ad.queryClient.Execute(query)
}

