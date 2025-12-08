package main

import (
	"fmt"
	"strings"
)

// Simple query parser for NRQL-like queries
// Supports: SELECT, FROM, WHERE, GROUP BY, ORDER BY, LIMIT
type QueryParser struct {
	queryClient *ClickHouseQuery
}

func NewQueryParser(queryClient *ClickHouseQuery) *QueryParser {
	return &QueryParser{
		queryClient: queryClient,
	}
}

func (qp *QueryParser) ParseAndExecute(query string, ctx *TenantContext) ([]map[string]interface{}, error) {
	// Simple query parser - convert to ClickHouse SQL
	// This is a basic implementation
	
	query = strings.TrimSpace(query)
	query = strings.ToUpper(query)
	
	// Extract SELECT clause
	if !strings.HasPrefix(query, "SELECT") {
		return nil, fmt.Errorf("query must start with SELECT")
	}
	
	// For now, support direct ClickHouse queries with tenant filtering
	// In production, would parse and translate NRQL to ClickHouse SQL
	
	// Add tenant filter if not present
	if !strings.Contains(strings.ToUpper(query), "ORGANIZATION_ID") {
		// Try to add WHERE clause with tenant filter
		if strings.Contains(query, "WHERE") {
			query = strings.Replace(query, "WHERE", 
				fmt.Sprintf("WHERE organization_id = '%s' AND project_id = '%s' AND", 
					escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID)), 1)
		} else {
			// Add WHERE clause before GROUP BY, ORDER BY, or LIMIT
			insertPos := len(query)
			if idx := strings.Index(query, "GROUP BY"); idx > 0 {
				insertPos = idx
			}
			if idx := strings.Index(query, "ORDER BY"); idx > 0 && idx < insertPos {
				insertPos = idx
			}
			if idx := strings.Index(query, "LIMIT"); idx > 0 && idx < insertPos {
				insertPos = idx
			}
			query = query[:insertPos] + fmt.Sprintf(" WHERE organization_id = '%s' AND project_id = '%s'", 
				escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID)) + query[insertPos:]
		}
	}
	
	// Execute query
	rows, err := qp.queryClient.Query(query)
	if err != nil {
		return nil, err
	}
	
	var results []map[string]interface{}
	for _, row := range rows {
		result := make(map[string]interface{})
		for key, value := range row {
			result[key] = value
		}
		results = append(results, result)
	}
	
	return results, nil
}

