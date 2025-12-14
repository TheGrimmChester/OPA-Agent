package main

import (
	"fmt"
	"strconv"
	"strings"
)

// BuildClickHouseWhere builds a ClickHouse WHERE clause from a filter AST
func BuildClickHouseWhere(ast *FilterAST, tenantFilter string) (string, error) {
	if ast == nil {
		if tenantFilter == "" {
			return "", nil
		}
		// Return tenant filter with WHERE prefix if needed
		if strings.HasPrefix(tenantFilter, " WHERE ") {
			return tenantFilter, nil
		}
		return " WHERE " + tenantFilter, nil
	}

	whereClause, err := buildWhereClause(ast)
	if err != nil {
		return "", err
	}

	if tenantFilter == "" {
		return " WHERE " + whereClause, nil
	}

	// Combine tenant filter with filter query
	// Remove WHERE prefix from tenant filter if present
	cleanTenantFilter := strings.TrimPrefix(tenantFilter, " WHERE ")
	return " WHERE " + cleanTenantFilter + " AND (" + whereClause + ")", nil
}

func buildWhereClause(ast *FilterAST) (string, error) {
	if ast == nil {
		return "", nil
	}

	switch ast.Type {
	case "comparison":
		return buildComparison(ast)
	case "logical":
		return buildLogical(ast)
	default:
		return "", fmt.Errorf("unknown AST node type: %s", ast.Type)
	}
}

func buildComparison(ast *FilterAST) (string, error) {
	fieldExpr, err := buildFieldExpression(ast.Field)
	if err != nil {
		return "", err
	}

	switch ast.Operator {
	case "EQUALS":
		return fmt.Sprintf("%s = %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "NOT_EQUALS":
		return fmt.Sprintf("%s != %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "GREATER_THAN":
		return fmt.Sprintf("%s > %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "LESS_THAN":
		return fmt.Sprintf("%s < %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "GREATER_THAN_OR_EQUAL":
		return fmt.Sprintf("%s >= %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "LESS_THAN_OR_EQUAL":
		return fmt.Sprintf("%s <= %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "LIKE":
		return fmt.Sprintf("%s LIKE %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "NOT_LIKE":
		return fmt.Sprintf("%s NOT LIKE %s", fieldExpr, buildValueExpression(ast.Value)), nil
	case "IN":
		return buildInExpression(fieldExpr, ast.Value)
	case "NOT_IN":
		inExpr, err := buildInExpression(fieldExpr, ast.Value)
		if err != nil {
			return "", err
		}
		return "NOT (" + inExpr + ")", nil
	default:
		return "", fmt.Errorf("unknown operator: %s", ast.Operator)
	}
}

func buildLogical(ast *FilterAST) (string, error) {
	switch ast.LogicalOp {
	case "AND":
		left, err := buildWhereClause(ast.Left)
		if err != nil {
			return "", err
		}
		right, err := buildWhereClause(ast.Right)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s) AND (%s)", left, right), nil
	case "OR":
		left, err := buildWhereClause(ast.Left)
		if err != nil {
			return "", err
		}
		right, err := buildWhereClause(ast.Right)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s) OR (%s)", left, right), nil
	case "NOT":
		operand, err := buildWhereClause(ast.Operand)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("NOT (%s)", operand), nil
	default:
		return "", fmt.Errorf("unknown logical operator: %s", ast.LogicalOp)
	}
}

func buildFieldExpression(field string) (string, error) {
	if field == "" {
		return "", fmt.Errorf("field name cannot be empty")
	}
	
	// Handle nested JSON fields
	if strings.HasPrefix(field, "tags.") {
		parts := strings.Split(field, ".")
		if len(parts) < 2 {
			return "", fmt.Errorf("invalid tags field: %s", field)
		}
		
		// tags.http_request.method -> JSONExtractString(tags, 'http_request', 'method')
		if len(parts) == 3 {
			return fmt.Sprintf("JSONExtractString(tags, '%s', '%s')",
				escapeSQL(parts[1]), escapeSQL(parts[2])), nil
		}
		// tags.http_request.http_response.status_code -> nested extraction
		if len(parts) == 4 {
			return fmt.Sprintf("JSONExtractString(JSONExtract(tags, '%s'), '%s', '%s')",
				escapeSQL(parts[1]), escapeSQL(parts[2]), escapeSQL(parts[3])), nil
		}
		// For deeper nesting, use recursive extraction
		// This is a simplified version - may need enhancement for complex cases
		return fmt.Sprintf("JSONExtractString(tags, '%s')", escapeSQL(strings.Join(parts[1:], "."))), nil
	}

	if strings.HasPrefix(field, "http.") {
		// For HTTP fields in the http array, we need to check if any element matches
		// This is complex - we'll use arrayExists or similar
		fieldName := strings.TrimPrefix(field, "http.")
		// For now, use a simpler approach: check if field exists in any http array element
		// This requires the query to use ARRAY JOIN or arrayExists
		return fmt.Sprintf("arrayExists(x -> JSONExtractString(x, '%s') != '', JSONExtractArrayRaw(http))",
			escapeSQL(fieldName)), nil
	}

	if strings.HasPrefix(field, "sql.") {
		fieldName := strings.TrimPrefix(field, "sql.")
		// Similar to http, SQL is an array
		return fmt.Sprintf("arrayExists(x -> JSONExtractString(x, '%s') != '', JSONExtractArrayRaw(sql))",
			escapeSQL(fieldName)), nil
	}

	if strings.HasPrefix(field, "net.") {
		fieldName := strings.TrimPrefix(field, "net.")
		return fmt.Sprintf("JSONExtractString(net, '%s')", escapeSQL(fieldName)), nil
	}

	// Direct field access (from spans_min or spans_full)
	// Map common field names to actual column names
	fieldMap := map[string]string{
		"service":           "service",
		"name":              "name",
		"status":             "status",
		"trace_id":          "trace_id",
		"span_id":           "span_id",
		"parent_id":         "parent_id",
		"duration_ms":       "duration_ms",
		"cpu_ms":            "cpu_ms",
		"start_ts":          "start_ts",
		"end_ts":            "end_ts",
		"url_scheme":        "url_scheme",
		"url_host":          "url_host",
		"url_path":          "url_path",
		"language":           "language",
		"language_version":  "language_version",
		"framework":         "framework",
		"framework_version": "framework_version",
	}

	if mapped, ok := fieldMap[field]; ok {
		return mapped, nil
	}

	// Unknown field - try direct access (may fail at query time)
	return escapeSQL(field), nil
}

func buildValueExpression(value interface{}) string {
	switch v := value.(type) {
	case string:
		return "'" + escapeSQL(v) + "'"
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return "'" + escapeSQL(fmt.Sprintf("%v", v)) + "'"
	}
}

func buildInExpression(fieldExpr string, values interface{}) (string, error) {
	valueList, ok := values.([]interface{})
	if !ok {
		return "", fmt.Errorf("IN operator requires an array of values")
	}

	if len(valueList) == 0 {
		return "1=0", nil // Empty IN list is always false
	}

	var valueExprs []string
	for _, val := range valueList {
		valueExprs = append(valueExprs, buildValueExpression(val))
	}

	return fmt.Sprintf("%s IN (%s)", fieldExpr, strings.Join(valueExprs, ", ")), nil
}

// adaptFilterForJoin adapts a filter WHERE clause to work with JOIN queries
// by adding table prefixes where needed
func adaptFilterForJoin(whereClause, tablePrefix string) string {
	// Simple adaptation: add table prefix to direct field references
	// This is a simplified version - may need enhancement for complex cases
	directFields := []string{
		"service", "name", "status", "trace_id", "span_id", "parent_id",
		"duration_ms", "cpu_ms", "start_ts", "end_ts",
		"url_scheme", "url_host", "url_path",
		"language", "language_version", "framework", "framework_version",
	}
	
	result := whereClause
	for _, field := range directFields {
		// Replace field references that are not already prefixed
		replacement := fmt.Sprintf("%s.%s", tablePrefix, field)
		result = strings.ReplaceAll(result, field, replacement)
	}
	
	return result
}

