package main

import (
	"fmt"
	"strconv"
	"strings"
)

// FilterAST represents the abstract syntax tree of a filter query
type FilterAST struct {
	Type     string      // "comparison", "logical"
	Field    string      // For comparison nodes
	Operator string      // For comparison nodes: EQUALS, GREATER_THAN, etc.
	Value    interface{} // For comparison nodes
	Left     *FilterAST  // For logical nodes
	Right    *FilterAST  // For logical nodes
	Operand  *FilterAST  // For NOT operator
	LogicalOp string     // For logical nodes: AND, OR, NOT
}

// ParseFilterQuery parses a filter query string into an AST
// Syntax examples:
// - status_code:500
// - duration_ms:>1000
// - service:api AND method:POST
// - (status_code:>=400 AND status_code:<500) OR error:true
func ParseFilterQuery(query string) (*FilterAST, error) {
	if query == "" || strings.TrimSpace(query) == "" {
		return nil, nil
	}

	tokens := tokenizeFilterQuery(query)
	parser := &filterParser{tokens: tokens, pos: 0}
	return parser.parse()
}

type filterToken struct {
	Type  string
	Value string
}

type filterParser struct {
	tokens []filterToken
	pos    int
}

func (p *filterParser) peek() *filterToken {
	if p.pos >= len(p.tokens) {
		return &filterToken{Type: "EOF", Value: ""}
	}
	return &p.tokens[p.pos]
}

func (p *filterParser) consume(expectedType string) (*filterToken, error) {
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("unexpected end of input, expected %s", expectedType)
	}
	token := p.tokens[p.pos]
	if expectedType != "" && token.Type != expectedType {
		return nil, fmt.Errorf("expected %s, got %s", expectedType, token.Type)
	}
	p.pos++
	return &token, nil
}

func (p *filterParser) parse() (*FilterAST, error) {
	return p.parseOrExpression()
}

func (p *filterParser) parseOrExpression() (*FilterAST, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == "LOGICAL_OP" && p.peek().Value == "OR" {
		p.consume("LOGICAL_OP")
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		left = &FilterAST{
			Type:      "logical",
			LogicalOp: "OR",
			Left:      left,
			Right:     right,
		}
	}

	return left, nil
}

func (p *filterParser) parseAndExpression() (*FilterAST, error) {
	left, err := p.parseNotExpression()
	if err != nil {
		return nil, err
	}

	for p.peek().Type == "LOGICAL_OP" && p.peek().Value == "AND" {
		p.consume("LOGICAL_OP")
		right, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}
		left = &FilterAST{
			Type:      "logical",
			LogicalOp: "AND",
			Left:      left,
			Right:     right,
		}
	}

	return left, nil
}

func (p *filterParser) parseNotExpression() (*FilterAST, error) {
	if p.peek().Type == "LOGICAL_OP" && p.peek().Value == "NOT" {
		p.consume("LOGICAL_OP")
		operand, err := p.parseNotExpression()
		if err != nil {
			return nil, err
		}
		return &FilterAST{
			Type:      "logical",
			LogicalOp: "NOT",
			Operand:   operand,
		}, nil
	}

	return p.parseComparison()
}

func (p *filterParser) parseComparison() (*FilterAST, error) {
	if p.peek().Type == "LPAREN" {
		p.consume("LPAREN")
		expr, err := p.parse()
		if err != nil {
			return nil, err
		}
		p.consume("RPAREN")
		return expr, nil
	}

	fieldToken, err := p.consume("FIELD")
	if err != nil {
		return nil, err
	}

	operatorToken, err := p.consume("OPERATOR")
	if err != nil {
		return nil, err
	}

	// Handle IN and NOT IN
	if operatorToken.Value == ":IN" || operatorToken.Value == ":NOT IN" {
		p.consume("LPAREN")
		var values []interface{}
		for {
			valueToken, err := p.consume("VALUE")
			if err != nil {
				return nil, err
			}
			values = append(values, parseValue(valueToken.Value))
			
			if p.peek().Type == "RPAREN" {
				break
			}
			p.consume("COMMA")
		}
		p.consume("RPAREN")
		
		operator := "IN"
		if operatorToken.Value == ":NOT IN" {
			operator = "NOT_IN"
		}
		
		return &FilterAST{
			Type:     "comparison",
			Field:    fieldToken.Value,
			Operator: operator,
			Value:    values,
		}, nil
	}

	// Regular operators
	valueToken, err := p.consume("VALUE")
	if err != nil {
		return nil, err
	}

	operator := mapOperator(operatorToken.Value)
	value := parseValue(valueToken.Value)

	return &FilterAST{
		Type:     "comparison",
		Field:    fieldToken.Value,
		Operator: operator,
		Value:    value,
	}, nil
}

func mapOperator(op string) string {
	switch op {
	case ":", "=":
		return "EQUALS"
	case ":>":
		return "GREATER_THAN"
	case ":<":
		return "LESS_THAN"
	case ":>=":
		return "GREATER_THAN_OR_EQUAL"
	case ":<=":
		return "LESS_THAN_OR_EQUAL"
	case ":!=":
		return "NOT_EQUALS"
	case ":LIKE":
		return "LIKE"
	case ":NOT LIKE":
		return "NOT_LIKE"
	default:
		return "EQUALS"
	}
}

func parseValue(valueStr string) interface{} {
	// Try to parse as number
	if num, err := strconv.ParseFloat(valueStr, 64); err == nil {
		return num
	}
	// Try to parse as boolean
	if valueStr == "true" || valueStr == "TRUE" {
		return true
	}
	if valueStr == "false" || valueStr == "FALSE" {
		return false
	}
	// Return as string
	return valueStr
}

func tokenizeFilterQuery(query string) []filterToken {
	var tokens []filterToken
	i := 0
	query = strings.TrimSpace(query)

	skipWhitespace := func() {
		for i < len(query) && (query[i] == ' ' || query[i] == '\t') {
			i++
		}
	}

	readField := func() string {
		start := i
		for i < len(query) && (isAlphanumeric(query[i]) || query[i] == '_' || query[i] == '.') {
			i++
		}
		return query[start:i]
	}

	readValue := func() (string, bool) {
		if i >= len(query) {
			return "", false
		}

		// Check for quoted string
		if query[i] == '"' || query[i] == '\'' {
			quote := query[i]
			i++
			start := i
			for i < len(query) && query[i] != quote {
				if query[i] == '\\' && i+1 < len(query) {
					i += 2
				} else {
					i++
				}
			}
			value := query[start:i]
			if i < len(query) {
				i++ // Skip closing quote
			}
			return value, true
		}

		// Check for number
		if isDigit(query[i]) || (query[i] == '-' && i+1 < len(query) && isDigit(query[i+1])) {
			start := i
			if query[i] == '-' {
				i++
			}
			for i < len(query) && (isDigit(query[i]) || query[i] == '.') {
				i++
			}
			return query[start:i], true
		}

		// Check for boolean
		if i+4 <= len(query) && strings.ToUpper(query[i:i+4]) == "TRUE" {
			i += 4
			return "true", true
		}
		if i+5 <= len(query) && strings.ToUpper(query[i:i+5]) == "FALSE" {
			i += 5
			return "false", true
		}

		// Read unquoted string
		start := i
		for i < len(query) && query[i] != ' ' && query[i] != '\t' &&
			query[i] != ':' && query[i] != '(' && query[i] != ')' && query[i] != ',' {
			i++
		}
		if i > start {
			return query[start:i], true
		}

		return "", false
	}

	readOperator := func() string {
		remaining := query[i:]
		if strings.HasPrefix(remaining, ":NOT IN") {
			i += 7
			return ":NOT IN"
		}
		if strings.HasPrefix(remaining, ":NOT LIKE") {
			i += 9
			return ":NOT LIKE"
		}
		if strings.HasPrefix(remaining, ":>=") {
			i += 3
			return ":>="
		}
		if strings.HasPrefix(remaining, ":<=") {
			i += 3
			return ":<="
		}
		if strings.HasPrefix(remaining, ":!=") {
			i += 3
			return ":!="
		}
		if strings.HasPrefix(remaining, ":IN") {
			i += 3
			return ":IN"
		}
		if strings.HasPrefix(remaining, ":LIKE") {
			i += 5
			return ":LIKE"
		}
		if strings.HasPrefix(remaining, ":>") {
			i += 2
			return ":>"
		}
		if strings.HasPrefix(remaining, ":<") {
			i += 2
			return ":<"
		}
		if i < len(query) && query[i] == ':' {
			i++
			return ":"
		}
		if i < len(query) && query[i] == '=' {
			i++
			return "="
		}
		return ""
	}

	readLogicalOp := func() string {
		remaining := strings.ToUpper(query[i:])
		if strings.HasPrefix(remaining, "AND") && (len(remaining) == 3 || remaining[3] == ' ' || remaining[3] == '\t') {
			i += 3
			return "AND"
		}
		if strings.HasPrefix(remaining, "OR") && (len(remaining) == 2 || remaining[2] == ' ' || remaining[2] == '\t') {
			i += 2
			return "OR"
		}
		if strings.HasPrefix(remaining, "NOT") && (len(remaining) == 3 || remaining[3] == ' ' || remaining[3] == '\t') {
			i += 3
			return "NOT"
		}
		return ""
	}

	for i < len(query) {
		skipWhitespace()
		if i >= len(query) {
			break
		}

		char := query[i]

		// Parentheses
		if char == '(' {
			tokens = append(tokens, filterToken{Type: "LPAREN", Value: "("})
			i++
			continue
		}
		if char == ')' {
			tokens = append(tokens, filterToken{Type: "RPAREN", Value: ")"})
			i++
			continue
		}

		// Comma
		if char == ',' {
			tokens = append(tokens, filterToken{Type: "COMMA", Value: ","})
			i++
			continue
		}

		// Logical operators
		if op := readLogicalOp(); op != "" {
			tokens = append(tokens, filterToken{Type: "LOGICAL_OP", Value: op})
			continue
		}

		// Operators
		if op := readOperator(); op != "" {
			tokens = append(tokens, filterToken{Type: "OPERATOR", Value: op})
			continue
		}

		// Check if we're expecting a value (after an operator or in IN/NOT IN list)
		expectValue := false
		if len(tokens) > 0 {
			lastToken := tokens[len(tokens)-1]
			expectValue = lastToken.Type == "OPERATOR" || lastToken.Type == "COMMA"
		}

		// If we're expecting a value, try to read it first (before field)
		// This handles cases like "service:symfony-app" where "symfony-app" should be a value, not a field
		if expectValue {
			if value, ok := readValue(); ok {
				tokens = append(tokens, filterToken{Type: "VALUE", Value: value})
				continue
			}
		}

		// Field name
		if isAlpha(char) || char == '_' {
			field := readField()
			if field != "" {
				tokens = append(tokens, filterToken{Type: "FIELD", Value: field})
				continue
			}
		}

		// Value
		if value, ok := readValue(); ok {
			tokens = append(tokens, filterToken{Type: "VALUE", Value: value})
			continue
		}

		// Unknown character - skip
		i++
	}

	return tokens
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isAlphanumeric(c byte) bool {
	return isAlpha(c) || isDigit(c)
}

