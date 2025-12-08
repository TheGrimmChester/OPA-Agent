package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

type TenantContext struct {
	OrganizationID string
	ProjectID      string
}

// Extract tenant context from request
// Supports:
// 1. DSN-based authentication: Authorization header with DSN
// 2. API key: Authorization header with API key format {org_id}:{project_id}:{key_hash}
// 3. Headers: X-Organization-ID and X-Project-ID
// 4. Query parameters: organization_id and project_id
func ExtractTenantContext(r *http.Request, queryClient *ClickHouseQuery) (*TenantContext, error) {
	ctx := &TenantContext{
		OrganizationID: "",
		ProjectID:      "",
	}

	// Try DSN-based authentication first
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		// Check if it's a DSN (starts with http:// or https://)
		if strings.HasPrefix(authHeader, "http://") || strings.HasPrefix(authHeader, "https://") {
			// Extract org/project from DSN by querying projects table
			dsn := strings.TrimPrefix(authHeader, "Bearer ")
			dsn = strings.TrimPrefix(dsn, "DSN ")
			query := fmt.Sprintf("SELECT org_id, project_id FROM opa.projects WHERE dsn = '%s' LIMIT 1",
				escapeSQL(dsn))
			rows, err := queryClient.Query(query)
			if err == nil && len(rows) > 0 {
				ctx.OrganizationID = getString(rows[0], "org_id")
				ctx.ProjectID = getString(rows[0], "project_id")
				return ctx, nil
			}
		}

		// Try API key format: {org_id}:{project_id}:{key_hash}
		parts := strings.Split(authHeader, " ")
		if len(parts) == 2 && (parts[0] == "Bearer" || parts[0] == "ApiKey") {
			keyParts := strings.Split(parts[1], ":")
			if len(keyParts) >= 2 {
				// Query API keys table to verify and get org/project
				keyHash := parts[1]
				if len(keyParts) >= 3 {
					keyHash = strings.Join(keyParts[2:], ":")
				}
				query := fmt.Sprintf("SELECT org_id, project_id FROM opa.api_keys WHERE key_hash = '%s' LIMIT 1",
					escapeSQL(keyHash))
				rows, err := queryClient.Query(query)
				if err == nil && len(rows) > 0 {
					ctx.OrganizationID = getString(rows[0], "org_id")
					ctx.ProjectID = getString(rows[0], "project_id")
					return ctx, nil
				}
				// Fallback: use first two parts as org/project if API key not found
				ctx.OrganizationID = keyParts[0]
				ctx.ProjectID = keyParts[1]
				return ctx, nil
			}
		}
	}

	// Try headers
	if orgID := r.Header.Get("X-Organization-ID"); orgID != "" && orgID != "all" {
		ctx.OrganizationID = orgID
	} else if orgID == "all" {
		// "all" means no filtering - return empty to indicate all tenants
		ctx.OrganizationID = ""
	}
	if projID := r.Header.Get("X-Project-ID"); projID != "" && projID != "all" {
		ctx.ProjectID = projID
	} else if projID == "all" {
		// "all" means no filtering - return empty to indicate all projects
		ctx.ProjectID = ""
	}

	// Try query parameters (only if headers didn't set it)
	if ctx.OrganizationID == "" || ctx.OrganizationID == "default-org" {
		if orgID := r.URL.Query().Get("organization_id"); orgID != "" && orgID != "all" {
			ctx.OrganizationID = orgID
		} else if orgID == "all" {
			ctx.OrganizationID = ""
		}
	}
	if ctx.ProjectID == "" || ctx.ProjectID == "default-project" {
		if projID := r.URL.Query().Get("project_id"); projID != "" && projID != "all" {
			ctx.ProjectID = projID
		} else if projID == "all" {
			ctx.ProjectID = ""
		}
	}

	return ctx, nil
}

// IsAllTenants returns true if the context represents "all tenants" (no filtering)
func (ctx *TenantContext) IsAllTenants() bool {
	return ctx.OrganizationID == "" || ctx.ProjectID == ""
}

// Add tenant context to request
func AddTenantContext(r *http.Request, ctx *TenantContext) {
	r.Header.Set("X-Organization-ID", ctx.OrganizationID)
	r.Header.Set("X-Project-ID", ctx.ProjectID)
}

// Middleware to extract and validate tenant context
func TenantMiddleware(handler http.HandlerFunc, queryClient *ClickHouseQuery) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, err := ExtractTenantContext(r, queryClient)
		if err != nil {
			http.Error(w, "invalid tenant context", 400)
			return
		}

		// Validate organization exists
		query := fmt.Sprintf("SELECT org_id FROM opa.organizations WHERE org_id = '%s' LIMIT 1",
			escapeSQL(ctx.OrganizationID))
		rows, err := queryClient.Query(query)
		if err != nil || len(rows) == 0 {
			http.Error(w, "organization not found", 404)
			return
		}

		// Validate project exists and belongs to organization
		query = fmt.Sprintf("SELECT project_id FROM opa.projects WHERE org_id = '%s' AND project_id = '%s' LIMIT 1",
			escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
		rows, err = queryClient.Query(query)
		if err != nil || len(rows) == 0 {
			http.Error(w, "project not found", 404)
			return
		}

		// Add context to request
		AddTenantContext(r, ctx)
		handler(w, r)
	}
}

// Generate DSN for a project
func GenerateDSN(orgID, projectID string) string {
	// Generate a unique DSN
	// Format: http://{hash}@agent:8080/{org_id}/{project_id}
	hash := base64.URLEncoding.EncodeToString([]byte(orgID + ":" + projectID))
	return fmt.Sprintf("http://%s@agent:8080/%s/%s", hash, orgID, projectID)
}

// AddTenantFilter adds organization_id and project_id WHERE clauses to a query
func AddTenantFilter(query string, ctx *TenantContext) string {
	// Check if query already has WHERE clause
	if strings.Contains(strings.ToUpper(query), "WHERE") {
		return query + fmt.Sprintf(" AND organization_id = '%s' AND project_id = '%s'",
			escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
	}
	return query + fmt.Sprintf(" WHERE organization_id = '%s' AND project_id = '%s'",
		escapeSQL(ctx.OrganizationID), escapeSQL(ctx.ProjectID))
}

