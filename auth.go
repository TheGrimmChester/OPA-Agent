package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

var jwtSecret = []byte("change-this-secret-key-in-production")

type User struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"password_hash"`
	Role         string    `json:"role"`
	CreatedAt    time.Time `json:"created_at"`
	LastLogin    *time.Time `json:"last_login,omitempty"`
}

type Claims struct {
	Username string `json:"username"`
	Role     string `json:"role"`
	jwt.RegisteredClaims
}

type AuthHandler struct {
	queryClient *ClickHouseQuery
}

func NewAuthHandler(queryClient *ClickHouseQuery) *AuthHandler {
	return &AuthHandler{
		queryClient: queryClient,
	}
}

func (ah *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}

	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&creds); err != nil {
		http.Error(w, "invalid request", 400)
		return
	}

	// Get user from database
	query := fmt.Sprintf("SELECT id, username, email, password_hash, role FROM opa.users WHERE username = '%s'",
		escapeSQL(creds.Username))
	rows, err := ah.queryClient.Query(query)
	if err != nil {
		LogError(err, "Failed to query user for login", map[string]interface{}{
			"username": creds.Username,
		})
		http.Error(w, "invalid credentials", 401)
		return
	}
	if len(rows) == 0 {
		LogWarn("Login attempt with invalid username", map[string]interface{}{
			"username": creds.Username,
		})
		http.Error(w, "invalid credentials", 401)
		return
	}

	row := rows[0]
	passwordHash := getString(row, "password_hash")

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(creds.Password)); err != nil {
		LogWarn("Login attempt with invalid password", map[string]interface{}{
			"username": creds.Username,
		})
		http.Error(w, "invalid credentials", 401)
		return
	}

	// Generate JWT token
	expirationTime := time.Now().Add(24 * time.Hour)
	claims := &Claims{
		Username: creds.Username,
		Role:     getString(row, "role"),
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtSecret)
	if err != nil {
		LogError(err, "Failed to generate JWT token", map[string]interface{}{
			"username": creds.Username,
		})
		http.Error(w, "failed to generate token", 500)
		return
	}

	// Update last login
	updateQuery := fmt.Sprintf("ALTER TABLE opa.users UPDATE last_login = now() WHERE username = '%s'",
		escapeSQL(creds.Username))
	ah.queryClient.Execute(updateQuery)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"token":    tokenString,
		"username": creds.Username,
		"role":     getString(row, "role"),
		"expires":  expirationTime.Unix(),
	})
}

func (ah *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}

	var user struct {
		Username string `json:"username"`
		Email    string `json:"email"`
		Password string `json:"password"`
		Role     string `json:"role"`
	}

	if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
		http.Error(w, "invalid request", 400)
		return
	}

	// Hash password
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		LogError(err, "Failed to hash password during registration", map[string]interface{}{
			"username": user.Username,
		})
		http.Error(w, "failed to hash password", 500)
		return
	}

	// Generate user ID
	userID := generateID()

	// Default role to viewer if not specified
	if user.Role == "" {
		user.Role = "viewer"
	}

	// Insert user
	query := fmt.Sprintf(`INSERT INTO opa.users (id, username, email, password_hash, role) 
		VALUES ('%s', '%s', '%s', '%s', '%s')`,
		escapeSQL(userID),
		escapeSQL(user.Username),
		escapeSQL(user.Email),
		escapeSQL(string(hashedPassword)),
		escapeSQL(user.Role),
	)

	if err := ah.queryClient.Execute(query); err != nil {
		LogError(err, "Failed to create user", map[string]interface{}{
			"username": user.Username,
			"email":    user.Email,
		})
		http.Error(w, fmt.Sprintf("failed to create user: %v", err), 500)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":       userID,
		"username": user.Username,
		"email":    user.Email,
		"role":     user.Role,
	})
}

func (ah *AuthHandler) VerifyToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return jwtSecret, nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*Claims); ok && token.Valid {
		return claims, nil
	}

	return nil, fmt.Errorf("invalid token")
}

func AuthMiddleware(handler http.HandlerFunc, requiredRole string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "unauthorized", 401)
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			http.Error(w, "invalid authorization header", 401)
			return
		}

		authHandler := &AuthHandler{queryClient: queryClient}
		claims, err := authHandler.VerifyToken(parts[1])
		if err != nil {
			http.Error(w, "invalid token", 401)
			return
		}

		// Check role if required
		if requiredRole != "" {
			if !hasPermission(claims.Role, requiredRole) {
				http.Error(w, "forbidden", 403)
				return
			}
		}

		// Add user info to request context
		r.Header.Set("X-User-Username", claims.Username)
		r.Header.Set("X-User-Role", claims.Role)

		handler(w, r)
	}
}

func hasPermission(userRole, requiredRole string) bool {
	roleHierarchy := map[string]int{
		"viewer": 1,
		"editor": 2,
		"admin":  3,
	}

	userLevel := roleHierarchy[userRole]
	requiredLevel := roleHierarchy[requiredRole]

	return userLevel >= requiredLevel
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

// API Key management
type APIKey struct {
	KeyID        string    `json:"key_id"`
	OrganizationID string  `json:"organization_id"`
	ProjectID    string    `json:"project_id"`
	KeyHash      string    `json:"key_hash"`
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    *time.Time `json:"expires_at,omitempty"`
}

func (ah *AuthHandler) CreateAPIKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "method not allowed", 405)
		return
	}

	var req struct {
		OrganizationID string     `json:"organization_id"`
		ProjectID      string     `json:"project_id"`
		Name           string     `json:"name"`
		ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), 400)
		return
	}

	if req.OrganizationID == "" || req.ProjectID == "" {
		http.Error(w, "organization_id and project_id are required", 400)
		return
	}

	// Generate API key (format: {org_id}:{project_id}:{random_hash})
	keyBytes := make([]byte, 32)
	rand.Read(keyBytes)
	keyHash := base64.URLEncoding.EncodeToString(keyBytes)
	
	// Create full key string
	fullKey := fmt.Sprintf("%s:%s:%s", req.OrganizationID, req.ProjectID, keyHash)
	
	// Hash the key for storage
	hashedKey, err := bcrypt.GenerateFromPassword([]byte(fullKey), bcrypt.DefaultCost)
	if err != nil {
		http.Error(w, "failed to hash key", 500)
		return
	}

	keyID := generateID()
	expiresAtStr := "NULL"
	if req.ExpiresAt != nil {
		expiresAtStr = fmt.Sprintf("'%s'", req.ExpiresAt.Format("2006-01-02 15:04:05"))
	}

	query := fmt.Sprintf(`INSERT INTO opa.api_keys (key_id, organization_id, project_id, key_hash, name, expires_at) 
		VALUES ('%s', '%s', '%s', '%s', '%s', %s)`,
		escapeSQL(keyID),
		escapeSQL(req.OrganizationID),
		escapeSQL(req.ProjectID),
		escapeSQL(string(hashedKey)),
		escapeSQL(req.Name),
		expiresAtStr)

	if err := ah.queryClient.Execute(query); err != nil {
		http.Error(w, fmt.Sprintf("failed to create API key: %v", err), 500)
		return
	}

	// Return the full key (only shown once)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"key_id":          keyID,
		"organization_id": req.OrganizationID,
		"project_id":      req.ProjectID,
		"name":            req.Name,
		"key":             fullKey, // Only returned on creation
		"created_at":      time.Now().Format("2006-01-02 15:04:05"),
		"expires_at":      req.ExpiresAt,
	})
}

func (ah *AuthHandler) ListAPIKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "method not allowed", 405)
		return
	}

	orgID := r.URL.Query().Get("organization_id")
	projID := r.URL.Query().Get("project_id")

	query := "SELECT key_id, organization_id, project_id, name, created_at, expires_at FROM opa.api_keys WHERE 1=1"
	if orgID != "" {
		query += fmt.Sprintf(" AND organization_id = '%s'", escapeSQL(orgID))
	}
	if projID != "" {
		query += fmt.Sprintf(" AND project_id = '%s'", escapeSQL(projID))
	}
	query += " ORDER BY created_at DESC"

	rows, err := ah.queryClient.Query(query)
	if err != nil {
		LogError(err, "Failed to query API keys", map[string]interface{}{
			"path": r.URL.Path,
		})
		http.Error(w, fmt.Sprintf("query error: %v", err), 500)
		return
	}

	var keys []map[string]interface{}
	for _, row := range rows {
		key := map[string]interface{}{
			"key_id":          getString(row, "key_id"),
			"organization_id": getString(row, "organization_id"),
			"project_id":      getString(row, "project_id"),
			"name":            getString(row, "name"),
			"created_at":      getString(row, "created_at"),
		}
		if expiresAt := getStringPtr(row, "expires_at"); expiresAt != nil {
			key["expires_at"] = *expiresAt
		}
		keys = append(keys, key)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{"api_keys": keys})
}

func (ah *AuthHandler) DeleteAPIKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != "DELETE" {
		http.Error(w, "method not allowed", 405)
		return
	}

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.Error(w, "bad request", 400)
		return
	}
	keyID := parts[3]

	query := fmt.Sprintf("ALTER TABLE opa.api_keys DELETE WHERE key_id = '%s'", escapeSQL(keyID))
	if err := ah.queryClient.Execute(query); err != nil {
		LogError(err, "Failed to delete API key", map[string]interface{}{
			"key_id": keyID,
		})
		http.Error(w, fmt.Sprintf("delete error: %v", err), 500)
		return
	}

	w.WriteHeader(204)
}

