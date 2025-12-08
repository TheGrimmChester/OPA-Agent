package main

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins in development
	},
}

type WebSocketHub struct {
	clients    map[*websocket.Conn]*Client
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

type Client struct {
	conn     *websocket.Conn
	send     chan []byte
	channels []string // Subscribed channels
}

func NewWebSocketHub() *WebSocketHub {
	return &WebSocketHub{
		clients:    make(map[*websocket.Conn]*Client),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *WebSocketHub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.conn] = client
			h.mu.Unlock()
			LogInfo("WebSocket client connected", map[string]interface{}{
				"total_clients": len(h.clients),
			})

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client.conn]; ok {
				delete(h.clients, client.conn)
				close(client.send)
			}
			h.mu.Unlock()
			LogInfo("WebSocket client disconnected", map[string]interface{}{
				"total_clients": len(h.clients),
			})

		case message := <-h.broadcast:
			h.mu.RLock()
			for conn, client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, conn)
				}
			}
			h.mu.RUnlock()
		}
	}
}

func (h *WebSocketHub) Broadcast(channel string, data interface{}) {
	message := map[string]interface{}{
		"channel": channel,
		"data":    data,
		"timestamp": time.Now().Unix(),
	}
	jsonData, _ := json.Marshal(message)
	h.broadcast <- jsonData
}

func (c *Client) readPump() {
	defer func() {
		c.conn.Close()
	}()
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			LogError(err, "WebSocket read error", nil)
			break
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				LogError(err, "WebSocket write error", nil)
				return
			}
			w.Write(message)
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}
			if err := w.Close(); err != nil {
				LogError(err, "WebSocket writer close error", nil)
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				LogError(err, "WebSocket ping error", nil)
				return
			}
		}
	}
}

func handleWebSocket(hub *WebSocketHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			LogError(err, "WebSocket upgrade error", map[string]interface{}{
				"path": r.URL.Path,
				"remote_addr": r.RemoteAddr,
			})
			return
		}
		client := &Client{
			conn:     conn,
			send:     make(chan []byte, 256),
			channels: []string{"traces", "metrics", "errors"},
		}
		hub.register <- client

		go client.writePump()
		go client.readPump()
	}
}

