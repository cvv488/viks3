package main

import (
	"bufio"
	"log"
	"net"
	"sync"
	"time"
)

// Структура конфигурации сервера
type ServerConfig struct {
	Port             string `json:"port"`
	MaxConnections   int    `json:"max_connections"`
	LogFile          string `json:"log_file"`
	KeepAlivePeriod  int    `json:"keep_alive_period"`
	KeepAliveTimeout int    `json:"keep_alive_timeout"`
}

// Структура учётных данных
type AuthCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Сервер соединений
type ConnectionServer struct {
	clients         map[string] *Client
	register        chan *Client
	unregister      chan *Client
	broadcast       chan Message
	config          *ServerConfig
	credentials     []AuthCredentials
	connectionCount int
	mutex           sync.RWMutex
	logger          *log.Logger
	keepAliveTicker *time.Ticker // тикер для периодических проверок
}

// Клиент с аутентификацией и keep‑alive
type Client struct {
	Idc      string
	Conn     net.Conn
	Writer   *bufio.Writer
	Reader   *bufio.Reader
	LastPing time.Time // время последнего успешного пинга
	Mutex    sync.Mutex
	logger   *log.Logger
}

// Создаёт новый сервер соединений с загрузкой конфигурации
func NewConnectionServer(configFile, authFile string) (*ConnectionServer, error) {
	config, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	credentials, err := loadAuthCredentials(authFile)
	if err != nil {
		return nil, err
	}

	logger := NewLogger(config.LogFile, "SRVR")

	return &ConnectionServer{
		clients:         make(map[string]*Client),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		broadcast:       make(chan Message), //[]byte),
		config:          config,
		credentials:     credentials,
		connectionCount: 0,
		logger:          logger,
	}, nil
}
