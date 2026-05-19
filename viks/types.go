package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

// Структура конфигурации сервера
type ServerConfig struct {
	Port             string `json:"port"`
	MaxConnections   int    `json:"max_connections"`
	LogFile          string `json:"log_file"`
	KeepAlivePeriod  int    `json:"keep_alive_period"`  // период в секундах
	KeepAliveTimeout int    `json:"keep_alive_timeout"` // таймаут в секундах
}

// Структура учётных данных
type AuthCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Сервер соединений
type ConnectionServer struct {
	clients         map[*Client]bool
	register        chan *Client
	unregister      chan *Client
	broadcast       chan Message //[]byte
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
}

// shared
// Message — структура сообщения
type Message struct {
	Type      string `json:"type"`
	ClientID  string `json:"client_id"`
	Dest      string
	Data      interface{} `json:"data,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
}

// Создаёт новый сервер соединений с загрузкой конфигурации
func NewConnectionServer(configFile, authFile string) (*ConnectionServer, error) {
	config, err := loadConfig(configFile)
	if err != nil {
		return nil, err
	}

	credentials, err := loadAuthCredentials(authFile)
	if err != nil {
		return nil, err
	}

	// Настройка логирования
	var logOutput io.Writer
	if config.LogFile != "" {
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("Не удалось открыть файл логов: %v, используем stdout", err)
			logOutput = os.Stdout
		} else {
			logOutput = logFile
		}
	} else {
		logOutput = os.Stdout
	}
	logger := log.New(logOutput, "SERVER: ", log.Ldate|log.Ltime|log.Lshortfile)

	return &ConnectionServer{
		clients:         make(map[*Client]bool),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		broadcast:       make(chan Message), //[]byte),
		config:          config,
		credentials:     credentials,
		connectionCount: 0,
		logger:          logger,
	}, nil
}
