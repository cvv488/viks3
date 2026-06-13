package main

import (
	"bufio"
	// "log"
	"net"
	"sync"
	"time"
)

type ConnectionServer struct {
	clients    map[string]*Client
	register   chan *Client
	unregister chan *Client
	// broadcast       chan Message
	config          *ServerConfig
	credentials     []AuthCredential //todo to map?
	connectionCount int
	mutex           sync.RWMutex
	// logger          *log.Logger
	keepAliveTicker *time.Ticker // тикер для периодических проверок
}

type ServerConfig struct {
	Port             string `json:"port"`
	MaxConnections   int    `json:"max_connections"`
	WaitReg          int    `json:"wait_reg"` //ожидание пакета регистрации, мс
	Timeout          int    `json:"timeout"`  //read/write timeout, мс
	LogFile          string `json:"log_file"`
	KeepAlivePeriod  int    `json:"keep_alive_period"`
	KeepAliveTimeout int    `json:"keep_alive_timeout"`
}

// Структура учётных данных
type AuthCredential struct {
	Id       int    `json:"id"`
	Info     string `json:"info"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type Client struct {
	Idc      int
	ids      string //строка-id для лога
	Conn     net.Conn
	Writer   *bufio.Writer
	Reader   *bufio.Reader
	LastPing time.Time // время последнего успешного пинга
	Mutex    sync.Mutex
	// logger   *log.Logger
}

func (cli *Client) say(msg string) {
	say(cli.ids + ": " + msg)
}
func (cli *Client) sayError(msg string, err error) {
	sayError(cli.ids+": "+msg, err)
}
func (cli *Client) sayError1(msg string) {
	sayError1(cli.ids + ": " + msg)
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

	// logger := NewLogger(config.LogFile, "SRVR")

	return &ConnectionServer{
		clients:    make(map[string]*Client),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		// broadcast:       make(chan Message), //[]byte),
		config:          config,
		credentials:     credentials,
		connectionCount: 0,
		// logger:          logger,
	}, nil
}
