package main

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type ConnectionServer struct {
	clients         map[int]*Client //подключенные клиенты
	register        chan *Client
	unregister      chan *Client
	routecast       chan RouteMessage
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
	WaitReg          int    `json:"wait_reg"`           //ожидание пакета регистрации, мс
	Timeout          int    `json:"timeout"`            //read/write timeout, мс
	LogMode          string `json:"logmode"`            //режим  логов
	LogDir           string `json:"logdir"`             //папка логов
	KeepAliveTimeout int    `json:"keep_alive_timeout"` //отключить молчащий объект спустя таймаут, мс
	Debug1           int    `json:"debug1"`
}

type AuthCredential struct {
	Id       int    `json:"id"`
	Rem      string `json:"rem"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type Client struct {
	Id        int
	ids       string //строка-id для лога
	Info      string //информация - опция50 от клиента
	Conn      net.Conn
	Writer    *bufio.Writer
	Reader    *bufio.Reader
	KaTimeout int
	Mutex     sync.Mutex
	// LastLive  time.Time // время последнего пакета от клиента
	// logger   *log.Logger
}

type RouteMessage struct {
	Dest int
	Data []byte
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
func NewConnectionServer(config *ServerConfig, authFile string) (*ConnectionServer, error) {

	var credentials []AuthCredential
	var err error
	if authFile != "" {
		credentials, err = loadAuthCredentials(authFile)
		if err != nil {
			return nil, err
		}
	}

	return &ConnectionServer{
		clients:         make(map[int]*Client),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		routecast:       make(chan RouteMessage),
		config:          config,
		credentials:     credentials,
		connectionCount: 0,
	}, nil
}
