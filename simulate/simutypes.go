package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
	// co "viks"
)

// Config — структура конфигурации
type Config struct {
	ServerAddress string         `json:"server"`
	PingInterval  int            `json:"ping_interval"`
	Clients       []ClientConfig `json:"clients"`
}
type ClientConfig struct {
	Id    string
	Passw string
	Mode  string
}

// TCPClient — структура TCP-клиента
type TCPClient struct {
	config          *Config
	id, passw, mode string
	state           int //1-connected, 2-authorized, 9-closed
	conn            net.Conn
	Writer          *bufio.Writer
	Reader          *bufio.Reader
}

// NewTCPClient создает новый TCP-клиент
func NewTCPClient(cli ClientConfig, conf *Config) *TCPClient {
	return &TCPClient{
		config: conf,
		id:     cli.Id,
		passw:  cli.Passw,
		mode:   cli.Mode,
	}
}
func (c *TCPClient) say(m string) {
	log.Println(c.id, m)
}
func (c *TCPClient) sayError(m string, e error) error {
	log.Println(c.id, "ERROR:", m, e.Error())
	return e
}

func LoadConfig(fpath string) (*Config, error) {
	bb, err := ReadFileToBytesJson(fpath)
	if err != nil {
		return nil, err
	}
	var config Config
	err = json.Unmarshal(bb, &config)
	if err != nil {
		return nil, err
	}
	return &config, err
}

func (c *TCPClient) connectWithRetries(maxRetries int, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout: timeout,
	}
	address := c.config.ServerAddress
	for attempt := 1; attempt <= maxRetries; attempt++ {
		// c.say(fmt.Sprintf("Попытка %d подключения к %s...", attempt, address))
		conn, err := dialer.Dial("tcp", address)
		if err == nil {
			// c.say(fmt.Sprintf("Успешно подключились к %s с %d попытки", address, attempt))
			return conn, nil // Возвращаем успешное соединение
		}
		c.say(fmt.Sprintf("Попытка %d неудачна: %v", attempt, err))
		// Пауза между попытками (кроме последней)
		if attempt < maxRetries {
			time.Sleep(5 * time.Second)
		}
	}
	return nil, c.sayError("", fmt.Errorf("не удалось подключиться к %s после %d попыток", address, maxRetries))
}

// func (c *TCPClient) readCon() (buffer []byte, err error) {
// 	buffer = make([]byte, 1024)
// 	n, err := c.conn.Read(buffer)
// 	if err != nil {
// 		return nil, fmt.Errorf("ошибка чтения подтверждения: %v", err)
// 	}
// 	return buffer[:n], nil
// }

// func (c *TCPClient) readMsg() (Message, error) {
// 	reader := bufio.NewReader(c.conn)
// 	bb, err := reader.ReadString('\n') //marshal убирает \n из json
// 	if err != nil {
// 		return Message{}, fmt.Errorf("readMsg: %v", err)
// 	}
// 	var msg Message
// 	if err := json.Unmarshal([]byte(bb), &msg); err == nil {
// 		return msg, nil
// 	}
// 	return Message{}, fmt.Errorf("readMsg: %v", err)
// }

// sendClientID отправляет идентификатор клиента на сервер
// func (c *TCPClient) sendMsg(msg *Message) error {
// 	pref := "sendMsg:"
// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		return fmt.Errorf("%v ошибка сериализации: %v", pref, err)
// 	}
// 	_, err = c.conn.Write(append(data, '\n'))
// 	if err != nil {
// 		return fmt.Errorf("%v ошибка отправки: %v", pref, err)
// 	}
// 	// c.say(pref + " Отправлен")
// 	return nil
// }

// waitForConnectionAck ждет подтверждение подключения от сервера
// func (c *TCPClient) waitForConnectionAck() error {
// 	response, err := c.readCon()
// 	if err != nil {
// 		return err
// 	}
// 	// Проверяем, что это подтверждение подключения
// 	if strings.Contains(response, "AUTH_SUCCESS") {
// 		log.Printf("OK: %s", response)
// 		return nil
// 	}
// 	return fmt.Errorf("неожиданный ответ сервера: %s", response)
// }

// Close закрывает соединение
func (c *TCPClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
	c.state = 9
	c.say("Close")
}
