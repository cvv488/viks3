package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"time"
	// co "../common"
	// "gopkg.in/yaml.v3"
)

func main() {
	fmt.Println("Start Simulate Viking")
	// Загружаем конфигурацию
	config, err := LoadConfig("simuconfig.json")
	if err != nil {
		log.Fatal("Ошибка загрузки конфигурации:", err)
	}
	fmt.Println(config)

	// new clients
	tcc := []TCPClient{}
	for _, cli := range config.Clients {
		client := NewTCPClient(cli, config)
		tcc = append(tcc, *client)
	}

	// Обработчик сигналов для корректного завершения
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Получен сигнал завершения, закрываем соединение...")
		for _, cli := range tcc {
			cli.Close()
		}
		os.Exit(0)
	}()

	//run clients
	for _, cli := range tcc {
		go cli.Start()
	}

	select {} // Бесконечное ожидание
}
func (c *TCPClient) say(m string) {
	log.Println(c.id, m)
}
func (c *TCPClient) sayError(m string, e error) error {
	log.Println(c.id, "ERROR:", m, e.Error())
	return e
}

// Start запускает клиента
func (c *TCPClient) Start() error {
	conn, err := c.connectWithRetries(99, time.Second*10)
	if err != nil {
		return err
	}
	c.conn = conn
	c.say("Успешно подключились к серверу")

	//отправка аутентификации - ожидание подтверждения
	msg := Message{Type: "auth", ClientID: c.id, Data: c.passw}
	err = c.sendMsg(&msg)
	if err != nil {
		return c.sayError("", err)
	}
	msg, err = c.readMsg()
	if err != nil {
		return c.sayError("", err)
	}
	if msg.Type != "auth_ok" {
		return c.sayError("auth", err)
	}
	c.say("Authorized ok")

	// Запускаем периодическую отправку данных
	go c.startHeartbeat()

	// Запускаем прием данных
	c.startReceiving()

	c.Close()
	c.say("Closed")
	return nil
}

// startHeartbeat запускает периодическую отправку heartbeat-сообщений
func (c *TCPClient) startHeartbeat() {
	tickerPing := time.NewTicker(time.Duration(c.config.PingInterval) * time.Second)
	ticker2 := time.NewTicker(5 * time.Second) // каждые 30 секунд
	// ticker3 := time.NewTicker(1 * time.Minute)  // каждую минуту
	defer func() {
		tickerPing.Stop()
		ticker2.Stop()
		// ticker3.Stop()
	}()
	for {
		select {
		case <-tickerPing.C:
			if c.closed {
				return
			}
			msg := Message{Type: "ping"} // Data: map[string]interface{}{"uptime": time.Since(time.Now()).String(), Timestamp: time.Now(),}
			err := c.sendMsg(&msg)
			if err != nil {
				return
			}
			c.say("<- ping")

		case <-ticker2.C:
			if c.mode == "1" {
				msg := Message{Type: "info", ClientID: c.id, Dest: "0002"}
				err := c.sendMsg(&msg)
				if err != nil {
					return
				}
				c.say("<= info")
			}
			// case <-ticker3.C:
		}
	}
}

// startReceiving запускает прием данных от сервера
func (c *TCPClient) startReceiving() {
	buffer := make([]byte, 4096)
	for {
		n, err := c.conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				c.say("Соединение закрыто сервером")
			} else {
				c.sayError("Ошибка чтения данных", err)
			}
			c.Close()
			return
		}
		// Обрабатываем полученное сообщение
		message := string(buffer[:n])
		var msg Message
		if err := json.Unmarshal([]byte(message), &msg); err == nil {
			switch msg.Type {
			case "pong":
				c.say("-> pong")
			// case "command":
			// 	log.Printf("Получена команда: %v", msg.Data)
			// Здесь можно добавить обработку команд от сервера
			default:
				c.say("=> " + msg.Type)
			}
		}
	}
}
