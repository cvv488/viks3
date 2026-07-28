package main

import (
	// 	"io"
	// 	"net"
	// 	"os"
	// 	"time"
	// 	// "gopkg.in/yaml.v3"
)

	// vf := VikingFrame{}
	// vf.AddOption(0x51, []byte{1, 0x96})
	// vf.AddOption(0x56, []byte{0x08, 0x70, 0x75, 0x31, 0x6B, 0x70, 0x31, 0x35, 0x30})
	// vf.AddOption(0x57, []byte{})
	// data := vf.GetBytes()
	// fmt.Println(data)



// // Config — структура конфигурации
// type Config struct {
// 	ServerAddress     string         `json:"server"`
// 	HeartbeatInterval int            `json:"heartbeat_interval"`
// 	Clients           []ClientConfig `json:"clients"`
// }
// type ClientConfig struct {
// 	Id string
// }

// // Message — структура сообщения
// type Message struct {
// 	Type      string      `json:"type"`
// 	ClientID  string      `json:"client_id"`
// 	Data      interface{} `json:"data,omitempty"`
// 	Timestamp time.Time   `json:"timestamp"`
// }

// // TCPClient — структура TCP-клиента
// type TCPClient struct {
// 	config *Config
// 	id     string
// 	conn   net.Conn
// 	closed bool
// }

// // NewTCPClient создает новый TCP-клиент
// func NewTCPClient(_id string, conf *Config) *TCPClient {
// 	return &TCPClient{
// 		config: conf,
// 		id:     _id,
// 		closed: false,
// 	}
// }

// // loadConfig загружает конфигурацию из файла
// // func loadConfig(filename string) (*Config, error) {
// // 	data, err := os.ReadFile(filename)
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	var config Config
// // 	err = json.Unmarshal(data, &config)
// // 	if err != nil {
// // 		return nil, err
// // 	}
// // 	return &config, nil
// // }

// // Start запускает клиента
// func (c *TCPClient) Start() error {

// 	for {
// 		conn, err := connectWithRetries(c.config.ServerAddress, 99, time.Second*10)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		c.conn = conn
// 		log.Println("Успешно подключились к серверу")

// 		time.Sleep(5 * time.Second)
// 		c.Close()
// 	}

// 	// Отправляем запрос на регистрацию
// 	// if err := c.sendClientID(); err != nil {
// 	// 	c.Close()
// 	// 	return err
// 	// }

// 	// // Ждем подтверждение подключения
// 	// if err := c.waitForConnectionAck(); err != nil {
// 	// 	c.Close()
// 	// 	return err
// 	// }

// 	// log.Println("Подключение подтверждено сервером")

// 	// // Запускаем периодическую отправку данных
// 	// go c.startHeartbeat()

// 	// // Запускаем прием данных
// 	// c.startReceiving()

// 	return nil
// }

// func connectWithRetries(address string, maxRetries int, timeout time.Duration) (net.Conn, error) {
// 	dialer := net.Dialer{
// 		Timeout: timeout,
// 	}
// 	for attempt := 1; attempt <= maxRetries; attempt++ {
// 		log.Printf("Попытка %d подключения к %s...", attempt, address)
// 		conn, err := dialer.Dial("tcp", address)
// 		if err == nil {
// 			log.Printf("Успешно подключились к %s с %d попытки", address, attempt)
// 			return conn, nil // Возвращаем успешное соединение
// 		}
// 		log.Printf("Попытка %d неудачна: %v", attempt, err)
// 		// Пауза между попытками (кроме последней)
// 		if attempt < maxRetries {
// 			time.Sleep(5 * time.Second)
// 		}
// 	}
// 	return nil, fmt.Errorf("не удалось подключиться к %s после %d попыток", address, maxRetries)
// }

// // sendClientID отправляет идентификатор клиента на сервер
// func (c *TCPClient) sendClientID() error {
// 	// vf := VikingFrame{}
// 	// vf.AddOption(0x51, []byte{1, 0x96})
// 	// vf.AddOption(0x56, []byte{0x08, 0x70, 0x75, 0x31, 0x6B, 0x70, 0x31, 0x35, 0x30})
// 	// vf.AddOption(0x57, []byte{})
// 	// data := vf.GetBytes()

// 	// msg := Message{
// 	// 	Type:      "client_id",
// 	// 	ClientID:  c.config.ClientID,
// 	// 	Timestamp: time.Now(),
// 	// }
// 	// data, err := json.Marshal(msg)
// 	// if err != nil {
// 	// 	return fmt.Errorf("ошибка сериализации идентификатора: %v", err)
// 	// }

// 	// _, err := c.conn.Write(data)
// 	// if err != nil {
// 	// 	return fmt.Errorf("ошибка отправки идентификатора: %v", err)
// 	// }
// 	// log.Printf("Отправлен идентификатор клиента: %s", c.config.)
// 	return nil
// }

// // waitForConnectionAck ждет подтверждение подключения от сервера
// func (c *TCPClient) waitForConnectionAck() error {
// 	buffer := make([]byte, 1024)
// 	n, err := c.conn.Read(buffer)
// 	if err != nil {
// 		return fmt.Errorf("ошибка чтения подтверждения: %v", err)
// 	}

// 	response := string(buffer[:n])
// 	log.Printf("Получено сообщение от сервера: %s", response)

// 	// Проверяем, что это подтверждение подключения
// 	if response != "CONNECTED\n" {
// 		return fmt.Errorf("неожиданный ответ сервера: %s", response)
// 	}

// 	return nil
// }

// // startHeartbeat запускает периодическую отправку heartbeat-сообщений
// func (c *TCPClient) startHeartbeat() {
// 	// ticker := time.NewTicker(time.Duration(c.config.HeartbeatInterval) * time.Second)
// 	// defer ticker.Stop()

// 	// for {
// 	// 	select {
// 	// 	case <-ticker.C:
// 	// 		if c.closed {
// 	// 			return
// 	// 		}

// 	// 		// Отправляем heartbeat-сообщение
// 	// 		msg := Message{
// 	// 			Type: "heartbeat",
// 	// 			// ClientID: c.config.ClientID,
// 	// 			Data: map[string]interface{}{
// 	// 				"uptime": time.Since(time.Now()).String(),
// 	// 			},
// 	// 			Timestamp: time.Now(),
// 	// 		}

// 	// 		data, err := json.Marshal(msg)
// 	// 		if err != nil {
// 	// 			log.Printf("Ошибка сериализации heartbeat: %v", err)
// 	// 			continue
// 	// 		}

// 	// 		_, err = c.conn.Write(append(data, '\n'))
// 	// 		if err != nil {
// 	// 			log.Printf("Ошибка отправки heartbeat: %v", err)
// 	// 			c.Close()
// 	// 			return
// 	// 		}

// 	// 		log.Printf("Отправлен heartbeat (интервал: %d сек)", c.config.HeartbeatInterval)

// 	// 	case <-time.After(5 * time.Minute):
// 	// 		// Периодически проверяем соединение
// 	// 		if _, err := c.conn.Write([]byte("PING\n")); err != nil {
// 	// 			log.Printf("Соединение потеряно: %v", err)
// 	// 			c.Close()
// 	// 			return
// 	// 		}
// 	// 	}
// 	// }
// }

// // startReceiving запускает прием данных от сервера
// func (c *TCPClient) startReceiving() {
// 	buffer := make([]byte, 4096)

// 	for {
// 		n, err := c.conn.Read(buffer)
// 		if err != nil {
// 			if err == io.EOF {
// 				log.Println("Соединение закрыто сервером")
// 				c.Close()
// 				return
// 			}
// 			if err != nil {
// 				log.Printf("Ошибка чтения данных: %v", err)
// 				c.Close()
// 				return
// 			}

// 			// Обрабатываем полученное сообщение
// 			message := string(buffer[:n])
// 			log.Printf("Получено сообщение от сервера: %s", message)

// 			// Разбираем JSON-сообщение, если это необходимо
// 			var msg Message
// 			if err := json.Unmarshal([]byte(message), &msg); err == nil {
// 				switch msg.Type {
// 				case "heartbeat_response":
// 					log.Println("Получен ответ на heartbeat")
// 				case "command":
// 					log.Printf("Получена команда: %v", msg.Data)
// 					// Здесь можно добавить обработку команд от сервера
// 				default:
// 					log.Printf("Неизвестный тип сообщения: %s", msg.Type)
// 				}
// 			} else {
// 				// Простое текстовое сообщение
// 				if message == "PING\n" {
// 					_, _ = c.conn.Write([]byte("PONG\n"))
// 					log.Println("Ответили PONG на PING")
// 				}
// 			}
// 		}
// 	}
// }

// // Close закрывает соединение
// func (c *TCPClient) Close() {
// 	if c.conn != nil {
// 		c.conn.Close()
// 	}
// 	c.closed = true
// 	log.Println("Соединение закрыто")
// }
