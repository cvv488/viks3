package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	RUNMODE = 1    //1-0001 посылает всем остальным, 2-все посылают всем рандомно, 3-все посылают в один
	CLIENTS = 1000 // [0001...1000]
)

var connects int
var ioCount int

func main() {
	fmt.Println("----- START Simulate Viking -----")
	// Загружаем конфигурацию
	config, err := LoadConfig("simuconfig.json")
	if err != nil {
		log.Fatal("Ошибка загрузки конфигурации:", err)
	}
	fmt.Println(config)

	// new clients
	tcc := []TCPClient{}
	//клиенты из файла
	// for _, cli := range config.Clients {
	// client := NewTCPClient(cli, config)
	// 	tcc = append(tcc, *client)
	// }
	//клиенты new
	// if RUNMODE == 1 {
	for i := 1; i <= CLIENTS; i++ {
		sid := fmt.Sprintf("%04d", i)
		cli := ClientConfig{Id: sid, Passw: sid + "p"}
		if i == 1 {
			cli.Mode = "1"
		}
		client := NewTCPClient(cli, config)
		tcc = append(tcc, *client)
	}
	// }

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
		time.Sleep(time.Millisecond * 10)
	}

	select {} // Бесконечное ожидание
}

// Start запускает клиента
func (c *TCPClient) Start() error {
	conn, err := c.connectWithRetries(99, time.Second*10)
	if err != nil {
		return err
	}
	c.conn = conn
	writer := bufio.NewWriter(conn)
	c.Writer = writer
	reader := bufio.NewReader(conn)
	c.Reader = reader
	c.state = 1
	c.say("Connected")

	//отправка аутентификации - ожидание подтверждения
	msg := Message{Type: "auth", ClientID: c.id, Data: c.passw}
	if err = sendMsg(&msg, writer); err != nil {
		return c.sayError("start", err)
	}
	if msg, err = readMsg(reader); err != nil {
		return c.sayError("start", err)
	}
	if msg.Type != "auth_ok" {
		return c.sayError("auth", err)
	}
	c.state = 2
	connects++
	c.say("Authorized")

	// Запускаем периодическую отправку данных
	go c.startHeartbeat()

	// Запускаем прием данных
	c.startReceiving()

	c.Close()
	c.say("exit")
	connects--
	return nil
}

// startHeartbeat запускает периодическую отправку heartbeat-сообщений
func (c *TCPClient) startHeartbeat() {
	tickerPing := time.NewTicker(time.Duration(c.config.PingInterval) * time.Second)
	tickerInfo := time.NewTicker(100 * time.Millisecond)
	// ticker3 := time.NewTicker(1 * time.Minute)  // каждую минуту
	data := make([]byte, 10240)
	defer func() {
		tickerPing.Stop()
		tickerInfo.Stop()
		// ticker3.Stop()
	}()
	destCount := 2
	for {
		select {
		case <-tickerPing.C:
			if c.state == 2 {
				msg := Message{Type: "ping"} // Data: map[string]interface{}{"uptime": time.Since(time.Now()).String(), Timestamp: time.Now(),}
				if err := sendMsg(&msg, c.Writer); err != nil {
					c.sayError("shb1", err)
					return
				}
				c.say("<- ping")
			}

		case <-tickerInfo.C:
			if connects != CLIENTS { //еще не все подключены
				continue
			}
			switch RUNMODE {
			case 1: //0001 передает сообщения на все другие
				if c.id == "0001" {
					msg := Message{Type: "info", ClientID: c.id, Dest: fmt.Sprintf("%04d", destCount), Data: data}
					destCount++
					if destCount > CLIENTS {
						destCount = 2
					}
					timeter := time.Now()
					if err := sendMsg(&msg, c.Writer); err != nil {
						c.sayError("shb2", err)
						return
					}
					fmt.Println()
					s := fmt.Sprintf("<- info to %v c=%v tim=%v", msg.Dest, ioCount, time.Since(timeter).Milliseconds())
					c.say(s) //"<- info to " + msg.Dest)
					// c.say("<- info to " + msg.Dest)
					ioCount++
				}
			case 2: //все всем рандомно
				randomDest := rand.Intn(CLIENTS) + 1 //может и сам себе
				msg := Message{Type: "info", ClientID: c.id, Dest: fmt.Sprintf("%04d", randomDest)}
				if err := sendMsg(&msg, c.Writer); err != nil {
					c.sayError("shb3", err)
					return
				}
				c.say("<- info to rand " + msg.Dest)

			case 3: //все посылают в один
				if c.id != "0001" {
					msg := Message{Type: "info", ClientID: c.id, Dest: "0001"}
					if err := sendMsg(&msg, c.Writer); err != nil {
						c.sayError("shb2", err)
						return
					}
					s := fmt.Sprintf("<- info to %v c=%v", msg.Dest, ioCount)
					c.say(s) //"<- info to " + msg.Dest)
					ioCount++
				}

			}

			// case <-ticker3.C:
		}
	}
}

// startReceiving запускает прием данных от сервера
func (c *TCPClient) startReceiving() {
	for {
		timeter := time.Now()
		msg, err := readMsg(c.Reader)
		fmt.Println("rx", time.Since(timeter).Milliseconds())
		if err != nil {
			c.sayError("rx", err)
			return
		}
		ioCount--
		switch msg.Type {
		case "pong":
			c.say("-> pong")
		default:
			c.say("=> " + msg.Type)
		}
		// time.Sleep(time.Second)
	}

	// buffer := make([]byte, 4096)
	// for {
	// 	n, err := c.conn.Read(buffer)
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			c.say("Соединение закрыто сервером")
	// 		} else {
	// 			c.sayError("Ошибка чтения данных", err)
	// 		}
	// 		c.Close()
	// 		return
	// 	}
	// 	// Обрабатываем полученное сообщение
	// 	message := string(buffer[:n])
	// 	var msg Message
	// 	if err := json.Unmarshal([]byte(message), &msg); err == nil {
	// 		switch msg.Type {
	// 		case "pong":
	// 			c.say("-> pong")
	// 		// case "command":
	// 		// 	log.Printf("Получена команда: %v", msg.Data)
	// 		// Здесь можно добавить обработку команд от сервера
	// 		default:
	// 			c.say("=> " + msg.Type)
	// 		}
	// 	}
	// }
}
