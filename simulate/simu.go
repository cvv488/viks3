package main

import (
	"bufio"
	"fmt"
	"log"

	// "math/rand"

	// "math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
	// viks "viks"
)

const (
	RUNMODE = 1 //1-0001 посылает всем остальным, 2-все посылают всем рандомно, 3-все посылают в один
	CLIENTS = 1 // [0001...1000]
)

var TotalScore int //счетчик подключений-отключений тестовый
var ioCount int

func main() {
	fmt.Println("----- START Simulate Viking -----")
	// Загружаем конфигурацию
	config, err := LoadConfig("simuconfig.json")
	if err != nil {
		log.Fatal("Ошибка загрузки конфигурации:", err)
	}
	// fmt.Println(config)

	// new clients
	tcc := []TCPClient{}
	//клиенты из файла
	// for _, cli := range config.Clients {
	// client := NewTCPClient(cli, config)
	// 	tcc = append(tcc, *client)
	// }
	//клиенты new
	// if RUNMODE == 1 {
	for i := 0; i < CLIENTS; i++ {
		// sid := fmt.Sprintf("%04d", i)
		// cli := ClientConfig{Id: i, Passw: sid + "p"}
		// if i == 1 {
		// 	cli.Mode = "1"
		// }
		client := NewTCPClient(i, config)
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
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)
	c.conn = conn
	c.Writer = writer
	c.Reader = reader
	c.state = 1
	c.say("Connected")

	//отправка запроса регистрации
	vf := NewVikingFrame(TS_INFO, 0, 0, 0x20)
	vf.AddOption(0x50, c.conf.Info)
	vf.AddOptionInt(0x51, c.id)
	vf.AddOption(0x56, c.conf.User)
	vf.AddOption(0x57, c.conf.Passw)
	vf.EndTx()
	err = Send(vf.txb, conn, writer, c.gconfig.Timeout)
	if err != nil {
		fmt.Println("send")
		return err
	}

	//прием ответа со статусом регистрации
	bb, err := ReadPac(conn, reader, c.gconfig.Timeout)
	if err != nil {
		fmt.Println("readp")
		return err
	}
	rxf := NewVikingFrameRx(bb)
	if rxf.msgid != 0x21 {
		fmt.Println("21")
		return err
	}
	opts := rxf.GetOptions()
	status := -1
	if op, ok := opts[0x55]; ok == true {
		is := IHL(op.Body)
		if is == 4 || is == 3 || is == 1 {
			status = is
		}
	} else {
		fmt.Println("op55")
	}
	if status < 0 {
		return c.sayError("bad reg status", err)
	}
	c.state = 2
	TotalScore++
	c.say("Authorized")

	// Запускаем периодическую отправку данных
	go c.startHeartbeat()

	// Запускаем прием данных
	c.startReceiving()

	c.Close()
	c.say("exit")
	TotalScore--
	return nil
}

// startHeartbeat запускает периодическую отправку heartbeat-сообщений
func (c *TCPClient) startHeartbeat() {
	tickerPing := time.NewTicker(time.Duration(c.gconfig.PingInterval) * time.Second)
	tickerInfo := time.NewTicker(100 * time.Millisecond)
	// ticker3 := time.NewTicker(1 * time.Minute)  // каждую минуту
	// data := make([]byte, 10240)

	defer func() {
		tickerPing.Stop()
		tickerInfo.Stop()
		// ticker3.Stop()
	}()

	//пакет для пинга
	pif := NewVikingFrame(TS_INFO, 0, 0, 0x28)
	pif.AddOptionInt(0x51, c.id) //PointID
	pif.EndTx()

	// destCount := 2
	for {
		select {
		case <-tickerPing.C:
			if c.state == 2 {
				if err := c.Send(pif.txb); err != nil {
					c.sayError("sendPing", err)
					return
				}
				c.say("<- ping")
			}

		case <-tickerInfo.C:
			if TotalScore != CLIENTS { //еще не все подключены
				continue
			}
			// switch RUNMODE {
			// case 1: //0001 передает сообщения на все другие
			// 	if c.id == 1 { //"0001" {
			// 		msg := Message{Type: "info", ClientID: c.id, Dest: fmt.Sprintf("%04d", destCount), Data: data}
			// 		destCount++
			// 		if destCount > CLIENTS {
			// 			destCount = 2
			// 		}
			// 		timeter := time.Now()
			// 		if err := sendMsg(&msg, c.Writer); err != nil {
			// 			c.sayError("shb2", err)
			// 			return
			// 		}
			// 		fmt.Println()
			// 		s := fmt.Sprintf("<- info to %v c=%v tim=%v", msg.Dest, ioCount, time.Since(timeter).Milliseconds())
			// 		c.say(s) //"<- info to " + msg.Dest)
			// 		// c.say("<- info to " + msg.Dest)
			// 		ioCount++
			// 	}
			// case 2: //все всем рандомно
			// 	randomDest := rand.Intn(CLIENTS) + 1 //может и сам себе
			// 	msg := Message{Type: "info", ClientID: c.id, Dest: fmt.Sprintf("%04d", randomDest)}
			// 	if err := sendMsg(&msg, c.Writer); err != nil {
			// 		c.sayError("shb3", err)
			// 		return
			// 	}
			// 	c.say("<- info to rand " + msg.Dest)

			// case 3: //все посылают в один
			// 	if c.id != "0001" {
			// 		msg := Message{Type: "info", ClientID: c.id, Dest: "0001"}
			// 		if err := sendMsg(&msg, c.Writer); err != nil {
			// 			c.sayError("shb2", err)
			// 			return
			// 		}
			// 		s := fmt.Sprintf("<- info to %v c=%v", msg.Dest, ioCount)
			// 		c.say(s) //"<- info to " + msg.Dest)
			// 		ioCount++
			// 	}
			// }

			// case <-ticker3.C:
		}
	}
}

// startReceiving запускает прием данных от сервера
func (c *TCPClient) startReceiving() {
	for {
		// timeter := time.Now()
		// msg, err := readMsg(c.Reader)
		// fmt.Println("rx", time.Since(timeter).Milliseconds())
		// if err != nil {
		// 	c.sayError("rx", err)
		// 	return
		// }
		// ioCount--
		// switch msg.Type {
		// case "pong":
		// 	c.say("-> pong")
		// default:
		// 	c.say("=> " + msg.Type)
		// }
		time.Sleep(time.Second)
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
