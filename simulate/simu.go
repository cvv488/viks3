package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
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
func (c *TCPClient) Start() {
	conn, err := c.connectWithRetries(99, time.Second*10)
	if err != nil {
		c.sayError("connect", err)
		return
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
		c.sayError("send", err)
		return
	}

	//прием ответа со статусом регистрации
	bb, err := ReadPac(conn, reader, c.gconfig.Timeout)
	if err != nil {
		c.sayError("readp", err)
		return
	}
	rxf := NewVikingFrameRx(bb)
	if rxf.msgid != 0x21 {
		c.sayError("21", nil)
		return
	}
	opts := rxf.GetOptions()
	status := -1
	if op, ok := opts[0x55]; ok == true {
		is := IHL(op.Body)
		if is == 4 || is == 3 || is == 1 {
			status = is
		}
	} else {
		c.sayError("no op55", nil)
		return
	}
	if status < 0 {
		c.sayError("bad reg status", err)
		return
	}
	c.state = 2
	TotalScore++
	c.say("Authorized")

	// Запускаем периодическую отправку данных
	// go c.startHeartbeat()
	c.startHeartbeat2()

	// Запускаем прием данных
	// c.startReceiving()

	c.Close()
	c.say("exit")
	TotalScore--
}

// в цикле диалог отправки пинга и запроса статуса
func (c *TCPClient) startHeartbeat2() {
	//пакет для пинга
	pif := NewVikingFrame(TS_INFO, 0, 0, 0x28)
	pif.AddOptionInt(0x51, c.id) //PointID
	pif.EndTx()

	//пакет для запроса статуса себя же
	stf := NewVikingFrame(TS_INFO, 0, 0, 0x22)
	stf.AddOptionInt(0x51, c.id) //PointID
	stf.EndTx()

	for {
		time.Sleep(time.Second * 2)

		//tx-rx ping
		if err := c.Send(pif.txb); err != nil {
			c.sayError("send ping", err)
			return
		}
		c.say("<- ping")
		bb, err := ReadPac(c.conn, c.Reader, c.gconfig.Timeout)
		if err != nil {
			c.sayError("rx ping", err)
			return
		}
		vf := NewVikingFrameRx(bb)
		if vf.msgid == 0x29 {
			c.say("-> pong")
		} else {
			c.say(fmt.Sprintf("-> no pong! msgid=0x%02X", vf.msgid))
		}

		//tx-rx req status
		if err := c.Send(stf.txb); err != nil {
			c.sayError("send req status", err)
			return
		}
		c.say("<- req status")
		bb, err = ReadPac(c.conn, c.Reader, c.gconfig.Timeout)
		if err != nil {
			c.sayError("rx req status", err)
			return
		}
		vf = NewVikingFrameRx(bb)
		if vf.msgid == 0x23 {
			c.say("-> status")
		} else {
			c.say(fmt.Sprintf("-> no status! msgid=0x%02X", vf.msgid))
		}

	}
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

	//пакет для запроса статуса себя же
	stf := NewVikingFrame(TS_INFO, 0, 0, 0x22)
	stf.AddOptionInt(0x51, c.id) //PointID
	stf.EndTx()

	// destCount := 2
	stat := 0 //
	for {
		select {
		case <-tickerPing.C:
			if c.state != 2 {
				continue
			}
			//попеременно тправляется пинг или инф-сообщение
			if stat == 0 {
				stat = 1
				if err := c.Send(pif.txb); err != nil {
					c.sayError("sendPing", err)
					return
				}
				c.say("<- ping")
			} else {
				stat = 0
				if err := c.Send(stf.txb); err != nil {
					c.sayError("sendStatus", err)
					return
				}
				c.say("<- req status")

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

// запускает прием данных от сервера
// func (c *TCPClient) startReceiving() {
// 	c.say("startReceiving")
// 	count := 0
// 	for {
// 		bb, err := ReadPac(c.conn, c.Reader, 0) //ждать без таймаута
// 		if err != nil {
// 			c.sayError("rx", err)
// 			return
// 		}
// 		count++
// 		vf := NewVikingFrameRx(bb)
// 		c.say(fmt.Sprintf("%v -> msgid=0x%02X", count, vf.msgid))

// 		switch vf.msgid {
// 		// case 0x20, //запрос на регистрацию
// 		// 0x21: //ответ на запрос о регистрации

// 		// 0x22 – запрос статуса клиента
// 		// 0x23 – ответ на запрос статуса клиента
// 		// 0x24 – уведомление о подключении/отключении клиента
// 		case 0x28: //Запрос “Keep alive”
// 			err = Send(pif.txb, c.Conn, c.Writer, s.config.Timeout)
// 			if err != nil {
// 				c.sayError("send28", err)
// 				return
// 			}
// 			c.say("<- pong")

// 			// 0x29 – ответ на запрос “Keep alive”
// 			// 0x30 – подписка на уведомление о подключении/отключении клиента
// 			// 0x31 – ответ сервера на команду подписки
// 			// 0x32 – запрос статуса подписки
// 			// 0xFE – команда не поддерживается
// 		}

// 		// timeter := time.Now()
// 		// fmt.Println("rx", time.Since(timeter).Milliseconds())
// 		time.Sleep(time.Millisecond)
// 	}

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
// }
