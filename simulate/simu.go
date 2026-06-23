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
	CLIENTS    = 100 //всего клиентов
	CLIENTS_PU = 10  //каждый такой% клиент это ПУ
)

var TotalScore int //счетчик подключений-отключений тестовый
var ioCount int

func main() {
	fmt.Println("----- START Simulate Viking -----")
	// new clients
	tcc := []TCPClient{}

	// Загружаем конфигурацию
	config, err := LoadConfig("simuconfig.json")
	if err != nil {
		log.Fatal("Ошибка загрузки конфигурации:", err)
	}
	//клиенты из файла
	// for _, cli := range config.Clients {
	// client := NewTCPClient(cli, config)
	// 	tcc = append(tcc, *client)
	// }

	//клиенты созданы без файла
	config.Clients = []ClientConfig{} //очистить взятые из файла
	// idx := 0
	// for _, ipu := range CLIENTS_PU {
	// 	for _, ikp := range CLIENTS_KP {
	// 		sid := fmt.Sprintf("%04d", idx+1)
	// 		cli := ClientConfig{Id: idx + 1, Info: "Info" + sid, User: "User" + sid, Passw: "Passw" + sid}
	// 		config.Clients = append(config.Clients, cli)
	// 		client := NewTCPClient(idx, config)
	// 		tcc = append(tcc, *client)
	// 	}
	// }

	for i := 0; i < CLIENTS; i++ {
		sid := fmt.Sprintf("%04d", i+1)
		cli := ClientConfig{Id: i + 1, Info: "Info" + sid, User: "User" + sid, Passw: "Passw" + sid}
		if i%CLIENTS_PU == 0 {
			cli.Mode = "pu"
		}
		config.Clients = append(config.Clients, cli)
		client := NewTCPClient(i, config)
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
		time.Sleep(time.Millisecond * 100)
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
	// reader := bufio.NewReaderSize(conn, 65536)
	c.conn = conn
	c.Writer = writer
	c.Reader = reader
	c.state = 1
	c.say("Connected")

	//отправка запроса регистрации
	vf := NewVikingFrame(TSLUG, 0, 0, MID_QREG)
	vf.AddOption(OPT_INF, c.conf.Info)
	vf.AddOptionInt(OPT_PID, c.id)
	vf.AddOption(OPT_USER, c.conf.User)
	vf.AddOption(OPT_PASW, c.conf.Passw)
	vf.EndTx()
	err = Send(vf.txb, conn, writer, c.gconfig.Timeout)
	if err != nil {
		c.sayError("send", err)
		return
	}

	//прием ответа со статусом регистрации
	bb, err := ReadPac(conn, reader, c.gconfig.Timeout)
	if err != nil {
		c.sayError("read status", err)
		return
	}
	rxf := NewVikingFrameRx(bb)
	if rxf.msgid != MID_AREG {
		c.sayError("-> no status21", nil)
		return
	}
	opts := rxf.GetOptions()
	status := false
	if op, ok := opts[OPT_STAT]; ok == true {
		is := op.Body[0]
		if is == 4 || is == 3 || is == 1 {
			status = true
		}
	} else {
		c.sayError("-> no op55", nil)
		return
	}
	if !status {
		c.sayError("-> bad reg status", err)
		return
	}
	c.state = 2
	TotalScore++
	c.say(fmt.Sprintf("Authorized %v", TotalScore))

	c.startHeartbeat()
	// if c.conf.Mode == "pu" {
	// 	c.startHeartbeatPU()
	// } else {
	// 	c.startHeartbeatKP()
	// }

	c.Close()
	c.say("exit")
	TotalScore--
}

func (c *TCPClient) startHeartbeat() {
	c.say("Start " + c.conf.Mode)
	data := make([]byte, 10)
	dataChan := make(chan string, 10)
	go c.Receive(dataChan)

	dest := 20
	for {
		if c.conf.Mode == "pu" {
			fmt.Println("TotalScore=", TotalScore, "  dest=", dest)

			//запрос статуса КП
			stf := NewVikingFrame(TSLUG, 0, 0, MID_QSTAT)
			stf.AddOptionInt(OPT_PID, dest) //PointID
			stf.EndTx()
			if err := c.Send(stf.txb); err != nil {
				c.sayError("send req status", err)
				return
			}
			c.say(fmt.Sprintf("<- req status of %v ", dest))
			var staton bool
			for {
				msg := <-dataChan
				if msg == "astat_on" {
					staton = true
					break
				} else if msg == "astat_off" {
					break
				} else {
					c.say(msg + "------------------as")
				}
			}

			time.Sleep(time.Millisecond * 500)

			if staton {
				//отправка инф-пакета в КП и прием ответа
				inf := NewVikingFrameInf(dest, c.id, data)
				if err := c.Send(inf.txb); err != nil {
					c.sayError("send inf", err)
					return
				}
				c.say(fmt.Sprintf("<- INF to %v ", dest))
				for {
					msg := <-dataChan
					if msg == "inf" {
						break
					} else {
						c.say(msg + "-----------------inf")
					}
				}
			}

			dest++
			if dest > CLIENTS { //TotalScore {
				dest = 2
				// c.say("------------ dest")
			}
			time.Sleep(time.Millisecond * 500)

		} else {
			time.Sleep(time.Millisecond * 3000)

			//отправка пинга и прием понга
			pif := NewVikingFrame(TSLUG, 0, 0, MID_PING)
			pif.AddOptionInt(OPT_PID, c.id) //PointID
			pif.EndTx()
			if err := c.Send(pif.txb); err != nil {
				c.sayError("send ping", err)
				return
			}
			c.say("<- ping")
			for {
				msg := <-dataChan
				// fmt.Println(msg, " TotalScore=", TotalScore)
				if msg == "pong" {
					break
				}
			}

		}

	}
}

func (c *TCPClient) Receive(dataChan chan string) {
	pref := "receive"
	for {
		bb, err := ReadPac(c.conn, c.Reader, 0) //ждать без таймаута
		if err != nil {
			c.sayError(pref, err)
			return
		}
		rxf := NewVikingFrameRx(bb)

		switch rxf.tid {
		case TSLUG:
			switch rxf.msgid {
			case MID_ASTAT:
				var rxstat byte
				opts := rxf.GetOptions()
				if op, ok := opts[OPT_STAT]; ok != true {
					c.sayError(pref+"po55", nil)
				} else {
					rxstat = op.Body[0]
				}
				if rxstat == 4 {
					c.say("-> status on")
					dataChan <- "astat_on"
				} else {
					c.say("-> status off ===========================")
					dataChan <- "astat_off"
				}

			case MID_PONG:
				c.say("-> pong")
				dataChan <- "pong"

			default:
				c.sayError(fmt.Sprintf("-> bad msgid=0x%02X", rxf.msgid), nil)
			}
		case TINFO:
			if c.conf.Mode == "pu" {
				c.say(fmt.Sprintf("-> INF from %v ", rxf.srcadr))
				dataChan <- "inf"
			} else {
				c.say(fmt.Sprintf("=> INF from %v ", rxf.srcadr))
				// ответить на инф-пакет отправителю
				inf := NewVikingFrameInf(rxf.srcadr, c.id, rxf.body)
				if err := c.Send(inf.txb); err != nil {
					c.sayError("send inf", err)
					return
				}
				c.say(fmt.Sprintf("<= INF to %v ", rxf.srcadr))
			}

		case TSPOR:
			c.say("-> SPOR")

		default:
			c.sayError(fmt.Sprintf("~~> unknown tid=%v", rxf.tid), nil)
		}
	}
}

// ПУ в цикле всем КП: запрашивает статус - отправляет инф-пакет - ждет ответный инф-пакет
// func (c *TCPClient) startHeartbeatPU() {
// 	c.say("Start PU")
// 	dest := 2
// 	data := make([]byte, 1024)
// 	dataChan := make(chan string, 10) // буферный канал для данных
// 	go c.ReceivePu(dataChan)
// 	for {
// 		time.Sleep(time.Millisecond * 1000)

// 		//получение статуса КП
// 		stf := NewVikingFrame(TSLUG, 0, 0, MID_QSTAT)
// 		stf.AddOptionInt(OPT_PID, dest) //PointID
// 		stf.EndTx()
// 		if err := c.Send(stf.txb); err != nil {
// 			c.sayError("send req status", err)
// 			return
// 		}
// 		c.say(fmt.Sprintf("<- req status of %v ", dest))

// 		bb, err := ReadPac(c.conn, c.Reader, c.gconfig.Timeout)
// 		if err != nil {
// 			c.sayError("rx req status", err)
// 			return
// 		}
// 		vf := NewVikingFrameRx(bb)
// 		if vf.msgid == MID_ASTAT {
// 			c.say("-> status")
// 		} else {
// 			c.sayError(fmt.Sprintf("-> no status msgid=0x%02X", vf.msgid), nil)
// 		}

// 		//отправка инф-пакета в КП
// 		inf := NewVikingFrameInf(dest, c.id, data)
// 		if err := c.Send(inf.txb); err != nil {
// 			c.sayError("send inf", err)
// 			return
// 		}
// 		c.say(fmt.Sprintf("<= INF to %v ", dest))

// 		//прием инф-пакета - ответа от КП
// 		bb, err = ReadPac(c.conn, c.Reader, c.gconfig.Timeout)
// 		if err != nil {
// 			c.sayError("rx inf", err)
// 			return
// 		}
// 		rxinf := NewVikingFrameRx(bb)
// 		if rxinf.tid == TINFO {
// 			c.say(fmt.Sprintf("=> INF from %v ", rxinf.srcadr))
// 		} else {
// 			c.sayError(fmt.Sprintf("-> no inf=0x%02X", rxinf.tid), nil)
// 		}

// 	}
// }

// func (c *TCPClient) ReceivePu(dataChan chan string) {
// 	pref := "receivePu"
// 	for {
// 		bb, err := ReadPac(c.conn, c.Reader, 0) //ждать без таймаута
// 		if err != nil {
// 			c.sayError(pref, err)
// 			return
// 		}
// 		rxf := NewVikingFrameRx(bb)
// 		switch rxf.tid {
// 		case TSLUG:
// 			switch rxf.msgid {
// 			case MID_ASTAT:
// 				c.say("-> status")
// 				dataChan <- "astat"
// 			default:
// 				c.sayError(fmt.Sprintf("-> bad msgid=0x%02X", rxf.msgid), nil)
// 			}
// 		case TINFO:
// 			c.say(fmt.Sprintf("=> INF from %v ", rxf.srcadr))
// 			dataChan <- "rx_inf"

// 		case TSPOR:
// 			c.say("-> SPOR")

// 		default:
// 			c.sayError(fmt.Sprintf("~~> unknown tid=%v", rxf.tid), nil)
// 		}
// 	}
// }

// КП в цикле диалог пинга
// func (c *TCPClient) startHeartbeatKP() {
// 	c.say("Start KP")
// 	dataChan := make(chan string, 10) // буферный канал для данных
// 	go c.ReceiveKp(dataChan)
// 	//пакет для пинга
// 	pif := NewVikingFrame(TSLUG, 0, 0, MID_PING)
// 	pif.AddOptionInt(OPT_PID, c.id) //PointID
// 	pif.EndTx()

// 	for {
// 		if err := c.Send(pif.txb); err != nil {
// 			c.sayError("send ping", err)
// 			return
// 		}
// 		c.say("<- ping")

// 		//ждать pong
// 		for {
// 			msg := <-dataChan
// 			fmt.Println(msg, " TotalScore=", TotalScore)
// 			if msg == "pong" {
// 				break
// 			}
// 		}
// 		time.Sleep(time.Millisecond * 10000)
// 	}
// }

// func (c *TCPClient) ReceiveKp(dataChan chan string) {
// 	pref := "receiveKp"
// 	for {
// 		bb, err := ReadPac(c.conn, c.Reader, 0) //ждать без таймаута
// 		if err != nil {
// 			c.sayError(pref, err)
// 			return
// 		}
// 		rxf := NewVikingFrameRx(bb)
// 		switch rxf.tid {
// 		case TSLUG:
// 			switch rxf.msgid {
// 			// case MID_ASTAT:
// 			// 	c.say("-> status")
// 			// 	dataChan <- "a_stat"
// 			case MID_PONG:
// 				c.say("-> pong")
// 				dataChan <- "pong"
// 			default:
// 				c.sayError(fmt.Sprintf("-> bad msgid=0x%02X", rxf.msgid), nil)
// 			}
// 		case TINFO:
// 			c.say(fmt.Sprintf("=> INF from %v ", rxf.srcadr))
// 			// dataChan <- "rx_inf"
// 			// ответить на инф-пакет отправителю
// 			inf := NewVikingFrameInf(rxf.srcadr, c.id, rxf.body)
// 			if err := c.Send(inf.txb); err != nil {
// 				c.sayError("send inf", err)
// 				return
// 			}
// 			c.say(fmt.Sprintf("<= INF to %v ", rxf.srcadr))

// 		case TSPOR:
// 			c.say("-> SPOR")

// 		default:
// 			c.sayError(fmt.Sprintf("~~> unknown tid=%v", rxf.tid), nil)
// 		}
// 	}
// }

// func (c *TCPClient) Receive(dataChan chan string) {
// 	pref := "receive"
// 	for {
// 		bb, err := ReadPac(c.conn, c.Reader, 0) //ждать без таймаута
// 		if err != nil {
// 			c.sayError(pref, err)
// 			return
// 		}
// 		rxf := NewVikingFrameRx(bb)
// 		switch rxf.tid {
// 		case TSLUG:
// 			switch rxf.msgid {
// 			case MID_ASTAT:
// 				c.say("-> status")
// 				dataChan <- "a_stat"
// 			case MID_PONG:
// 				c.say("-> pong")
// 			default:
// 				c.sayError(fmt.Sprintf("-> bad msgid=0x%02X", rxf.msgid), nil)
// 			}
// 		case TINFO:
// 			c.say(fmt.Sprintf("=> INF from %v ", rxf.srcadr))
// 			// dataChan <- "rx_inf"
// 			// ответить на инф-пакет отправителю
// 			inf := NewVikingFrameInf(rxf.srcadr, c.id, rxf.body)
// 			if err := c.Send(inf.txb); err != nil {
// 				c.sayError("send inf", err)
// 				return
// 			}
// 			c.say(fmt.Sprintf("<= INF to %v ", rxf.srcadr))

// 		case TSPOR:
// 			c.say("-> SPOR")

// 		default:
// 			c.sayError(fmt.Sprintf("~~> unknown tid=%v", rxf.tid), nil)
// 		}
// 	}
// }

// func (c *TCPClient) startHeartbeat(mode string) {
// 	c.say("startHeartbeat:" + mode)
// 	ticker := time.NewTicker(time.Second * 5) //time.Duration(c.gconfig.PingInterval))
// 	defer func() {
// 		ticker.Stop()
// 	}()
// 	// data := make([]byte, 10240)

// 	//пакет для пинга
// 	pif := NewVikingFrame(TSLUG, 0, 0, MID_PING)
// 	pif.AddOptionInt(OPT_PID, c.id) //PointID
// 	pif.EndTx()

// 	//пакет для запроса статуса себя же
// 	stf := NewVikingFrame(TSLUG, 0, 0, MID_QSTAT)
// 	stf.AddOptionInt(OPT_PID, c.id) //PointID
// 	stf.EndTx()

// 	destCount := 2
// 	for {
// 		select {
// 		case <-ticker.C:
// 			if c.state != 2 {
// 				continue
// 			}
// 			if mode == "pu" {
// 				//отправка запроса статуса
// 				stf := NewVikingFrame(TSLUG, 0, 0, MID_QSTAT)
// 				stf.AddOptionInt(OPT_PID, destCount) //PointID
// 				stf.EndTx()
// 				if err := c.Send(stf.txb); err != nil {
// 					c.sayError("send req status", err)
// 					return
// 				}
// 				c.say(fmt.Sprintf("<- req status of %v ", destCount))
// 			}
// 		}
// 		fmt.Println("")
// 	}

// 	//пакет для пинга
// 	// pif := NewVikingFrame(TSLUG, 0, 0, MID_PING)
// 	// pif.AddOptionInt(OPT_PID, c.id) //PointID
// 	// pif.EndTx()

// 	// for {
// 	// 	if err := c.Send(pif.txb); err != nil {
// 	// 		c.sayError("send ping", err)
// 	// 		return
// 	// 	}
// 	// 	c.say("<- ping")

// 	// 	time.Sleep(time.Millisecond * 30000)
// 	// }
// }

/*





func (c *TCPClient) startHeartbeat2() {
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
	pif := NewVikingFrame(TSLUG, 0, 0, MID_PING)
	pif.AddOptionInt(OPT_PID, c.id) //PointID
	pif.EndTx()

	//пакет для запроса статуса себя же
	stf := NewVikingFrame(TSLUG, 0, 0, MID_QSTAT)
	stf.AddOptionInt(OPT_PID, c.id) //PointID
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
*/
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
