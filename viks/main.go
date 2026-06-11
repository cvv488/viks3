package main

import (
	"bufio"
	"fmt"

	// "log"
	"net"
	"os"
	"os/signal"
	"time"
)

var timeter time.Time

const (
	APP_INFO = "Viking Server v1.3"
	SERVERID = "0000"
)

func main() {
	msg := "===== START " + APP_INFO + " ====="
	defer say("===== STOP " + APP_INFO + " =====")
	// fmt.Println(msg)
	LogSetup()
	say(msg)

	// Загружаем конфигурацию и учётные данные
	server, err := NewConnectionServer("config.json", "auth.json")
	if err != nil {
		sayError("Ошибка инициализации сервера:", err)
		return
	}

	go func() {
		// Обработка прерывания (Ctrl+C)
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)
		<-sigChan
		say("Получен сигнал завершения, останавливаем сервер...")
		server.Stop()
		// return ?
	}()

	server.Start()
}

func (s *ConnectionServer) Start() {
	listener, err := net.Listen("tcp", ":"+s.config.Port)
	if err != nil {
		sayError("Ошибка при запуске сервера:", err)
	}
	defer listener.Close()
	say("Сервер запущен на порту " + s.config.Port)

	// go s.handleEvents()

	for {
		conn, err := listener.Accept()
		if err != nil {
			sayError("Ошибка при принятии соединения:", err)
			continue
		}
		say("Новое подключение: " + conn.RemoteAddr().String())

		// Проверяем лимит подключений
		s.mutex.Lock()
		if s.connectionCount >= s.config.MaxConnections {
			s.mutex.Unlock()
			conn.Close()
			sayError1("Сервер перегружен. Попробуйте позже")
			continue
		}
		s.connectionCount++
		s.mutex.Unlock()

		go s.authenticateClient(conn)
	}
}

// Аутентифицирует клиента перед регистрацией
func (s *ConnectionServer) authenticateClient(conn net.Conn) {
	defer func() {
		s.mutex.Lock()
		s.connectionCount--
		s.mutex.Unlock()
		conn.Close()
	}()

	reader := bufio.NewReader(conn) //def buf 4k, writer := bufio.NewWriterSize(conn, 8192) // буфер 8 КБ
	writer := bufio.NewWriter(conn)
	if err := clearBufferSafe(reader, 4096); err != nil { // максимум 4 КБ мусора
		sayError1(err.Error())
		return
	}

	//ждем аутентификацию с таймаутом
	bb, err := ReadPac(conn, reader, s.config.WaitReg)
	if err != nil {
		sayError("не дождался пакет регистрации", err)
		return
	}
	vf := NewVikingFrameRx(bb)
	if vf.msgid != 0x20 {
		sayError1("это не пакет регистрации 0x20")
		return
	}
	opts := vf.GetOptions()
	//по полученному pointId найти его в списке разрешенных (в конфигурации)
	op, ok := opts[0x51]
	if ok != true {
		sayError1("в пакете регистрации отсутствует pointId")
		return
	}
	pointId := IHL(op.Body)
	pids := fmt.Sprintf("%04d", pointId)

	var cre *AuthCredential
	for i, pid := range s.credentials {
		if pid.Id == pointId { //есть в списке
			cre = &s.credentials[i]
			break
		}
	}
	if cre == nil {
		say(pids + ": отсутствует в списке конфигурации")
		return
	}
	//проверить логин и пароль если есть
	if cre.Username != "" {
		op, ok := opts[0x56]
		if ok != true {
			say(pids + ": нет юзера")
			return
		}
		if cre.Username != string(op.Body) {
			say(pids + ": не верный юзер")
			return
		}
	}
	if cre.Password != "" {
		op, ok := opts[0x57]
		if ok != true {
			say(pids + ": нет пароля")
			return
		}
		if cre.Password != string(op.Body) {
			say(pids + ": не верный пароль")
			return
		}
	}
	//todo7 если такой уже есть отключить оба!

	// Успешная аутентификация - ответить клиенту
	txf := NewVikingFrame(TS_INFO, pointId, 0, 0x21) //todo уточнить destAdr=pointId ?
	txf.AddOptionInt(0x52, pointId)                  //NetID
	txf.AddOptionInt(0x55, 4)                        //Статус
	txf.EndTx()
	err = Send(txf.txb, conn, writer, s.config.Timeout)
	if err != nil {
		sayError("asend", err)
		return
	}

	client := &Client{
		Idc:      pointId,
		ids:      pids,
		Conn:     conn,
		Writer:   writer,
		Reader:   reader,
		LastPing: time.Now(),
		// logger:   NewLogger(s.config.LogFile, fmt.Sprintf("%04d", pointId)) //у каждого клиента свой логер
	}
	// s.register <- client
	// say(ids + "успешно аутентифицирован")
	client.handleClient(s)
	// logger.Println("exit")
}

// Обрабатывает сообщения от конкретного клиента
func (c *Client) handleClient(s *ConnectionServer) {
	defer func() {
		s.unregister <- c
		c.say("exit")
	}()

	c.say("успешно аутентифицирован")

	//подготовить пакет ответа на пинг // В ответ сервер передаёт клиенту пакет подтверждения, содержащий следующие опции: PointID (0x51); NetID (0x52); Статус (0x55).
	pif := NewVikingFrame(TS_INFO, c.Idc, 0, 0x29)
	pif.AddOptionInt(0x51, c.Idc) //PointID
	pif.AddOptionInt(0x52, c.Idc) //NetID
	pif.AddOptionInt(0x55, 4)     //Статус
	pif.EndTx()

	count := 0
	for {
		bb, err := ReadPac(c.Conn, c.Reader, 0) //ждать без таймаута
		if err != nil {
			c.sayError("handleClient1", err)
			return
		}
		count++
		vf := NewVikingFrameRx(bb)
		c.say(fmt.Sprintf("%v -> msgid=0x%02X", count, vf.msgid))

		switch vf.msgid {
		// case 0x20, //запрос на регистрацию
		// 0x21: //ответ на запрос о регистрации

		// 0x22 – запрос статуса клиента
		// 0x23 – ответ на запрос статуса клиента
		// 0x24 – уведомление о подключении/отключении клиента
		case 0x28: //Запрос “Keep alive”
			err = Send(pif.txb, c.Conn, c.Writer, s.config.Timeout)
			if err != nil {
				c.sayError("send28", err)
				return
			}
			c.say("<- pong")

			// 0x29 – ответ на запрос “Keep alive”
			// 0x30 – подписка на уведомление о подключении/отключении клиента
			// 0x31 – ответ сервера на команду подписки
			// 0x32 – запрос статуса подписки
			// 0xFE – команда не поддерживается
		}

		// opts := rxf.GetOptions()
		// op, ok := opts[0x51]
		// if ok != true {
		// 	fmt.Print("no opt")
		// 	return
		// }
		// pointId := IHL(op.Body)

		// msg, err := readMsg(client.Reader)
		// if err != nil {
		// 	client.logger.Println(err)
		// 	return
		// }
		// switch msg.Type {
		// case "ping":
		// 	client.logger.Println("-> ping")
		// 	client.Mutex.Lock()
		// 	client.LastPing = time.Now()
		// 	client.Mutex.Unlock()

		// 	msg.Type = "pong"
		// 	err = sendMsg(&msg, client.Writer)
		// 	if err != nil {
		// 		client.logger.Println(err)
		// 		return
		// 	}

		// case "info":
		// client.logger.Println("-> info to",msg.Dest)
		//todo? проверить что сообщение самому себе - не нужно транслировать
		// time.Sleep(time.Second)
		// client.logger.Println("-> info *")
		// s.broadcast <- msg

		// default:
		// 	client.logger.Println("Получен:", msg.Type)
		// }
	}
}

// Обрабатывает события регистрации/удаления клиентов и рассылку сообщений
// func (s *ConnectionServer) handleEvents() {
// 	for {
// 		select {
// 		case client := <-s.register:
// 			s.mutex.Lock()
// 			s.clients[client.Idc] = client
// 			s.mutex.Unlock()
// 			///client.logger.Printf("Registered, links: %d", len(s.clients))

// 		case client := <-s.unregister:
// 			s.mutex.Lock()
// 			if _, ok := s.clients[client.Idc]; ok {
// 				delete(s.clients, client.Idc)
// 				client.Conn.Close()
// 			}
// 			s.mutex.Unlock()
// 			///client.logger.Printf("Unregistered, links: %d", len(s.clients))

// 		case message := <-s.broadcast:
// 			// find := false
// 			s.mutex.RLock()
// 			// fmt.Println(message.Dest)
// 			if cli, ok := s.clients[message.Dest]; ok == true {
// 				timeter = time.Now()
// 				err := sendMsg(&message, cli.Writer)
// 				fmt.Println(message.ClientID, "->", message.Dest, time.Since(timeter).Microseconds())
// 				if err != nil {
// 					// Если ошибка записи, помечаем клиента к удалению
// 					s.unregister <- cli
// 					cli.logger.Println(err)
// 				}
// 			} else {
// 				s.logger.Printf("Bad Dest %s", message.Dest)
// 			}

// 			// for client := range s.clients { //todo map[]
// 			// 	if message.Dest == client.Idc {
// 			// 		fmt.Println(time.Since(timeter).Microseconds())
// 			// 		err := sendMsg(&message, client.Writer)
// 			// 		if err != nil {
// 			// 			// Если ошибка записи, помечаем клиента к удалению
// 			// 			s.unregister <- client
// 			// 			client.logger.Println(err)
// 			// 		}
// 			// 		find = true
// 			// 		break
// 			// 	}
// 			// }
// 			s.mutex.RUnlock()
// 			// if !find {
// 			// 	s.logger.Printf("Bad Dest %s", message.Dest)
// 			// }
// 		}
// 	}
// }

// Останавливает сервер
func (s *ConnectionServer) Stop() {
	s.keepAliveTicker.Stop()
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Отключаем всех клиентов
	for _, value := range s.clients {
		value.Conn.Close()
	}
	s.clients = make(map[string]*Client)
}
