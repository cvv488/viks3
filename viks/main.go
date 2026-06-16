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
	APP_INFO = "Viking Server v1.4"
	// SERVERID = "0000"
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

	go s.handleEvents()

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
		if op, ok := opts[0x56]; ok != true {
			say(pids + ": нет юзера")
			return
		} else {
			if cre.Username != string(op.Body) {
				say(pids + ": не верный юзер")
				return
			}
		}
	}
	if cre.Password != "" {
		if op, ok := opts[0x57]; ok != true {
			say(pids + ": нет пароля")
			return
		} else {
			if cre.Password != string(op.Body) {
				say(pids + ": не верный пароль")
				return
			}
		}
	}
	//todo7 если такой уже есть отключить оба!

	// Успешная аутентификация - ответить клиенту
	txf := NewVikingFrame(TSLUG, 0, 0, 0x21)
	txf.AddOptionInt(0x52, pointId) //NetID
	txf.AddOptionInt(0x55, 4)       //Статус
	txf.EndTx()
	err = Send(txf.txb, conn, writer, s.config.Timeout)
	if err != nil {
		sayError("asend", err)
		return
	}

	client := &Client{
		Id:       pointId,
		ids:      pids,
		Conn:     conn,
		Writer:   writer,
		Reader:   reader,
		LastPing: time.Now(),
		// logger:   NewLogger(s.config.LogFile, fmt.Sprintf("%04d", pointId)) //у каждого клиента свой логер
	}
	s.register <- client
	client.handleClient(s)
}

// Обрабатывает сообщения от конкретного клиента
func (c *Client) handleClient(s *ConnectionServer) {
	pref := "handleClient: "
	defer func() {
		s.unregister <- c
		c.say("exit")
	}()
	c.say("успешно аутентифицирован")

	//подготовить пакет ответа на пинг
	pif := NewVikingFrame(TSLUG, c.Id, 0, 0x29)
	pif.AddOptionInt(0x51, c.Id) //PointID
	pif.AddOptionInt(0x52, c.Id) //NetID
	pif.AddOptionInt(0x55, 4)    //Статус
	pif.EndTx()

	count := 0
	for {
		bb, err := ReadPac(c.Conn, c.Reader, 0) //ждать без таймаута
		if err != nil {
			c.sayError(pref, err)
			continue //return
		}
		count++
		vf := NewVikingFrameRx(bb)
		switch vf.tid {
		case TSLUG:
			switch vf.msgid {
			case 0x22: //запрос статуса клиента
				opts := vf.GetOptions()
				op, ok := opts[0x51]
				if ok != true {
					sayError1(pref + "-> req_status no pointId")
					return
				}
				pointId := IHL(op.Body)
				c.say(fmt.Sprintf("-> req_status of %v", pointId))

				sf := NewVikingFrame(TSLUG, 0, 0, 0x23)
				sf.AddOptionInt(0x51, pointId) //PointID
				sf.AddOptionInt(0x52, pointId) //NetID
				mm := ""
				if _, ok := s.clients[pointId]; ok == true {
					sf.AddOptionInt(0x55, 4)
					mm = "on"
				} else {
					sf.AddOptionInt(0x55, 2) //отключен
					mm = "off"
				}
				sf.EndTx()
				err = Send(sf.txb, c.Conn, c.Writer, s.config.Timeout)
				if err != nil {
					c.sayError("send status", err)
					return
				}
				c.say(fmt.Sprintf("<- status of %v is %v", pointId, mm))

			case 0x28: //Запрос “Keep alive”
				c.say(fmt.Sprintf("%v -> ping ", count))
				err = Send(pif.txb, c.Conn, c.Writer, s.config.Timeout)
				if err != nil {
					c.sayError("send pong", err)
					return
				}
				c.say("<- pong")

			default:
				c.sayError1(fmt.Sprintf("-> bad msgid=0x%02X", vf.msgid))
				// 0x20 - запрос на регистрацию
				// 0x21 - ответ на запрос о регистрации
				// 0x23 – ответ на запрос статуса клиента
				// 0x24 – уведомление о подключении/отключении клиента
				// 0x29 – ответ на запрос “Keep alive”
				// 0x30 – подписка на уведомление о подключении/отключении клиента
				// 0x31 – ответ сервера на команду подписки
				// 0x32 – запрос статуса подписки
				// 0xFE – команда не поддерживается
			}
		case TINFO:
			c.say(fmt.Sprintf("=> inf to %v", vf.destadr))

		case TSPOR:
			c.say(fmt.Sprintf("==> spor to %v", vf.destadr))

		default:
			c.sayError1(fmt.Sprintf("~~> unknown tid=%v", vf.tid))
		}
	}
}

// Обрабатывает события регистрации/удаления клиентов и рассылку сообщений
func (s *ConnectionServer) handleEvents() {
	for {
		select {
		case client := <-s.register:
			s.mutex.Lock()
			s.clients[client.Id] = client
			s.mutex.Unlock()
			///client.logger.Printf("Registered, links: %d", len(s.clients))

		case client := <-s.unregister:
			s.mutex.Lock()
			if _, ok := s.clients[client.Id]; ok {
				delete(s.clients, client.Id)
				client.Conn.Close()
			}
			s.mutex.Unlock()
			///client.logger.Printf("Unregistered, links: %d", len(s.clients))

		case message := <-s.broadcast:
			// find := false
			s.mutex.RLock()
			// fmt.Println(message.Dest)
			if cli, ok := s.clients[message.Dest]; ok == true {
				// timeter = time.Now()
				// err := sendMsg(&message, cli.Writer)
				// fmt.Println(message.ClientID, "->", message.Dest, time.Since(timeter).Microseconds())
				// if err != nil {
				// Если ошибка записи, помечаем клиента к удалению
				s.unregister <- cli
				// cli.logger.Println(err)
			}
			// } else {
			// 	s.logger.Printf("Bad Dest %s", message.Dest)
			// }
			s.mutex.RUnlock()
			// if !find {
			// 	s.logger.Printf("Bad Dest %s", message.Dest)
			// }
		}
	}
}

// Останавливает сервер
func (s *ConnectionServer) Stop() {
	s.keepAliveTicker.Stop()
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Отключаем всех клиентов
	for _, value := range s.clients {
		value.Conn.Close()
	}
	s.clients = make(map[int]*Client) //clr
}
