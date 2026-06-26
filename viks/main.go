package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

var timeter time.Time

const (
	APP_INFO = "Viking Server v1.6"
)

func main() {
	msg := "===== START " + APP_INFO + " ====="
	defer say("===== STOP " + APP_INFO + " =====")

	//сначала настройка логера с загрузкой конфигурации
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Fatal(err)
	}
	LogSetup(config.Logs)
	say(msg)

	// Загружаем учётные данные
	server, err := NewConnectionServer(config, "auth.json")
	if err != nil {
		sayError("NewConnectionServer", err)
		return
	}

	go func() {
		// Обработка прерывания (Ctrl+C)
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)
		<-sigChan
		say("Получен сигнал завершения, останавливаем сервер...")
		server.Stop()
	}()

	server.Start()
}

func (srv *ConnectionServer) Start() {
	listener, err := net.Listen("tcp", ":"+srv.config.Port)
	if err != nil {
		sayError("Ошибка при запуске сервера", err)
	}
	defer listener.Close()
	say("Сервер запущен на порту " + srv.config.Port)

	go srv.handleEvents()

	for {
		conn, err := listener.Accept()
		if err != nil {
			sayError("Ошибка при принятии соединения", err)
			continue
		}
		say("Новое подключение: " + conn.RemoteAddr().String())

		// Проверяем лимит подключений
		srv.mutex.Lock()
		if srv.connectionCount >= srv.config.MaxConnections {
			srv.mutex.Unlock()
			conn.Close()
			say("Сервер перегружен. Попробуйте позже")
			continue
		}
		srv.connectionCount++
		srv.mutex.Unlock()

		go srv.authenticateClient(conn)
	}
}

// Аутентификация клиента
func (srv *ConnectionServer) authenticateClient(conn net.Conn) {
	defer func() {
		srv.mutex.Lock()
		srv.connectionCount--
		srv.mutex.Unlock()
		conn.Close()
	}()

	reader := bufio.NewReader(conn) //def buf 4k, writer := bufio.NewWriterSize(conn, 8192) // буфер 8 КБ
	writer := bufio.NewWriter(conn)
	//при тестировании иногда выявлялся мусор в новом подключении - очистить
	clearBufferSafe(reader, 4096)

	//ждем аутентификацию с таймаутом
	bb, err := ReadPac(conn, reader, srv.config.WaitReg)
	if err != nil {
		sayError("не дождался пакет регистрации", err)
		return
	}
	vf := NewVikingFrameRx(bb)
	if vf.msgid != MID_QREG {
		sayError1("это не пакет регистрации")
		return
	}
	opts := vf.GetOptions()
	//по полученному pointId найти его в списке разрешенных (в конфигурации)
	op, ok := opts[OPT_PID]
	if ok != true {
		sayError1("в пакете регистрации нет pointId")
		return
	}
	pointId := IHL(op.Body)
	pids := fmt.Sprintf("%04d", pointId)
	if srv.config.Debug1 == 1 { //debug - пускать всех
	} else {
		var cre *AuthCredential
		for i, pid := range srv.credentials {
			if pid.Id == pointId { //есть в списке
				cre = &srv.credentials[i]
				break
			}
		}
		if cre == nil {
			say(pids + ": запрещен")
			return
		}
		//проверить логин и пароль если есть
		if cre.Username != "" {
			if op, ok := opts[OPT_USER]; ok != true {
				say(pids + ": в пакете регистрации нет user")
				return
			} else {
				if cre.Username != string(op.Body) {
					say(pids + ": не верный user")
					return
				}
			}
		}
		if cre.Password != "" {
			if op, ok := opts[OPT_PASW]; ok != true {
				say(pids + ": в пакете регистрации нет password")
				return
			} else {
				if cre.Password != string(op.Body) {
					say(pids + ": не верный password")
					return
				}
			}
		}
	}
	info := ""
	if op, ok := opts[OPT_INF]; ok == true {
		info = string(op.Body)
	}
	//todo7 если такой уже есть отключить оба!

	// Успешная аутентификация - ответить клиенту
	txf := NewVikingFrame(TSLUG, 0, 0, MID_AREG)
	txf.AddOptionInt(OPT_NETID, pointId) //NetID
	txf.AddOptionInt(OPT_PID, pointId)   //и PointId на всякий случай
	txf.AddOptionByte(OPT_STAT, 4)       //Статус=аутентифицирован
	txf.EndTx()
	err = Send(txf.txb, conn, writer, srv.config.Timeout)
	if err != nil {
		sayError(pids+": asend", err)
		return
	}

	client := &Client{
		Id:       pointId,
		ids:      pids,
		Info:     info,
		Conn:     conn,
		Writer:   writer,
		Reader:   reader,
		LastPing: time.Now(),
	}
	srv.register <- client
	client.handleClient(srv)
}

// Обрабатывает сообщения от конкретного клиента
func (c *Client) handleClient(s *ConnectionServer) {
	pref := "handleClient: "
	defer func() {
		s.unregister <- c
		c.say("exit")
	}()
	c.say("успешно аутентифицирован")

	//подготовить пакет ответа на пинг (понг)
	pif := NewVikingFrame(TSLUG, 0, 0, MID_PONG)
	pif.AddOptionInt(OPT_PID, c.Id)   //PointID
	pif.AddOptionInt(OPT_NETID, c.Id) //NetID
	pif.AddOptionByte(OPT_STAT, 4)    //Статус
	pif.EndTx()

	count := 0
	for {
		bb, err := ReadPac(c.Conn, c.Reader, 0) //ждать без таймаута
		if err != nil {
			c.sayError(pref, err)
			return
		}
		count++
		vf := NewVikingFrameRx(bb)
		switch vf.tid {
		case TSLUG:
			switch vf.msgid {
			case MID_QSTAT: //запрос статуса клиента
				opts := vf.GetOptions()
				op, ok := opts[OPT_PID]
				if ok != true {
					c.sayError1(pref + "-> req_status no pointId")
					return
				}
				pointId := IHL(op.Body)
				c.say(fmt.Sprintf("-> req_status of %v", pointId))

				sf := NewVikingFrame(TSLUG, 0, 0, MID_ASTAT)
				sf.AddOptionInt(OPT_PID, pointId)   //PointID
				sf.AddOptionInt(OPT_NETID, pointId) //NetID
				astat := 4
				if _, ok := s.clients[pointId]; ok != true {
					astat = 2 //отключен
				}
				sf.AddOptionByte(OPT_STAT, byte(astat))
				sf.EndTx()
				err = Send(sf.txb, c.Conn, c.Writer, s.config.Timeout)
				if err != nil {
					c.sayError("send astat", err)
					return
				}
				c.say(fmt.Sprintf("<- status of %v is %v", pointId, astat))

			case MID_PING: //Запрос “Keep alive”
				c.say("-> ping")
				err = Send(pif.txb, c.Conn, c.Writer, s.config.Timeout)
				if err != nil {
					c.sayError("send pong", err)
					return
				}
				c.say("<- pong")

			default:
				//отправить ответ на неподдерживаемый тип сообщения
				//Note: также касается и известных команд протокола которые не поддерживаются версией, например подписки
				sf := NewVikingFrame(TSLUG, 0, 0, MID_UNSU)
				sf.EndTx()
				err = Send(sf.txb, c.Conn, c.Writer, s.config.Timeout)
				if err != nil {
					c.sayError("send unsupport", err)
					return
				}
				c.say("<- unsupport")
				c.sayError1(fmt.Sprintf("-> bad msgid=0x%02X", vf.msgid))
			}

		case TINFO:
			//информационный пакет перенаправить по назначению
			c.say(fmt.Sprintf("=> inf to %v", vf.destadr))
			rm := RouteMessage{Dest: vf.destadr, Data: bb}
			s.routecast <- rm

		case TSPOR:
			c.say(fmt.Sprintf("==> spor to %v", vf.destadr))

		default:
			c.sayError1(fmt.Sprintf("~~> unknown tid=%v", vf.tid))
		}
	}
}

// Обрабатывает события регистрации/удаления клиентов и рассылку сообщений
func (srv *ConnectionServer) handleEvents() {
	for {
		select {
		case client := <-srv.register:
			srv.mutex.Lock()
			srv.clients[client.Id] = client
			srv.mutex.Unlock()
			say(fmt.Sprintf("Registered %v, total %d", client.Id, len(srv.clients)))

		case client := <-srv.unregister:
			srv.mutex.Lock()
			if _, ok := srv.clients[client.Id]; ok {
				delete(srv.clients, client.Id)
				client.Conn.Close()
			}
			srv.mutex.Unlock()
			say(fmt.Sprintf("Unregistered %v, total %d", client.Id, len(srv.clients)))

		case message := <-srv.routecast:
			var cli *Client
			var ok bool
			srv.mutex.RLock()
			if cli, ok = srv.clients[message.Dest]; ok == true { //клиент Dest есть
			} else {
				say(fmt.Sprintf("No route: Bad Dest %v", message.Dest))
				//todo отправить резерным клиентам
			}
			srv.mutex.RUnlock()
			if ok {
				//восстановить поле LEN в отправку, crc должен совпасть
				lenb := BHL(len(message.Data) - 2)
				err := SendFirst(lenb, cli.Writer)
				if err != nil {
					cli.sayError("routecast", err)
					srv.unregister <- cli // Если ошибка записи, помечаем клиента к удалению
					return
				}

				err = Send(message.Data, cli.Conn, cli.Writer, srv.config.Timeout)
				if err != nil {
					cli.sayError("routecast", err)
					srv.unregister <- cli // Если ошибка записи, помечаем клиента к удалению
					return
				}
				cli.say("<- INF")
			}
		}
	}
}

// Останавливает сервер
func (srv *ConnectionServer) Stop() {
	srv.keepAliveTicker.Stop()
	srv.mutex.Lock()
	defer srv.mutex.Unlock()

	// Отключаем всех клиентов
	for _, value := range srv.clients {
		value.Conn.Close()
	}
	srv.clients = make(map[int]*Client) //clr
}

// Загружает конфигурацию из файла
func LoadConfig(fpath string) (*ServerConfig, error) {
	bb, err := ReadFileToBytesJson(fpath)
	if err != nil {
		return nil, err
	}
	var config ServerConfig
	err = json.Unmarshal(bb, &config)
	if err != nil {
		return nil, err
	}
	return &config, err
}

// Загружает учётные данные из файла
func loadAuthCredentials(filename string) ([]AuthCredential, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("не удалось открыть файл аутентификации: %v", err)
	}
	defer file.Close()

	var credentials []AuthCredential
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&credentials)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга учётных данных: %v", err)
	}

	return credentials, nil
}

func clearBufferSafe(reader *bufio.Reader, maxBytes int) error {
	if reader.Buffered() <= 0 {
		return nil
	}
	discarded := 0
	buf := make([]byte, 1024) // буфер для чтения
	for discarded < maxBytes {
		n, err := reader.Read(buf)
		discarded += n
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// Если прочитали меньше, чем в буфере — значит, данных больше нет
		if n < len(buf) {
			return nil
		}
	}
	return fmt.Errorf("clearBufferSafe: превышен лимит очистки, прочитано %d байт", discarded)
}
