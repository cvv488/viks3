package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	APP_INFO = "Viking Server v1.7"
	PLINE    = "-----------------------------------"
)

func main() {
	msg := "===== START " + APP_INFO + " ====="
	defer say("===== STOP " + APP_INFO + " =====")

	//сначала загрузка конфигурации и настройка логера
	config, err := LoadConfig("config.json")
	if err != nil {
		log.Fatal(err)
	}
	logstr, cu, err := LogSetup(config.LogMode, config.LogDir)
	say(msg)
	if !cu {
		fmt.Println(msg)
	}
	if err != nil {
		log.Fatal(err)
		return
	}
	say(logstr)
	if !cu {
		fmt.Println(logstr)
	}

	// Загружаем учётные данные
	server, err := NewConnectionServer(config, "credentials.json")
	if err != nil {
		sayError("NewConnectionServer", err)
		return
	}

	server.Start()
	server.Stop()
	time.Sleep(2 * time.Second)
}

func (srv *ConnectionServer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv.ctx = ctx
	var wg sync.WaitGroup

	// CLI: читает строки через bufio.Reader / поодерживается история команд - стрелки вверх/вниз
	// не в wg: при выходе по ctx.Done() горутина не помешает завершению, даже если висит на ReadString
	go func() {
		fmt.Println("CLI started. Coommands:")
		fmt.Println("  ?, help: вывод текущей информации")
		fmt.Println("  l, list: вывод списка подключенных клиентов")
		fmt.Println("  exit, Ctrl+C: выход")
		reader := bufio.NewReader(os.Stdin)
		startTime := time.Now()
		for {
			// Сначала проверяем контекст, чтобы не делать блокирующий вызов, если уже пора выходить
			if ctx.Err() != nil {
				fmt.Println("ctx.Done() в stdin — выход")
				return
			}
			line, err := reader.ReadString('\n')
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Println("EOF — выход")
					return
				}
				fmt.Printf("read err: %v\n", err)
				continue
			}
			line = strings.TrimRight(line, "\r\n") // line содержит '\n' в конце, можно обрезать
			if line == "" {
				continue // пустой ввод (просто Enter)
			}

			fmt.Printf(">>:  %s\n", line)
			fmt.Println(PLINE)
			switch line {
			case "?", "help":
				fmt.Println("Help:")
				fmt.Println(APP_INFO)
				fmt.Printf("Start time: %v, runtime: %v\n", startTime.Format(time.RFC3339), time.Since(startTime).Truncate(time.Second))
				fmt.Println(PLINE)
			case "exit":
				cancel()
				return
			case "l", "list":
				fmt.Println(APP_INFO)
				fmt.Printf("Start time: %v, Now: %v, runtime: %v\n", startTime.Format(time.RFC3339), time.Now().Format(time.RFC3339), time.Since(startTime).Truncate(time.Second))
				fmt.Printf("List: Клиентов: %d Соединений: %d \n", len(srv.clients), srv.connectionCount)

				//список клиентов с сортировкой
				srv.mutex.RLock()
				clients := make([]*Client, 0, len(srv.clients))
				for _, c := range srv.clients {
					clients = append(clients, c)
				}
				srv.mutex.RUnlock()
				sort.Slice(clients, func(i, j int) bool {
					return clients[i].ids < clients[j].ids
				})
				for _, client := range clients {
					fmt.Printf("%s\n", client.Print())
				}

				fmt.Println(PLINE)

			case "r": //runtime info
				fmt.Println(PLINE)
				srv.mutex.RLock()
				n := len(srv.clients)
				srv.mutex.RUnlock()
				say(fmt.Sprintf("*************** [stat] goroutines=%d clients=%d conns=%d", runtime.NumGoroutine(), n, srv.connectionCount))
				// [stat] goroutines=9 clients=2 conns=3 - лишняя горутина от дубля без защиты в v1.6
				// [stat] goroutines=8 clients=2 conns=2 - в v1.7 защита от дубля сработала - все ок
				fmt.Println(PLINE)
			}
		}
	}()

	// подписка на SIGINT (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt) // signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		say("Получен Ctrl+C, остановка сервера...")
		cancel()
	}()
	// go func() {
	// 	for {
	// 		fmt.Print("* ")
	// 		time.Sleep(1 * time.Second)
	// 	}
	// }()

	// Запуск listener
	listener, err := net.Listen("tcp", ":"+srv.config.Port)
	if err != nil {
		sayError("Ошибка при запуске сервера", err)
		return
	}
	// srv.listener = listener
	defer func() {
		_ = listener.Close()
		// srv.listener = nil
	}()
	say("Сервер запущен на порту " + srv.config.Port)

	wg.Add(1)
	go func() {
		defer wg.Done()
		srv.handleEvents()
	}()

	// listener.Accept() блокирующий поэтому в отдельной горутине
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) { //Это нормальный стоп - listener закрыли
					return
				}
				sayError("Accept error", err)
				continue
			}

			// Проверяем лимит подключений
			srv.mutex.Lock()
			if srv.connectionCount >= srv.config.MaxConnections {
				srv.mutex.Unlock()
				conn.Close()
				sayW("Сервер перегружен, попробуйте позже. Новое подключение: " + conn.RemoteAddr().String())
				continue
			}
			srv.connectionCount++
			srv.mutex.Unlock()
			say("Новое подключение: " + conn.RemoteAddr().String())

			go srv.authenticateClient(conn)
		}
	}()

	<-ctx.Done()
	fmt.Println("Exit — закрываем listener")
	_ = listener.Close()
	<-acceptDone // ждём, пока listener.Accept() завершится и изза Close
	wg.Wait()
}

// Аутентификация клиента
func (srv *ConnectionServer) authenticateClient(conn net.Conn) {
	defer func() {
		srv.mutex.Lock()
		srv.connectionCount--
		srv.mutex.Unlock()
		conn.Close()
	}()

	reader := bufio.NewReaderSize(conn, 4096) // def buf 4k
	writer := bufio.NewWriterSize(conn, 4096) // def buf 4k
	//todo? выбрать компромис между количеством вызовов и общим потреблением памяти

	//при тестировании иногда выявлялся мусор в новом подключении - очистить
	clearBufferSafe(reader, 4096)

	//ждем аутентификацию с таймаутом
	bb, reto, err := ReadPac(conn, reader, srv.config.WaitReg)
	if err != nil {
		sayError("authenticateClient", err)
		return
	}
	if reto {
		sayW("выход, не дождался пакет регистрации")
		return
	}

	if srv.logBytes {
		say("~~> " + BufToHex(bb))
	}

	vf, err := NewVikingFrameRx(bb)
	if err != nil {
		sayError("authenticateClient", err)
		return
	}
	if vf.msgid != MID_QREG {
		sayW("отказано, это не пакет регистрации")
		return
	}
	opts := vf.GetOptions()
	//по полученному pointId найти его в списке разрешенных (в конфигурации)
	op, ok := opts[OPT_PID]
	if ok != true {
		sayW("отказано, в пакете регистрации нет pointId")
		return
	}
	pointId := IHL(op.Body)
	pids := fmt.Sprintf("%04X", pointId)
	var cre *AuthCredential
	if srv.config.Debug1 == 1 { //debug - пускать всех
	} else {
		for i, pid := range srv.credentials {
			if pid.Id == pointId { //есть в списке
				cre = &srv.credentials[i]
				break
			}
		}
		if cre == nil {
			sayW(pids + ": отказано, клиент отсутствует в списке")
			return
		}
		//проверить логин и пароль если есть
		if cre.Username != "" {
			if op, ok := opts[OPT_USER]; ok != true {
				sayW(pids + ": отказано, в пакете регистрации нет user")
				return
			} else {
				if cre.Username != string(op.Body) {
					sayW(pids + ": отказано, не верный user")
					return
				}
			}
		}
		if cre.Password != "" {
			if op, ok := opts[OPT_PASW]; ok != true {
				sayW(pids + ": отказано, в пакете регистрации нет password")
				return
			} else {
				if cre.Password != string(op.Body) {
					sayW(pids + ": отказано, не верный password")
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
	if srv.logBytes {
		say("<~~ " + BufToHex(txf.txb))
	}

	var spor []int
	if cre != nil { //при debug=1 cre=nil
		spor = cre.Spor
	}
	client := &Client{
		Id:        pointId,
		ids:       pids,
		Info:      info,
		Conn:      conn,
		Writer:    writer,
		Reader:    reader,
		KaTimeout: srv.config.KeepAliveTimeout,
		spor:      spor,
		RunTime:   time.Now(),
	}
	srv.register <- client
	client.handleClient(srv)
}

// с мьютексом для Writer
func (c *Client) Send(bb []byte, timeoutms int) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return Send(bb, c.Conn, c.Writer, timeoutms)
}

// Обрабатывает сообщения от конкретного клиента
func (c *Client) handleClient(srv *ConnectionServer) {
	pref := "handleClient: "
	defer func() {
		select {
		case srv.unregister <- c:
		case <-srv.ctx.Done():
		default:
			// если очередь переполнена — не блокируем горутину,
			// handleEvents удалит клиента по факту закрытия Conn
		}
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
		bb, reto, err := ReadPac(c.Conn, c.Reader, c.KaTimeout) //ждать с KeepAliveTimeout
		if err != nil {
			if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "EOF") {
				c.sayW(pref + "eof:disconnected") //это не ошибка
			} else {
				c.sayError(pref, err)
			}
			return
		}
		if reto {
			c.sayW(fmt.Sprintf("Close keep-alive timeout=%dms", c.KaTimeout)) //принудительное отключение молчащего клиента
			return
		}
		count++

		if srv.logBytes {
			c.say("--> " + BufToHex(bb))
		}

		vf, err := NewVikingFrameRx(bb)
		if err != nil {
			c.sayError(pref, err)
			continue
		}
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
				c.say(fmt.Sprintf("-> req_status of %04X", pointId))

				sf := NewVikingFrame(TSLUG, 0, 0, MID_ASTAT)
				sf.AddOptionInt(OPT_PID, pointId)   //PointID
				sf.AddOptionInt(OPT_NETID, pointId) //NetID

				srv.mutex.RLock()
				_, online := srv.clients[pointId]
				srv.mutex.RUnlock()
				astat := 4
				if !online {
					astat = 2 //отключен
				}

				sf.AddOptionByte(OPT_STAT, byte(astat))
				sf.EndTx()
				err = c.Send(sf.txb, srv.config.Timeout)
				if err != nil {
					c.sayError("send astat", err)
					return
				}
				if srv.logBytes {
					c.say("<-- " + BufToHex(sf.txb))
				}
				c.say(fmt.Sprintf("<- status of %04X is %v", pointId, astat))

			case MID_PING: //Запрос “Keep alive”
				c.say("-> ping")
				err = c.Send(pif.txb, srv.config.Timeout)
				if err != nil {
					c.sayError("send pong", err)
					return
				}
				if srv.logBytes {
					c.say("<~~ " + BufToHex(pif.txb))
				}
				c.say("<- pong")

			default:
				//отправить ответ на неподдерживаемый тип сообщения
				//Note: также касается и известных команд протокола которые не поддерживаются версией, например подписки
				sf := NewVikingFrame(TSLUG, 0, 0, MID_UNSU)
				sf.EndTx()
				err = c.Send(sf.txb, srv.config.Timeout)
				if err != nil {
					c.sayError("send unsupport", err)
					return
				}
				if srv.logBytes {
					c.say("<~~ " + BufToHex(sf.txb))
				}
				c.sayW(fmt.Sprintf("-> bad msgid=0x%02X <- unsupport", vf.msgid))
			}

		case TINFO:
			//информационный пакет отправить по destadr
			msg := fmt.Sprintf("inf to %04X", vf.destadr)
			c.say("=> " + msg)
			rm := RouteMessage{Dest: vf.destadr, Data: bb, LogMsg: msg} //информационный пакет отправляется по назначению без изменений
			srv.routecast <- rm

		case TSPOR:
			//спорадический пакет отправить по destadr если не 0
			msg := fmt.Sprintf("spor to %04X", vf.destadr)
			c.say("=> " + msg)
			firstDest := -1
			if vf.destadr != 0 {
				rm := RouteMessage{Dest: vf.destadr, Data: bb, LogMsg: msg}
				srv.routecast <- rm
				firstDest = vf.destadr
			}

			//и отправить по списку рассылки этого клиента (если есть, можно и в 0)
			for _, ds := range c.spor {
				if ds == firstDest {
					continue //не повторять туда же
				}
				msg = fmt.Sprintf("spor list to %04X", ds)
				srv.mutex.RLock()
				_, online := srv.clients[ds]
				srv.mutex.RUnlock()
				if online { //приемник зарегистрирован
					vf.SetTxb(bb, ds) //сформировать пакет с новым dest_adr
					rm := RouteMessage{Dest: ds, Data: vf.txb, LogMsg: msg}
					srv.routecast <- rm
				} else {
					c.sayW("Off dest: " + msg)
					//todo? отправить резервным клиентам
				}
			}

		default:
			c.sayW(fmt.Sprintf("~~> unknown tid=%v", vf.tid))
		}
	}
}

// Обрабатывает события регистрации/удаления клиентов и рассылку сообщений
func (srv *ConnectionServer) handleEvents() {
	for {
		select {
		case <-srv.ctx.Done():
			return

		case client := <-srv.register:
			srv.mutex.Lock()
			//удалить старый дубликат если есть
			if old, exists := srv.clients[client.Id]; exists && old != client {
				old.Conn.Close() // закроем старый сокет — его ReadPac вернёт ошибку, handleClient завершится
				sayW(fmt.Sprintf("%v: *** Вытеснен новым подключением", old.ids))
			}
			srv.clients[client.Id] = client
			srv.mutex.Unlock()
			say(fmt.Sprintf("Registered %v, total %d", client.ids, len(srv.clients)))

		case client := <-srv.unregister:
			srv.mutex.Lock()
			if cur, ok := srv.clients[client.Id]; ok && cur == client {
				//тут еще сравнивается указатель - удалит только себя, но новый клиент с тем же Id (дубликат) не удалится
				delete(srv.clients, client.Id)
			}
			srv.mutex.Unlock()
			client.Conn.Close()
			say(fmt.Sprintf("Unregistered %v, total %d", client.ids, len(srv.clients)))

		case message := <-srv.routecast:
			srv.mutex.RLock()
			cli, ok := srv.clients[message.Dest]
			srv.mutex.RUnlock()
			if !ok {
				sayW("Off dest: " + message.LogMsg)
				//todo отправить резервным клиентам
				continue
			}
			err := cli.Send(message.Data, srv.config.Timeout)
			if err != nil {
				cli.sayError("routecast:"+message.LogMsg, err)
				// Удаляем клиента напрямую, чтобы не блокировать handleEvents
				// на записи в srv.unregister (единственный читатель — он сам).
				srv.mutex.Lock()
				if cur, exist := srv.clients[cli.Id]; exist && cur == cli {
					delete(srv.clients, cli.Id)
				}
				srv.mutex.Unlock()
				cli.Conn.Close()
				say(fmt.Sprintf("Unregistered2 %v, total %d", cli.ids, len(srv.clients)))
				continue
			}
			if srv.logBytes {
				cli.say("<== " + BufToHex(message.Data))
			}
			cli.say("<= " + message.LogMsg)
		}
	}
}

// Останавливает сервер
func (srv *ConnectionServer) Stop() {
	// srv.keepAliveTicker.Stop()
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

	// значения по умолчанию для критичных полей
	if config.Port == "" {
		config.Port = "45000"
	}
	if config.MaxConnections <= 0 {
		config.MaxConnections = 10_000
	}
	if config.WaitReg <= 0 {
		config.WaitReg = 10_000
	}
	if config.Timeout <= 0 {
		config.Timeout = 10_000
	}
	if config.KeepAliveTimeout <= 0 {
		config.KeepAliveTimeout = 600_000 //10m
	}
	return &config, err
}

// Загружает учётные данные из файла (ai)
func loadAuthCredentials(filename string) ([]AuthCredential, error) {
	bb, err := ReadFileToBytesJson(filename)
	if err != nil {
		return nil, err
	}

	// file, err := os.Open(filename)
	// if err != nil {
	// 	return nil, fmt.Errorf("не удалось открыть файл аутентификации: %v", err)
	// }
	// defer file.Close()

	var credentials []AuthCredential
	err = json.Unmarshal(bb, &credentials)
	// decoder := json.NewDecoder(file)
	// err = decoder.Decode(&credentials)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга учётных данных: %v", err)
	}

	//заполнить int-поля из hex-полей
	for i := range credentials {
		if credentials[i].IdHex == "" {
			return nil, fmt.Errorf("пустой IdHex[%d]", i)
		}
		vv, err := HexToInt(credentials[i].IdHex)
		if err != nil {
			return nil, fmt.Errorf("некорректный IdHex '%s': %v", credentials[i].IdHex, err)
		}
		credentials[i].Id = vv

		for _, sporcpy := range credentials[i].SporHex {
			ss, err := HexToInt(sporcpy)
			if err != nil {
				return nil, err
			}
			credentials[i].Spor = append(credentials[i].Spor, ss)
		}
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

// go func() {
// 	defer wg.Done()
// 	reader := bufio.NewReader(os.Stdin)
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			fmt.Println("ctx.Done() в stdin")
// 			return
// 		// case <-time.After(500 * time.Millisecond):
// 		// 	continue // к регулярной проверке ctx при блокирующем ReadRune (в отличие от listener.Accept() в select блокируется)
// 		default:
// 			// line, _ := reader.ReadString('\n') тут не блокируется
// 			// fmt.Println(">>", line)

// 			r, _, err := reader.ReadRune()
// 			if err != nil {
// 				if errors.Is(err, io.EOF) {
// 					fmt.Println("EOF — выход")
// 					return
// 				}
// 				fmt.Printf("read err: %v\n", err)
// 				continue
// 			}
// 			if r == '\r' || r == '\n' {
// 				fmt.Println(">> Enter нажат!")
// 				continue
// 			}
// 			fmt.Printf("Символ: %c\n", r)
// 		}
// 	}
// }()
