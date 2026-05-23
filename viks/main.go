package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

var timeter time.Time

func main() {
	fmt.Println("===== START Viking Server =====")
	// Загружаем конфигурацию и учётные данные
	server, err := NewConnectionServer("config.json", "auth.json")
	if err != nil {
		log.Fatal("Ошибка инициализации сервера:", err)
	}

	go func() {
		// Обработка прерывания (Ctrl+C)
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)
		<-sigChan
		server.logger.Println("Получен сигнал завершения, останавливаем сервер...")
		server.Stop()
	}()

	// Запускаем сервер
	server.Start()
}

// Запускает сервер
func (s *ConnectionServer) Start() {
	listener, err := net.Listen("tcp", ":"+s.config.Port)
	if err != nil {
		s.logger.Fatal("Ошибка при запуске сервера:", err)
	}
	defer listener.Close()
	s.logger.Printf("Сервер запущен на порту %s", s.config.Port)

	go s.handleEvents()

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.logger.Println("Ошибка при принятии соединения:", err)
			continue
		}
		// s.logger.Printf("Новое подключение: %s", conn.RemoteAddr().String())

		// Проверяем лимит подключений
		s.mutex.Lock()
		if s.connectionCount >= s.config.MaxConnections {
			s.mutex.Unlock()
			conn.Close()
			s.logger.Println("Сервер перегружен. Попробуйте позже")
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
	}()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn) //def buf 4k, writer := bufio.NewWriterSize(conn, 8192) // буфер 8 КБ

	if err := clearBufferSafe(reader, 4096); err != nil { // максимум 4 КБ мусора
		conn.Close()
		s.logger.Printf("Слишком много мусора в буфере")
		return
	}

	//ждем аутентификацию
	msg, err := readMsg(reader)
	if err != nil {
		s.logger.Printf("%v", err)
		return
	}

	// authenticated := false
	// if passw, ok := msg.Data.(string); ok {
	// 	// Проверяем учётные данные
	// 	for _, cred := range s.credentials {
	// 		if cred.Username == msg.ClientID && cred.Password == passw {
	// 			authenticated = true
	// 			break
	// 		}
	// 	}
	// } else {
	// 	s.logger.Printf("bad data passw")
	// 	return
	// }
	// if !authenticated {
	// 	conn.Close()
	// 	s.logger.Printf("Отклонено подключение от %s: неверные учётные данные", conn.RemoteAddr().String())
	// 	return
	// }
	// Успешная аутентификация - ответить
	msg = Message{Type: "auth_ok", ClientID: msg.ClientID}
	err = sendMsg(&msg, writer)
	if err != nil {
		s.logger.Println(err)
		return
	}

	logger := NewLogger(s.config.LogFile, msg.ClientID) //у каждого клиента

	client := &Client{
		Conn:     conn,
		Writer:   writer,
		Reader:   reader,
		Idc:      msg.ClientID,
		LastPing: time.Now(),
		logger:   logger,
	}
	s.register <- client
	// s.logger.Printf("Клиент %s успешно аутентифицирован", msg.ClientID)
	go s.handleClient(client)
}

// Обрабатывает сообщения от конкретного клиента
func (s *ConnectionServer) handleClient(client *Client) {
	defer func() {
		s.unregister <- client
	}()
	for {
		msg, err := readMsg(client.Reader)
		if err != nil {
			client.logger.Println(err)
			return
		}
		switch msg.Type {
		case "ping":
			client.logger.Println("-> ping")
			client.Mutex.Lock()
			client.LastPing = time.Now()
			client.Mutex.Unlock()

			msg.Type = "pong"
			err = sendMsg(&msg, client.Writer)
			if err != nil {
				client.logger.Println(err)
				return
			}

		case "info":
			// client.logger.Println("-> info")
			s.broadcast <- msg

		default:
			client.logger.Println("Получен:", msg.Type)
		}
	}
}

// Обрабатывает события регистрации/удаления клиентов и рассылку сообщений
func (s *ConnectionServer) handleEvents() {
	for {
		select {
		case client := <-s.register:
			s.mutex.Lock()
			s.clients[client.Idc] = client
			s.mutex.Unlock()
			///client.logger.Printf("Registered, links: %d", len(s.clients))

		case client := <-s.unregister:
			s.mutex.Lock()
			if _, ok := s.clients[client.Idc]; ok {
				delete(s.clients, client.Idc)
				client.Conn.Close()
			}
			s.mutex.Unlock()
			///client.logger.Printf("Unregistered, links: %d", len(s.clients))

		case message := <-s.broadcast:
			// find := false
			s.mutex.RLock()
			timeter = time.Now()
			// fmt.Println(message.Dest)
			if cli, ok := s.clients[message.Dest]; ok == true {
				fmt.Println(message.ClientID, "->", message.Dest, time.Since(timeter).Microseconds())
				err := sendMsg(&message, cli.Writer)
				if err != nil {
					// Если ошибка записи, помечаем клиента к удалению
					s.unregister <- cli
					cli.logger.Println(err)
				}
			} else {
				s.logger.Printf("Bad Dest %s", message.Dest)
			}

			// for client := range s.clients { //todo map[]
			// 	if message.Dest == client.Idc {
			// 		fmt.Println(time.Since(timeter).Microseconds())
			// 		err := sendMsg(&message, client.Writer)
			// 		if err != nil {
			// 			// Если ошибка записи, помечаем клиента к удалению
			// 			s.unregister <- client
			// 			client.logger.Println(err)
			// 		}
			// 		find = true
			// 		break
			// 	}
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
	s.clients = make(map[string]*Client)
}
