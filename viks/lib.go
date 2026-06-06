package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

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
	return fmt.Errorf("превышен лимит очистки: прочитано %d байт", discarded)
}

// func (s *ConnectionServer) readMsg(reader *bufio.Reader) (Message, error) {
// 	bb, err := reader.ReadString('\n') //marshal убирает \n из json
// 	if err != nil {
// 		s.logger.Printf("reader")
// 		return Message{}, err
// 	}
// 	var msg Message
// 	err = json.Unmarshal([]byte(bb), &msg)
// 	if err != nil {
// 		s.logger.Printf("ошибка десериализации JSON: %v", err)
// 		return Message{}, err
// 	}
// 	// fmt.Println(msg)
// 	return msg, nil
// }

// func sendMsg(msg *Message, writer *bufio.Writer) error {
// 	pref := msg.ClientID + " sendMsg:"
// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		return fmt.Errorf("%v ошибка сериализации: %v", pref, err)
// 	}
// 	_, err = writer.Write(append(data, '\n'))
// 	if err != nil {
// 		return fmt.Errorf("%v ошибка отправки: %v", pref, err)
// 	}
// 	if err = writer.Flush(); err != nil {
// 		return fmt.Errorf("%v ошибка записи: %v", pref, err)
// 	}
// 	// log.Printf("%v Отправлен", pref)
// 	return nil
// }

// func (s *ConnectionServer) sendMsg(msg *Message, writer *bufio.Writer) error {
// 	pref := "sendMsg:"
// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		s.logger.Printf("%v ошибка сериализации: %v", pref, err)
// 		return err
// 	}
// 	_, err = writer.Write(append(data, '\n'))
// 	if err != nil {
// 		s.logger.Printf("%v ошибка отправки: %v", pref, err)
// 		return err
// 	}
// 	if err = writer.Flush(); err != nil {
// 		s.logger.Printf("%v ошибка записи: %v", pref, err)
// 	}
// 	// log.Printf("%v Отправлен", pref)
// 	return nil
// }

// Менеджер keep‑alive — периодически проверяет активность клиентов
// func (s *ConnectionServer) keepAliveManager() {
// 	for range s.keepAliveTicker.C {
// 		s.mutex.RLock()
// 		activeClients := make([]*Client, 0, len(s.clients))
// 		for client := range s.clients {
// 			activeClients = append(activeClients, client)
// 		}
// 		s.mutex.RUnlock()

// 		// Отправляем пинг каждому клиенту
// 		for _, client := range activeClients {
// 			client.Mutex.Lock()
// 			// Если клиент давно не отвечал на пинг — отключаем
// 			if time.Since(client.LastPing) > time.Duration(s.config.KeepAliveTimeout)*time.Second {
// 				s.logger.Printf("Клиент %s неактивен (последний пинг %v), отключаем",
// 					client.Username, client.LastPing)
// 				client.Mutex.Unlock()
// 				s.unregister <- client
// 				continue
// 			}

// 			// Отправляем пинг
// 			_, err := client.Writer.Write([]byte("PING\n"))
// 			if err != nil {
// 				client.Mutex.Unlock()
// 				s.unregister <- client
// 				continue
// 			}

// 			err = client.Writer.Flush()
// 			if err != nil {
// 				client.Mutex.Unlock()
// 				s.unregister <- client
// 				continue
// 			}
// 			client.Mutex.Unlock()
// 		}
// 	}
// }
