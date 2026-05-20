package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

// Message — структура сообщения
type Message struct {
	Type      string `json:"type"`
	ClientID  string `json:"client_id"`
	Dest      string
	Data      any       `json:"data,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

func NewLogger(LogFile string, prefix string) *log.Logger {
	var logOutput io.Writer
	if LogFile != "" {
		logFile, err := os.OpenFile(LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("Не удалось открыть файл логов: %v, используем stdout", err)
			logOutput = os.Stdout
		} else {
			logOutput = logFile
		}
	} else {
		logOutput = os.Stdout
	}
	logger := log.New(logOutput, prefix + " ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix)
	return logger
}

// as json5: Поддерживаются однострочные и многострочные комментарии; Записи и списки могут иметь запятую после последнего элемента
func ReadFileToBytesJson(fpath string) ([]byte, error) {
	if fpath == "" {
		return nil, fmt.Errorf("ReadFileToBytes:no_fpath")
	}
	file, err := os.Open(fpath)
	if err != nil {
		return nil, fmt.Errorf("ReadFileToBytes: не найден файл %s", fpath)
	}
	defer file.Close()
	var data []byte
	comment := false
	scanner := bufio.NewScanner(file) //построчное чтение
	for scanner.Scan() {
		line := scanner.Text()
		if comment {
			if strings.Contains(line, "*/") {
				comment = false
			}
			continue
		}
		if strings.Contains(line, "/*") {
			comment = true
			continue
		}
		//функция удаления комментариев
		processedLine := func(line string) string {
			commentPos := strings.Index(line, "//")
			if commentPos != -1 {
				line = line[:commentPos]
			}
			line = strings.TrimSpace(line)
			return line
		}(line)

		if processedLine != "" {
			data = append(data, []byte(processedLine)...)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("ReadFileToBytes: ошибка чтения файла: %s | %v", fpath, err)
	}
	//уберет забытую ',' перед ] и }
	for i := 1; i < len(data); i++ {
		bb := data[i] //; fmt.Printf("%s", string(bb))
		if bb == byte(']') || bb == byte('}') {
			if data[i-1] == byte(',') {
				data[i-1] = byte(' ')
			}
		}
	}
	// fmt.Println(string(data))
	return data, nil
}

func readMsg(reader *bufio.Reader) (Message, error) {
	bb, err := reader.ReadString('\n') //marshal убрал \n из json
	if err != nil {
		return Message{}, fmt.Errorf("readMsg ReadString: %v", err)
	}
	var msg Message
	if err := json.Unmarshal([]byte(bb), &msg); err == nil {
		return msg, nil
	}
	return Message{}, fmt.Errorf("readMsg Unmarshal: %v", err)
}

func sendMsg(msg *Message, writer *bufio.Writer) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("sendMsg Marshal: %v", err)
	}
	_, err = writer.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("sendMsg Write: %v", err)
	}
	if err = writer.Flush(); err != nil {
		return fmt.Errorf("sendMsg Flush: %v", err)
	}
	// log.Printf("%v Отправлен", pref)
	return nil
}
