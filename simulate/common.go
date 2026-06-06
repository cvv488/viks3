package main

//VER 2
import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
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

// re []byteHL из Int16
func BHL(vv int) []byte {
	bb := make([]byte, 2)
	bb[0] = byte(vv >> 8)
	bb[1] = byte(vv)
	return bb
}

// re Int16 из []byteHL
func IHL(bb []byte) int {
	return int(bb[0])<<8 + int(bb[1])
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
	logger := log.New(logOutput, prefix+" ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix)
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

// быстрый - без аллокаций и внешнего буфера
// Убедитесь, что размер буфера bufio.Reader достаточен для самых больших пакетов.
func ReadPac(conn net.Conn, reader *bufio.Reader, timeout time.Duration) ([]byte, error) {
	const pref = "ReadPac:"
	// чтение длины пакета[2] с дедлайном
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("%v setTimeout: %v", pref, err)
	}
	lenb, err := reader.Peek(2) //если в буфере нет будет ждать 2 байта
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil, fmt.Errorf("%v timeout", pref)
		}
		return nil, fmt.Errorf("%v peek: %v", pref, err)
	}
	_, err = reader.Discard(2)
	if err != nil {
		return nil, fmt.Errorf("%v discard: %v", pref, err)
	}

	//и чтение тела пакета с дедлайном
	length := int(binary.BigEndian.Uint16(lenb)) // осталось принять lenb-2+2&crc
	if length < 3 {
		return nil, fmt.Errorf("%v len=0", pref)
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("%v setTimeout2: %v", pref, err)
	}
	result, err := reader.Peek(length)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil, fmt.Errorf("%v timeout2", pref)
		}
		return nil, fmt.Errorf("%v peek2: %v", pref, err)
	}
	_, err = reader.Discard(length)
	if err != nil {
		return nil, fmt.Errorf("%v discard2: %v", pref, err)
	}
	return result, nil //возвращает тело пакета без len[2]
}

func Send(bb []byte, writer *bufio.Writer) error {
	_, err := writer.Write(bb)
	if err != nil {
		return fmt.Errorf("Send_write: %v", err)
	}
	if err = writer.Flush(); err != nil {
		return fmt.Errorf("Send_flush: %v", err)
	}
	return nil
}

// преобразует HEX‑строку с любыми разделителями в []byte
func HexToBuf(hexStr string) ([]byte, error) {
	// Удаляем префиксы 0x/0X, если есть
	hexStr = strings.ReplaceAll(hexStr, "0x", "")
	// hexStr = strings.ReplaceAll(hexStr, "0X", "")
	// Оставляем только hex‑символы (0‑9, A‑F, a‑f)
	var cleaned strings.Builder
	fmt.Println("")
	cc := 0
	for _, c := range hexStr {
		if (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') { //|| (c >= 'a' && c <= 'f') {
			fmt.Printf("%c", c)
			if cc%2 != 0 {
				fmt.Print("-")
			}
			cc++
			cleaned.WriteRune(c)
		}
	}
	fmt.Println("")
	hexClean := cleaned.String()
	// Проверяем, что длина чётная (каждому байту нужно 2 hex‑символа)
	if len(hexClean)%2 != 0 {
		return nil, fmt.Errorf("HexToBuf not even")
	}
	// Конвертируем пары hex‑символов в байты
	result := make([]byte, len(hexClean)/2)
	for i := 0; i < len(hexClean); i += 2 {
		byteStr := hexClean[i : i+2]
		val, err := parseHexByte(byteStr)
		if err != nil {
			return nil, fmt.Errorf("HexToBuf bad pos %d: %v", i, err)
		}
		result[i/2] = val
	}
	return result, nil
}

// parseHexByte конвертирует 2 hex‑символа в байт
func parseHexByte(s string) (byte, error) {
	var b byte
	for _, c := range s {
		b <<= 4
		switch {
		case c >= '0' && c <= '9':
			b += byte(c - '0')
		case c >= 'A' && c <= 'F':
			b += byte(c - 'A' + 10)
		case c >= 'a' && c <= 'f':
			b += byte(c - 'a' + 10)
		default:
			return 0, fmt.Errorf("bad symp")
		}
	}
	return b, nil
}

// Convert []byte to string "XX-XX...""
func BufToHex(arr []byte) (str string) {
	for _, b := range arr {
		// str += " 0x" //prefix
		if b < 0x10 {
			str += fmt.Sprintf("0%X-", b)
		} else {
			str += fmt.Sprintf("%X-", b)
		}
	}
	if len(str) > 1 {
		return str[:len(str)-1]
	}
	return ""
}

// читает длину, потом тело пакета с дедлайнами
// не в VikingFrame т.к. нужен доступ к таймаутам conn
// ! может блокировать
// с внешним буфером
// func ReadPac1(conn net.Conn, reader *bufio.Reader, buffer []byte, timeout time.Duration) ([]byte, error) {
// 	const pref = "ReadPac:"
// 	if len(buffer) < 2 {
// 		return nil, fmt.Errorf("%v bad short buffer", pref)
// 	}
// 	// чтение длины пакета с дедлайном
// 	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
// 		return nil, fmt.Errorf("%v SetReadDeadline: %v", pref, err)
// 	}
// 	_, err := io.ReadFull(reader, buffer[:2])
// 	if err != nil {
// 		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
// 			return nil, fmt.Errorf("%v timeout", pref)
// 		}
// 		return nil, fmt.Errorf("%v ReadFull: %v", pref, err)
// 	}
// 	length := int(binary.BigEndian.Uint16(buffer[:2]))
// 	if length == 0 {
// 		return nil, fmt.Errorf("%v empty", pref)
// 	}
// 	// Проверяем размер буфера
// 	if len(buffer) < length+4 {
// 		return nil, fmt.Errorf("%v bad short buffer2", pref)
// 	}
// 	// чтение тела пакета с дедлайном
// 	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
// 		return nil, fmt.Errorf("%v SetReadDeadline2: %v", pref, err)
// 	}
// 	_, err = io.ReadFull(reader, buffer[2:length+2])
// 	if err != nil {
// 		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
// 			return nil, fmt.Errorf("%v timeout2", pref)
// 		}
// 		return nil, fmt.Errorf("%v ReadFull2: %v", pref, err)
// 	}
// 	return buffer[:length+2], nil
// }

// func readMsg(reader *bufio.Reader) (Message, error) {
// 	bb, err := reader.ReadString('\n') //marshal убрал \n из json
// 	if err != nil {
// 		return Message{}, fmt.Errorf("readMsg ReadString: %v", err)
// 	}
// 	var msg Message
// 	if err := json.Unmarshal([]byte(bb), &msg); err == nil {
// 		return msg, nil
// 	}
// 	return Message{}, fmt.Errorf("readMsg Unmarshal: %v", err)
// }
// func sendMsg(msg *Message, writer *bufio.Writer) error {
// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		return fmt.Errorf("sendMsg Marshal: %v", err)
// 	}
// 	_, err = writer.Write(append(data, '\n'))
// 	if err != nil {
// 		return fmt.Errorf("sendMsg Write: %v", err)
// 	}
// 	if err = writer.Flush(); err != nil {
// 		return fmt.Errorf("sendMsg Flush: %v", err)
// 	}
// 	// log.Printf("%v Отправлен", pref)
// 	return nil
// }
