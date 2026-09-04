package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

// re []byteHL из Int16
func BHL(vv int) []byte {
	bb := make([]byte, 2)
	bb[0] = byte(vv >> 8)
	bb[1] = byte(vv)
	return bb
}

// re Int16 из []byteHL
func IHL(bb []byte) int {
	if len(bb) < 2 {
		return 0
	}
	return int(bb[0])<<8 + int(bb[1])
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
			data = append(data, processedLine...)
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

	return data, nil
}

// вычитывает и возвращает пакет, re true-выход по таймауту (ai)
// быстрый - без аллокаций и внешнего буфера
// если указан timeoutms ждем первые 2 байта с этим таймаутом, но следующие байты всегда дочитываются с таймаутом 5с
// Убедитесь, что размер буфера bufio.Reader достаточен для самых больших пакетов
func ReadPac(conn net.Conn, reader *bufio.Reader, timeoutms int) ([]byte, bool, error) {
	const pref = "ReadPac:"
	const maxPacketSize = 65000
	const readBodyTimeout = 5 * time.Second

	//установка таймаута
	if timeoutms > 0 {
		timeout := time.Duration(timeoutms) * time.Millisecond
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, false, fmt.Errorf("%v setTimeout: %v", pref, err)
		}
	} else {
		//бесконечный таймаут 'conn.SetReadDeadline(time.Time{})' не все реализации поддерживают
		// if err := conn.SetReadDeadline(time.Now().Add(100 * 365 * 24 * time.Hour)); err != nil { уст на 100лет
		if err := conn.SetReadDeadline(time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)); err != nil { //3000 год - без вычислений
			return nil, false, fmt.Errorf("%v setTimeout2: %v", pref, err)
		}
	}

	// чтение длины пакета[2]
	lenb, err := reader.Peek(2) //если в буфере нет - будет ждать 2 байта
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil, true, nil //fmt.Errorf("%v timeout", pref)
		}
		return nil, false, fmt.Errorf("%v peek: %v", pref, err)
	}

	header := make([]byte, 2) // копия, потому что lenb станет невалидным после Discard
	copy(header, lenb)
	_, err = reader.Discard(2)
	if err != nil {
		return nil, false, fmt.Errorf("%v discard: %v", pref, err)
	}

	//и чтение остатка пакета с дедлайном
	length := IHL(header) + 2 // осталось принять lenb+2crc
	if length < 3 || length > maxPacketSize {
		//очистить буфер
		available := reader.Buffered()
		if available > 0 {
			reader.Discard(available)
		}
		return nil, false, fmt.Errorf("%v bad len pac %d", pref, length)
	}

	if err := conn.SetReadDeadline(time.Now().Add(readBodyTimeout)); err != nil {
		return nil, false, fmt.Errorf("%v setTimeout2: %v", pref, err)
	}
	// читаем тело напрямую (не через Peek + Discard)
	buf := make([]byte, length)
	n, err := io.ReadFull(reader, buf)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return nil, false, fmt.Errorf("%v timeout2", pref)
		}
		if err == io.ErrUnexpectedEOF {
			// прочитано меньше, чем ожидалось - buf игнорируем
			return nil, false, fmt.Errorf("%v unexpected EOF: read %d of %d bytes", pref, n, length)
		}
		return nil, false, fmt.Errorf("%v read body: %v", pref, err)
	}

	// Собираем результат: заголовок + тело, append тут непредсказуем
	result := make([]byte, 2+length)
	copy(result[:2], header)
	copy(result[2:], buf)

	return result, false, nil
}

func Send(bb []byte, conn net.Conn, writer *bufio.Writer, timeoutms int) error {
	conn.SetWriteDeadline(time.Now().Add(time.Duration(timeoutms) * time.Millisecond))
	_, err := writer.Write(bb)
	if err != nil {
		return fmt.Errorf("Send_write: %v", err)
	}
	// fmt.Printf("Буфер содержит %d байт перед Flush()", writer.Buffered())
	if err = writer.Flush(); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("Send_flush: timeout")
		} else {
			return fmt.Errorf("Send_flush: %v", err)
		}
	}
	return nil
}

// только помещает в буфер для последующей отправки (для поля LEN)
// func SendFirst(bb []byte, writer *bufio.Writer) error {
// 	_, err := writer.Write(bb)
// 	if err != nil {
// 		return fmt.Errorf("SendFirst_write: %v", err)
// 	}
// 	return nil
// }

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
func BufToHex(arr []byte) string {
	if len(arr) == 0 {
		return ""
	}
	var builder strings.Builder
	builder.Grow(len(arr) * 3) // выделяем память заранее: каждый байт -> "XX-" (3 символа)

	for _, b := range arr {
		builder.WriteString(fmt.Sprintf("%02X-", b))
	}

	// убираем последний "-"
	result := builder.String()
	return result[:len(result)-1]
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
