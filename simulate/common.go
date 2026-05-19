package main //is copy main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

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
