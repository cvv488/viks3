package main

import (
	"io"
	"strconv"
	"strings"

	// stdlog "log"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	//
	// Ротация логов с lumberjack
	// "gopkg.in/natefinch/lumberjack.v2"
)

/*
	// Создаём логгер с ротацией
	logger := zerolog.New(rotationWriter).
		With().
		Timestamp().
		Logger()

	// Устанавливаем глобальный логгер
	log.Logger = logger

	// Тестовые сообщения
	log.Info().Str("version", "1.0.0").Msg("Сервис запущен")
	log.Debug().Str("method", "GET").Str("path", "/api/users").Msg("HTTP запрос")
*/

func LogSetup(mode, logDir string) error {

	modes := strings.Split(mode, ",")
	switch modes[0] {
	case "1": //в файл
	case "2":
		logDir = "" //в консоль
	default:
		return nil //по умолчанию
	}
	var w io.Writer
	timeFormat := "2006-01-02 15:04:05"
	if logDir == "" {
		if len(modes) > 2 && modes[2] == "j" { //JSON
			w = os.Stdout
		} else {
			// Консоль: цветной, читаемый формат
			w = zerolog.ConsoleWriter{
				Out:        os.Stdout,
				TimeFormat: timeFormat,
				NoColor:    false, // цвета включены
				// LevelFormat:   "%s",                 // можно кастомизировать
				// MessageFormat: "%s",
			}
		}
	} else {
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			return err
		}
		path := filepath.Join(logDir, "app.log")
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}

		if len(modes) > 2 && modes[2] == "j" {
			w = file //JSON
		} else {
			w = zerolog.ConsoleWriter{
				Out:        file,
				TimeFormat: timeFormat,
				NoColor:    true, // в файле цвета не нужны
			}
		}
	}

	logger := zerolog.New(w).With().Timestamp().Logger()

	// Установка глобального уровня логирования
	level := int(zerolog.DebugLevel) //0-DEBUG 1-INFO 2-WARN 3-ERROR 4-FATAL 5-PANIC 6-NO -1-TRACE
	if len(modes) > 1 {
		number, err := strconv.Atoi(modes[1])
		if err == nil {
			level = number
		}
	}
	logger = logger.Level(zerolog.Level(level))

	log.Logger = logger
	return nil
}

func say(msg string) {
	log.Info().Msg(msg)
}
func sayW(msg string) {
	log.Warn().Msg(msg) //todo other Levels
}

// ошибка с контекстом
func sayError(msg string, err error) {
	log.Error().Err(err).Msg(msg)
}

func sayError1(msg string) {
	log.Error().Msg(msg)
}

// func sayError(msg any) {
// 	str := ""
// 	switch v := msg.(type) {
// 	case string:
// 		str += v
// 	case error:
// 		str += v.Error()
// 	}
// 	// case int, int32, int64, uint, uint32, uint64: str += fmt.Sprintf("%d", v)
// 	// fmt.Println("ERROR:", str)
// 	// log.Error().Err(str)
// 	log.Error().Msg(str)
// }

//std log
// func NewLogger(LogFile string, prefix string) *log.Logger {
// 	var logOutput io.Writer
// 	if LogFile != "" {
// 		logFile, err := os.OpenFile(LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
// 		if err != nil {
// 			log.Printf("Не удалось открыть файл логов: %v, используем stdout", err)
// 			logOutput = os.Stdout
// 		} else {
// 			logOutput = logFile
// 		}
// 	} else {
// 		logOutput = os.Stdout
// 	}
// 	logger := log.New(logOutput, prefix+" ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix)
// 	return logger
// }
