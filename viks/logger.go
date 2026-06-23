package main

import (
	stdlog "log"
	"os"
	"path/filepath"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	//
	// Ротация логов с lumberjack
	// "gopkg.in/natefinch/lumberjack.v2"
)

/*
// Базовое логирование

	log.Info().Msg("Приложение запущено")
	log.Warn().Msg("Предупреждение: низкий уровень памяти")
	log.Error().Msg("Критическая ошибка")


	rotationWriter := &lumberjack.Logger{
		Filename:   "/var/log/myapp/app.log",
		MaxSize:    100,    // МБ до ротации
		MaxBackups: 7,     // количество старых файлов
		MaxAge:     28,   // дней хранения
		Compress:   true,  // сжатие старых файлов
	}

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

func LogSetup(logDir string) {

	// Временный вывод в stderr на случай ошибки
	stdlog.SetOutput(os.Stderr)
	var logFile *os.File

	// logDir = "" //tst
	if logDir != "" {
		//создать файл лога
		err := os.MkdirAll(logDir, 0755)
		if err != nil {
			stdlog.Fatalln("Не удалось создать директорию для логов:", logDir)
		}
		logFile, err = os.OpenFile(filepath.Join(logDir, "viksrv.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			stdlog.Fatalln("Не удалось создать файл для логов:", err)
		}
	}

	// Настраиваем zerolog
	// Установка глобального уровня логирования
	zerolog.SetGlobalLevel(zerolog.InfoLevel) //Устанавливаем уровень логирования: INFO + WARN, ERROR, FATAL

	// Настройка формата времени
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05"
	// zerolog.TimeFieldFormat = zerolog.TimeFormatUnix // Unix timestamp

	if logFile != nil {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: logFile})
	} else {
		//в консоль без json
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
}

func say(msg string) {
	log.Info().Msg(msg)
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
