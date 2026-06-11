package main

import (
	"os"
	// "log"

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

func LogSetup() {
	// Установка глобального уровня логирования
	zerolog.SetGlobalLevel(zerolog.InfoLevel) // INFO + WARN, ERROR, FATAL

	// Настройка формата времени
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05" // Человекочитаемый формат
	// zerolog.TimeFieldFormat = zerolog.TimeFormatUnix // Unix timestamp

	//в консоль без json
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
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
