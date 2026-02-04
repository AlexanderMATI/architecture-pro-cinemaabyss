package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Структуры событий с добавленными тегами для валидации
type MovieEvent struct {
	MovieID int    `json:"movie_id" validate:"required"`
	Title   string `json:"title" validate:"required"`
	Action  string `json:"action" validate:"required"`
	UserID  int    `json:"user_id" validate:"required"`
}

type UserEvent struct {
	UserID    int       `json:"user_id" validate:"required"`
	Username  string    `json:"username" validate:"required"`
	Action    string    `json:"action" validate:"required"`
	Timestamp time.Time `json:"timestamp" validate:"required"`
}

type PaymentEvent struct {
	PaymentID int       `json:"payment_id" validate:"required"`
	UserID    int       `json:"user_id" validate:"required"`
	Amount    float64   `json:"amount" validate:"required"`
	Status    string    `json:"status" validate:"required"`
	Timestamp time.Time `json:"timestamp" validate:"required"`
}

// Константы для названий топиков
const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

var (
	kafkaWriter *kafka.Writer
	once        sync.Once
)

// getEnv возвращает значение переменной окружения или значение по умолчанию
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// initKafkaWriter инициализирует Kafka writer (используется паттерн singleton)
func initKafkaWriter() *kafka.Writer {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokers, ",")

	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}
}

// getKafkaWriter возвращает экземпляр Kafka writer
func getKafkaWriter() *kafka.Writer {
	once.Do(func() {
		kafkaWriter = initKafkaWriter()
	})
	return kafkaWriter
}

func main() {
	// Инициализация Kafka writer
	writer := getKafkaWriter()
	defer writer.Close()

	// Запуск потребителей для каждого топика
	var wg sync.WaitGroup
	topics := []string{movieTopic, userTopic, paymentTopic}

	for _, topic := range topics {
		wg.Add(1)
		go consumeTopic(context.Background(), topic, &wg)
	}

	// Настройка HTTP маршрутов
	http.HandleFunc("/api/events/movie", handleEvent(movieTopic))
	http.HandleFunc("/api/events/user", handleEvent(userTopic))
	http.HandleFunc("/api/events/payment", handleEvent(paymentTopic))
	http.HandleFunc("/api/events/health", healthCheckHandler)

	// Запуск HTTP сервера
	port := getEnv("PORT", "8082")
	log.Printf("Сервис событий запускается на порту %s", port)
	log.Printf("Подключение к Kafka brokers: %s", getEnv("KAFKA_BROKERS", "localhost:9092"))

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Ошибка запуска сервера: %v", err)
	}

	wg.Wait()
}

// handleEvent создает обработчик для разных типов событий
func handleEvent(topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Проверка метода HTTP
		if r.Method != http.MethodPost {
			http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
			return
		}

		// Парсинг события в зависимости от топика
		eventData, err := parseEventData(topic, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Отправка события в Kafka
		if err := sendToKafka(topic, eventData); err != nil {
			log.Printf("Ошибка отправки в Kafka: %v", err)
			http.Error(w, "Ошибка обработки события", http.StatusInternalServerError)
			return
		}

		// Успешный ответ
		sendSuccessResponse(w, topic, eventData)
	}
}

// parseEventData парсит тело запроса в соответствующую структуру
func parseEventData(topic string, r *http.Request) (interface{}, error) {
	var eventData interface{}

	switch topic {
	case movieTopic:
		eventData = &MovieEvent{}
	case userTopic:
		eventData = &UserEvent{}
	case paymentTopic:
		eventData = &PaymentEvent{}
	default:
		return nil, &json.UnsupportedTypeError{}
	}

	if err := json.NewDecoder(r.Body).Decode(eventData); err != nil {
		return nil, err
	}

	return eventData, nil
}

// sendToKafka отправляет событие в Kafka
func sendToKafka(topic string, eventData interface{}) error {
	eventBytes, err := json.Marshal(eventData)
	if err != nil {
		return err
	}

	writer := getKafkaWriter()
	message := kafka.Message{
		Topic: topic,
		Value: eventBytes,
	}

	return writer.WriteMessages(context.Background(), message)
}

// sendSuccessResponse отправляет успешный HTTP ответ
func sendSuccessResponse(w http.ResponseWriter, topic string, eventData interface{}) {
	eventBytes, _ := json.Marshal(eventData)
	log.Printf("Сообщение отправлено в топик %s: %s", topic, string(eventBytes))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Событие успешно обработано",
	})
}

// healthCheckHandler обработчик проверки здоровья сервиса
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    true,
		"service":   "events-service",
		"timestamp": time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// consumeTopic потребляет сообщения из указанного топика Kafka
func consumeTopic(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokers, ",")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "cinemaabyss-events-consumer-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Printf("Потребитель запущен для топика: %s", topic)

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("Ошибка чтения из топика %s: %v", topic, err)
			break
		}

		log.Printf("[ПОТРЕБИТЕЛЬ] Топик: %s, Смещение: %d, Сообщение: %s",
			message.Topic, message.Offset, string(message.Value))
	}
}
