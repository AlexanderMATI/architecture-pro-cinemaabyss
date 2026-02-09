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

type MovieEvent struct {
	MovieID int    `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserID  int    `json:"user_id"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  string    `json:"username"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID int       `json:"payment_id"`
	UserID    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

var writer *kafka.Writer

const (
	movieTopic   = "movie-events"
	userTopic    = "user-events"
	paymentTopic = "payment-events"
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func main() {
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokers, ",")

	writer = &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	var wg sync.WaitGroup
	topics := []string{movieTopic, userTopic, paymentTopic}
	for _, topic := range topics {
		wg.Add(1)
		go consume(context.Background(), topic, &wg)
	}

	http.HandleFunc("/api/events/movie", handleEvent(movieTopic))
	http.HandleFunc("/api/events/user", handleEvent(userTopic))
	http.HandleFunc("/api/events/payment", handleEvent(paymentTopic))
	http.HandleFunc("/api/events/health", handleHealth)

	port := getEnv("PORT", "8082")
	log.Printf("Events service starting on port %s", port)
	log.Printf("Connecting to Kafka brokers at %s", kafkaBrokers)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	wg.Wait()
}

func handleEvent(topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var eventData interface{}
		switch topic {
		case movieTopic:
			eventData = &MovieEvent{}
		case userTopic:
			eventData = &UserEvent{}
		case paymentTopic:
			eventData = &PaymentEvent{}
		default:
			http.Error(w, "Unknown event type", http.StatusBadRequest)
			return
		}

		if err := json.NewDecoder(r.Body).Decode(eventData); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		eventBytes, err := json.Marshal(eventData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = writer.WriteMessages(context.Background(), kafka.Message{
			Topic: topic,
			Value: eventBytes,
		})

		if err != nil {
			log.Printf("Failed to write message to Kafka: %v", err)
			http.Error(w, "Failed to write message to Kafka", http.StatusInternalServerError)
			return
		}

		log.Printf("Successfully produced message to topic %s: %s", topic, string(eventBytes))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func consume(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(kafkaBrokers, ",")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  "cinemaabyss-events-consumer-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer r.Close()

	log.Printf("Consumer started for topic %s", topic)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			log.Printf("Error reading message from topic %s: %v", topic, err)
			break
		}
		log.Printf("[CONSUMER] Received message from topic %s at offset %d: %s = %s\n", m.Topic, m.Offset, string(m.Key), string(m.Value))
	}
}