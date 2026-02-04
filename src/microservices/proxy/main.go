package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Конфигурация прокси
type ProxyConfig struct {
	Port                  string
	MonolithURL           *url.URL
	MoviesServiceURL      *url.URL
	EventsServiceURL      *url.URL
	GradualMigration      bool
	MoviesMigrationPercent int
}

// getEnv возвращает значение переменной окружения или значение по умолчанию
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// parseURL безопасно парсит URL из строки
func parseURL(rawURL, envVarName string) *url.URL {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("Ошибка парсинга %s: %v", envVarName, err)
	}
	return parsedURL
}

// parseMigrationPercent парсит процент миграции
func parseMigrationPercent(percentStr string) int {
	percent, err := strconv.Atoi(percentStr)
	if err != nil || percent < 0 || percent > 100 {
		log.Printf("Некорректное значение MOVIES_MIGRATION_PERCENT '%s', используется 0", percentStr)
		return 0
	}
	return percent
}

// loadConfig загружает конфигурацию из переменных окружения
func loadConfig() *ProxyConfig {
	// Инициализация генератора случайных чисел
	rand.New(rand.NewSource(time.Now().UnixNano()))
	
	// Чтение конфигурации
	port := getEnv("PORT", "8000")
	monolithURL := getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	eventsServiceURL := getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")
	gradualMigration := getEnv("GRADUAL_MIGRATION", "false") == "true"
	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "0")
	
	// Парсинг URL
	monoURL := parseURL(monolithURL, "MONOLITH_URL")
	movURL := parseURL(moviesServiceURL, "MOVIES_SERVICE_URL")
	evtURL := parseURL(eventsServiceURL, "EVENTS_SERVICE_URL")
	
	// Парсинг процента миграции
	migrationPercent := parseMigrationPercent(migrationPercentStr)
	
	return &ProxyConfig{
		Port:                  port,
		MonolithURL:           monoURL,
		MoviesServiceURL:      movURL,
		EventsServiceURL:      evtURL,
		GradualMigration:      gradualMigration,
		MoviesMigrationPercent: migrationPercent,
	}
}

// createReverseProxy создает обратный прокси для указанного URL
func createReverseProxy(targetURL *url.URL) *httputil.ReverseProxy {
	return httputil.NewSingleHostReverseProxy(targetURL)
}

// shouldRouteToMovies определяет, нужно ли маршрутизировать запрос к сервису фильмов
func shouldRouteToMovies(config *ProxyConfig) bool {
	if !config.GradualMigration {
		return false
	}
	
	// Генерация случайного числа от 0 до 99
	randomValue := rand.Intn(100)
	return randomValue < config.MoviesMigrationPercent
}

// routeRequest определяет куда маршрутизировать запрос
func routeRequest(config *ProxyConfig, path string) (*httputil.ReverseProxy, string) {
	switch {
	case strings.HasPrefix(path, "/api/movies"):
		if shouldRouteToMovies(config) {
			return createReverseProxy(config.MoviesServiceURL), "movies-service"
		}
		return createReverseProxy(config.MonolithURL), "monolith"
		
	case strings.HasPrefix(path, "/api/events"):
		return createReverseProxy(config.EventsServiceURL), "events-service"
		
	default:
		return createReverseProxy(config.MonolithURL), "monolith"
	}
}

// mainHandler обрабатывает входящие HTTP запросы
func mainHandler(config *ProxyConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Логирование входящего запроса
		log.Printf("Входящий запрос: %s %s", r.Method, r.URL.Path)
		
		// Определение целевого сервиса и создание прокси
		proxy, targetService := routeRequest(config, r.URL.Path)
		
		// Логирование маршрутизации
		log.Printf("Маршрутизация к %s", targetService)
		
		// Проксирование запроса
		proxy.ServeHTTP(w, r)
	}
}

// healthHandler обработчик проверки здоровья
func healthHandler(w http.ResponseWriter, r *http.Request) {
	response := map[string]string{
		"status":   "healthy",
		"service":  "strangler-fig-proxy",
		"datetime": time.Now().Format(time.RFC3339),
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	
	// В реальном приложении здесь был бы json.NewEncoder
	jsonResponse := `{"status":"healthy","service":"strangler-fig-proxy","datetime":"` + 
		time.Now().Format(time.RFC3339) + `"}`
	
	w.Write([]byte(jsonResponse))
}

// logConfig выводит конфигурацию при запуске
func logConfig(config *ProxyConfig) {
	log.Printf("Strangler Fig Proxy запущен на порту %s", config.Port)
	log.Printf("Монолит URL: %s", config.MonolithURL.String())
	log.Printf("Сервис фильмов URL: %s", config.MoviesServiceURL.String())
	log.Printf("Сервис событий URL: %s", config.EventsServiceURL.String())
	log.Printf("Постепенная миграция включена: %v", config.GradualMigration)
	log.Printf("Процент миграции фильмов: %d%%", config.MoviesMigrationPercent)
}

func main() {
	// Загрузка конфигурации
	config := loadConfig()
	
	// Создание прокси для каждого сервиса
	// (в реальном коде они создаются в routeRequest, но можно кэшировать)
	
	// Настройка HTTP обработчиков
	http.HandleFunc("/", mainHandler(config))
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/health", healthHandler)
	
	// Логирование конфигурации
	logConfig(config)
	
	// Запуск HTTP сервера
	serverAddr := ":" + config.Port
	log.Printf("Запуск сервера на %s", serverAddr)
	
	if err := http.ListenAndServe(serverAddr, nil); err != nil {
		log.Fatalf("Ошибка запуска сервера: %v", err)
	}
}