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

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func main() {
	rand.Seed(time.Now().UnixNano())

	port := getEnv("PORT", "8000")
	monolithURL := getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	eventsServiceURL := getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")
	gradualMigrationEnabled := getEnv("GRADUAL_MIGRATION", "false") == "true"
	migrationPercentStr := getEnv("MOVIES_MIGRATION_PERCENT", "0")

	migrationPercent, err := strconv.Atoi(migrationPercentStr)
	if err != nil {
		log.Printf("Invalid MOVIES_MIGRATION_PERCENT value, defaulting to 0. Error: %v", err)
		migrationPercent = 0
	}

	monoURL, err := url.Parse(monolithURL)
	if err != nil {
		log.Fatalf("Failed to parse MONOLITH_URL: %v", err)
	}
	movURL, err := url.Parse(moviesServiceURL)
	if err != nil {
		log.Fatalf("Failed to parse MOVIES_SERVICE_URL: %v", err)
	}
	evtURL, err := url.Parse(eventsServiceURL)
	if err != nil {
		log.Fatalf("Failed to parse EVENTS_SERVICE_URL: %v", err)
	}

	monolithProxy := httputil.NewSingleHostReverseProxy(monoURL)
	moviesProxy := httputil.NewSingleHostReverseProxy(movURL)
	eventsProxy := httputil.NewSingleHostReverseProxy(evtURL)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Incoming request: %s %s", r.Method, r.URL.Path)

		switch {
		case strings.HasPrefix(r.URL.Path, "/api/movies"):
			if gradualMigrationEnabled && rand.Intn(100) < migrationPercent {
				log.Printf("Routing to movies-service (migration)")
				moviesProxy.ServeHTTP(w, r)
			} else {
				log.Printf("Routing to monolith")
				monolithProxy.ServeHTTP(w, r)
			}
		case strings.HasPrefix(r.URL.Path, "/api/events"):
			log.Printf("Routing to events-service")
			eventsProxy.ServeHTTP(w, r)
		default:
			log.Printf("Routing to monolith (default)")
			monolithProxy.ServeHTTP(w, r)
		}
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Strangler Fig Proxy is healthy"))
	})

	log.Printf("Strangler Fig Proxy started on port %s", port)
	log.Printf("Monolith URL: %s", monolithURL)
	log.Printf("Movies Service URL: %s", moviesServiceURL)
	log.Printf("Gradual migration enabled: %v", gradualMigrationEnabled)
	log.Printf("Movies migration percentage: %d%%", migrationPercent)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}