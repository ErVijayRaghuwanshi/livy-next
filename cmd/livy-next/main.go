// @title Livy-Next API
// @version 1.0
// @description Apache Livy successor working on Spark 4.0 and Spark Connect.
// @host localhost:8998
// @BasePath /
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"livy-next/pkg/api"
	"livy-next/pkg/session"
	"livy-next/pkg/spark"

	_ "livy-next/docs"
)

func main() {
	addr := flag.String("addr", ":8998", "HTTP service address to bind to")
	sparkRemote := flag.String("spark-remote", "sc://localhost:15002", "Spark Connect remote endpoint")
	idleTimeout := flag.Duration("idle-timeout", 30*time.Minute, "Session idle timeout")
	deadTimeout := flag.Duration("dead-timeout", 5*time.Minute, "Session dead/stopped retention duration in history")
	corsAllowedOrigins := flag.String("cors-allowed-origins", "*", "Comma-separated list of allowed CORS origins")
	mockMode := flag.Bool("mock", false, "Use in-memory mock Spark client for testing without a real Spark cluster")
	flag.Parse()

	log.Printf("Starting livy-next on %s", *addr)
	log.Printf("Spark Connect remote endpoint: %s", *sparkRemote)
	log.Printf("CORS allowed origins: %s", *corsAllowedOrigins)
	log.Printf("Mock mode: %v", *mockMode)
	log.Printf("Dead session retention timeout: %s", *deadTimeout)

	// 1. Initialize session manager
	manager := session.NewManager(*idleTimeout, *deadTimeout)
	defer manager.CloseAll()

	// 2. Define ClientCreator
	creator := func(name string, kind string, conf map[string]string, jars []string, proxyUser string) (session.SparkClient, error) {
		if *mockMode {
			log.Printf("Creating MOCK Spark Connect client for session %q", name)
			return &spark.MockClient{AppName: name}, nil
		}

		remote := *sparkRemote
		if proxyUser != "" {
			if strings.Contains(remote, ";") {
				remote = fmt.Sprintf("%s;user_id=%s", remote, proxyUser)
			} else {
				remote = fmt.Sprintf("%s/;user_id=%s", remote, proxyUser)
			}
		}

		log.Printf("Creating Spark Connect client for session %q, kind: %s, remote: %s", name, kind, remote)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		client, err := spark.NewClient(ctx, remote, name)
		if err != nil {
			return nil, fmt.Errorf("failed to build Spark session: %w", err)
		}

		// Apply custom configurations
		for k, v := range conf {
			log.Printf("Setting config %s = %s", k, v)
			if err := client.SetConfig(ctx, k, v); err != nil {
				client.Close()
				return nil, fmt.Errorf("failed to set config %s: %w", k, err)
			}
		}

		// Add custom JARs dynamically using Spark's ADD JAR command
		for _, jar := range jars {
			log.Printf("Adding jar: %s", jar)
			if _, err := client.ExecuteSQL(ctx, fmt.Sprintf("ADD JAR %s", jar)); err != nil {
				client.Close()
				return nil, fmt.Errorf("failed to add jar %s: %w", jar, err)
			}
		}

		return client, nil
	}

	// 3. Create Handlers and Router
	origins := strings.Split(*corsAllowedOrigins, ",")
	for i := range origins {
		origins[i] = strings.TrimSpace(origins[i])
	}

	handler := api.NewHandler(manager, creator)
	router := api.SetupRouter(handler, origins)

	// 4. Start HTTP Server
	server := &http.Server{
		Addr:    *addr,
		Handler: router,
	}

	log.Printf("Server listening on %s", *addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
}
