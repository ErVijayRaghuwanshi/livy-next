// @title Livy-Next REST API
// @version 1.0.0
// @description High-performance, lightweight Apache Livy successor designed for Spark 4.x and Apache Spark Connect.
// @description Features interactive Spark session management, decoupled identity, multi-tenancy, statement execution, and result pagination.
// @contact.name Livy-Next Maintainers
// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @host localhost:8998
// @BasePath /
// @tag.name sessions
// @tag.description Interactive Spark Connect session management and lifecycle operations
// @tag.name statements
// @tag.description Asynchronous statement submission, cancellation, and result pagination
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"livy-next/pkg/api"
	"livy-next/pkg/session"
	"livy-next/pkg/spark"

	_ "livy-next/docs"

	"github.com/google/uuid"
)

func buildSparkRemoteURI(baseRemote string, userId string, sessionId string, userAgent string, token string, keepaliveTime time.Duration, keepaliveTimeout time.Duration) string {
	remote := strings.TrimRight(baseRemote, "/")
	separator := "/;"
	if strings.Contains(remote, ";") {
		separator = ";"
	}

	var params []string
	if userId != "" {
		params = append(params, fmt.Sprintf("user_id=%s", userId))
	}
	if sessionId != "" {
		params = append(params, fmt.Sprintf("session_id=%s", sessionId))
	}
	if userAgent != "" {
		params = append(params, fmt.Sprintf("user_agent=%s", userAgent))
	}
	if token != "" {
		params = append(params, fmt.Sprintf("token=%s", token))
	}
	if keepaliveTime > 0 {
		params = append(params, "grpc_keepalive_enabled=true")
		params = append(params, fmt.Sprintf("grpc_keepalive_time_ms=%d", keepaliveTime.Milliseconds()))
		params = append(params, fmt.Sprintf("grpc_keepalive_timeout_ms=%d", keepaliveTimeout.Milliseconds()))
		params = append(params, "grpc_keepalive_without_calls=true")
	}

	if len(params) == 0 {
		return remote
	}

	return fmt.Sprintf("%s%s%s", remote, separator, strings.Join(params, ";"))
}

func main() {
	addr := flag.String("addr", ":8998", "HTTP service address to bind to")
	sparkRemote := flag.String("spark-remote", "sc://localhost:15002", "Spark Connect remote endpoint")
	defaultSparkUI := "http://localhost:4141"
	if envUI := os.Getenv("SPARK_UI_URL"); envUI != "" {
		defaultSparkUI = envUI
	}
	sparkUIUrl := flag.String("spark-ui-url", defaultSparkUI, "Base URL for the Spark Web UI")
	sparkHistoryUrl := flag.String("spark-history-url", "http://localhost:18088", "Base URL for the Spark History Server UI")
	sparkToken := flag.String("spark-token", "", "Pre-shared authentication token for Spark Connect")
	grpcKeepaliveTime := flag.Duration("grpc-keepalive-time", 60*time.Second, "gRPC keepalive ping time")
	grpcKeepaliveTimeout := flag.Duration("grpc-keepalive-timeout", 20*time.Second, "gRPC keepalive ping timeout")
	defaultStatementLimit := flag.Int("default-statement-limit", 10000, "Default maximum rows returned by SQL statements (0 for unlimited)")
	idleTimeout := flag.Duration("idle-timeout", 30*time.Minute, "Session idle timeout")
	deadTimeout := flag.Duration("dead-timeout", 5*time.Minute, "Session dead/stopped retention duration in history")
	syncSessionTimeout := flag.Bool("sync-session-timeout", true, "Synchronize session idle timeout with Spark Connect server")
	corsAllowedOrigins := flag.String("cors-allowed-origins", "*", "Comma-separated list of allowed CORS origins")
	mockMode := flag.Bool("mock", false, "Use in-memory mock Spark client for testing without a real Spark cluster")
	flag.Parse()

	log.Printf("Starting livy-next on %s", *addr)
	log.Printf("Spark Connect remote endpoint: %s", *sparkRemote)
	log.Printf("Spark UI URL: %s", *sparkUIUrl)
	log.Printf("CORS allowed origins: %s", *corsAllowedOrigins)
	log.Printf("Mock mode: %v", *mockMode)
	log.Printf("Dead session retention timeout: %s", *deadTimeout)
	log.Printf("Sync session timeout with Spark Connect: %v", *syncSessionTimeout)

	// 1. Initialize session manager
	manager := session.NewManager(*idleTimeout, *deadTimeout)
	defer manager.CloseAll()

	// 1b. Asynchronously probe Spark Connect server on startup to discover exact version, master, and timeout
	if !*mockMode {
		go func() {
			probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer probeCancel()
			probeClient, err := spark.NewClient(probeCtx, *sparkRemote, "livy-next-startup-probe")
			if err == nil {
				defer probeClient.Close()
				if version, err := probeClient.GetSparkVersion(probeCtx); err == nil && version != "" {
					master, _ := probeClient.GetMaster(probeCtx)
					manager.SetSparkInfo(version, master)
					log.Printf("Connected to Apache Spark %s (Master: %s)", version, master)
				}
				if *syncSessionTimeout {
					if remoteTimeout, err := probeClient.GetSessionTimeout(probeCtx); err == nil && remoteTimeout > 0 {
						manager.SetIdleTimeout(remoteTimeout)
						log.Printf("Synchronized Spark Connect session timeout: %v", remoteTimeout)
					}
				}
			} else {
				log.Printf("Startup probe to %s deferred (%v); will sync on first session creation", *sparkRemote, err)
			}
		}()
	}

	// 2. Define ClientCreator
	creator := func(params session.SessionCreateParams) (session.SparkClient, error) {
		name := params.Name
		if *mockMode {
			log.Printf("Creating MOCK Spark Connect client for session %q (sessionId=%s, userId=%s)", name, params.SessionID, params.UserID)
			return &spark.MockClient{AppName: name, SessionID: params.SessionID}, nil
		}

		sessionId := params.SessionID
		if sessionId == "" {
			sessionId = uuid.New().String()
			params.SessionID = sessionId
		}

		userId := params.UserID
		if userId == "" {
			userId = params.ProxyUser
		}

		userAgent := params.UserAgent
		if userAgent == "" {
			userAgent = "livy-next"
		}

		token := params.Token
		if token == "" {
			token = *sparkToken
		}

		remote := buildSparkRemoteURI(*sparkRemote, userId, sessionId, userAgent, token, *grpcKeepaliveTime, *grpcKeepaliveTimeout)

		log.Printf("Creating Spark Connect client for session %q, kind: %s, remote: %s", name, params.Kind, remote)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		client, err := spark.NewClient(ctx, remote, name, *defaultStatementLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to build Spark session: %w", err)
		}

		// Apply custom configurations
		for k, v := range params.Conf {
			log.Printf("Setting config %s = %s", k, v)
			if err := client.SetConfig(ctx, k, v); err != nil {
				if strings.Contains(err.Error(), "CANNOT_MODIFY_STATIC_CONFIG") {
					log.Printf("Warning: configuration %s is static on Spark Connect server and cannot be modified dynamically: %v", k, err)
				} else {
					client.Close()
					return nil, fmt.Errorf("failed to set config %s: %w", k, err)
				}
			}
		}

		// Add custom JARs dynamically using Spark's ADD JAR command
		for _, jar := range params.Jars {
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

	handler := api.NewHandler(manager, creator, *sparkUIUrl, *sparkHistoryUrl, *syncSessionTimeout)
	router := api.SetupRouter(handler, origins)

	// 4. Start HTTP Server with Graceful Shutdown
	server := &http.Server{
		Addr:    *addr,
		Handler: router,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		log.Printf("Server listening on %s", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	sig := <-sigCh
	log.Printf("Received shutdown signal %s, initiating graceful shutdown...", sig)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown warning: %v", err)
	}

	log.Printf("Closing all active Spark Connect sessions...")
	manager.CloseAll()
	log.Printf("Livy-Next stopped gracefully.")
}
