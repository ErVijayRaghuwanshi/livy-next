package api

import (
	_ "embed"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

//go:embed ui/index.html
var uiHTML string

// SetupRouter configures the Chi router and registers the REST API endpoints.
func SetupRouter(h *Handler, allowedOrigins []string) *chi.Mux {
	r := chi.NewRouter()

	r.Use(CORSMiddleware(allowedOrigins))
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", http.StatusMovedPermanently)
	})

	r.Get("/ui", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(uiHTML))
	})

	r.Get("/version", h.GetVersion)

	r.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	r.Route("/sessions", func(r chi.Router) {
		r.Get("/", h.ListSessions)
		r.Post("/", h.CreateSession)

		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.GetSession)
			r.Delete("/", h.DeleteSession)

			r.Route("/statements", func(r chi.Router) {
				r.Get("/", h.ListStatements)
				r.Post("/", h.SubmitStatement)
				r.Get("/{statementId}", h.GetStatement)
				r.Post("/{statementId}/cancel", h.CancelStatement)
			})
		})
	})

	return r
}

// CORSMiddleware returns a standard CORS handler middleware.
func CORSMiddleware(allowedOrigins []string) func(http.Handler) http.Handler {
	allowedMap := make(map[string]bool)
	allowAll := false
	for _, origin := range allowedOrigins {
		if origin == "*" {
			allowAll = true
			break
		}
		allowedMap[origin] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" {
				if allowAll {
					w.Header().Set("Access-Control-Allow-Origin", "*")
				} else if allowedMap[origin] {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
				}

				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
				w.Header().Set("Access-Control-Max-Age", "86400")
			}

			// Handle preflight
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

