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

	faviconHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			w.Write([]byte(faviconSVG))
		}
	}
	r.Get("/favicon.ico", faviconHandler)
	r.Head("/favicon.ico", faviconHandler)

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

const faviconSVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" fill="none">
  <defs>
    <linearGradient id="lnBg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0e1322"/>
      <stop offset="100%" stop-color="#060913"/>
    </linearGradient>
    <linearGradient id="lnBorder" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#38bdf8"/>
      <stop offset="40%" stop-color="#6366f1"/>
      <stop offset="80%" stop-color="#a855f7"/>
      <stop offset="100%" stop-color="#f43f5e"/>
    </linearGradient>
    <linearGradient id="lnArmL" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#38bdf8"/>
      <stop offset="50%" stop-color="#6366f1"/>
      <stop offset="100%" stop-color="#8b5cf6"/>
    </linearGradient>
    <linearGradient id="lnNext" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#c084fc"/>
      <stop offset="40%" stop-color="#f43f5e"/>
      <stop offset="100%" stop-color="#fb923c"/>
    </linearGradient>
    <linearGradient id="lnSpark" x1="0%" y1="100%" x2="100%" y2="0%">
      <stop offset="0%" stop-color="#22d3ee"/>
      <stop offset="50%" stop-color="#ffffff"/>
      <stop offset="100%" stop-color="#fef08a"/>
    </linearGradient>
    <filter id="lnGlow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="3" result="blur"/>
      <feComposite in="SourceGraphic" in2="blur" operator="over"/>
    </filter>
  </defs>
  <rect x="4" y="4" width="92" height="92" rx="24" fill="url(#lnBg)" stroke="url(#lnBorder)" stroke-width="2.5"/>
  <circle cx="20" cy="28" r="3" fill="#38bdf8" opacity="0.85"/>
  <circle cx="80" cy="30" r="3" fill="#fb923c" opacity="0.85"/>
  <circle cx="24" cy="76" r="2.5" fill="#818cf8" opacity="0.75"/>
  <circle cx="76" cy="74" r="3" fill="#f43f5e" opacity="0.85"/>
  <path d="M26 20 C26 17.8 27.8 16 30 16 L36 16 C38.2 16 40 17.8 40 20 L40 62 C40 63.1 40.9 64 42 64 L64 64 C66.2 64 68 65.8 68 68 L68 72 C68 74.2 66.2 76 64 76 L30 76 C27.8 76 26 74.2 26 72 Z" fill="url(#lnArmL)" filter="url(#lnGlow)"/>
  <path d="M52 24 C50.5 22.5 50.5 20 52 18.5 L55 15.5 C56.5 14 59 14 60.5 15.5 L79 34 C80.5 35.5 80.5 38 79 39.5 L60.5 58 C59 59.5 56.5 59.5 55 58 L52 55 C50.5 53.5 50.5 51 52 49.5 L65 36.8 Z" fill="url(#lnNext)" filter="url(#lnGlow)"/>
  <g filter="url(#lnGlow)">
    <path d="M50 36 L43 50 L51 50 L45 66 L59 48 L49 48 Z" fill="url(#lnSpark)"/>
    <circle cx="49" cy="49" r="2.5" fill="#ffffff"/>
  </g>
</svg>`

