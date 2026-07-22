package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	reachcache "github.com/vernmorn/reachcache"
)

var (
	db       *sql.DB
	cache    *reachcache.Group
	baseURL  string
	nodeName string
)

func main() {
	name := flag.String("name", "node-a", "node name for display")
	grpcAddr := flag.String("addr", ":50051", "gRPC server listen address")
	httpAddr := flag.String("http", ":8081", "HTTP server listen address")
	advAddr := flag.String("advertise", "", "gRPC advertise address for etcd (default: same as -addr with local IP)")
	etcdEP := flag.String("etcd", getEnv("ETCD_ENDPOINTS", "127.0.0.1:2379"), "etcd endpoints (comma-separated)")
	svcName := flag.String("svc", "reach-cache", "service name for etcd")
	dbPath := flag.String("db", "./shortlink.db", "path to SQLite database")
	flag.Parse()

	nodeName = *name
	advertiseAddr := *advAddr
	if advertiseAddr == "" {
		advertiseAddr = "127.0.0.1" + (*grpcAddr)
		if advertiseAddr[0] == ':' {
			advertiseAddr = "127.0.0.1" + *grpcAddr
		} else {
			advertiseAddr = *grpcAddr
		}
	}
	baseURL = fmt.Sprintf("http://localhost%s", *httpAddr)

	log.SetPrefix(fmt.Sprintf("[%s] ", *name))
	log.Printf("starting node: gRPC=%s HTTP=%s advertise=%s etcd=%s", *grpcAddr, *httpAddr, advertiseAddr, *etcdEP)

	var err error
	db, err = initShortLinkDB(*dbPath)
	if err != nil {
		log.Fatalf("db init: %v", err)
	}
	defer db.Close()

	initTestData(db)

	picker, err := reachcache.NewClientPicker(
		advertiseAddr,
		reachcache.WithServiceName(*svcName),
		reachcache.WithEtcdEndpointsForClientPicker(strings.Split(*etcdEP, ",")),
	)
	if err != nil {
		log.Fatalf("create picker: %v", err)
	}
	defer picker.Close()

	server, err := reachcache.NewServer(*grpcAddr, *svcName,
		reachcache.WithEtcdEndpoints(strings.Split(*etcdEP, ",")),
		reachcache.WithDialTimeout(5*time.Second),
		reachcache.WithAdvertiseAddr(advertiseAddr),
	)
	if err != nil {
		log.Fatalf("create server: %v", err)
	}

	cache = reachcache.NewGroup("shortlink", 32<<20,
		reachcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("[DB] query code=%s", key)
			sl, err := getLink(db, key)
			if err != nil {
				return nil, err
			}
			return []byte(sl.URL), nil
		}),
		reachcache.WithPeers(picker),
		reachcache.WithExpiration(1*time.Hour),
	)
	defer reachcache.DestroyGroup("shortlink")

	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/stats", handleStats)
	mux.HandleFunc("/shorten", handleShorten)
	mux.HandleFunc("/", handleRoot)

	httpServer := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		log.Printf("HTTP server on %s", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	log.Printf("node ready")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("shutting down...")
	httpServer.Close()
	server.Stop()
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	if path == "/" || path == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<!DOCTYPE html>
<html><head><title>ReachCache Shortlink</title></head><body>
<h2>ReachCache Shortlink Demo</h2>
<form action="/shorten" method="post">
  <input type="url" name="url" placeholder="Enter URL to shorten" size="60" required>
  <button type="submit">Shorten</button>
</form>
<p>Try: <code>curl -X POST "http://localhost:`+portFromRequest(r)+`/shorten?url=https://example.com"</code></p>
</body></html>`)
		return
	}

	code := strings.TrimPrefix(path, "/")
	if code == "" {
		http.NotFound(w, r)
		return
	}

	handleRedirect(w, r, code)
}

func handleShorten(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}

	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		if r.Method == http.MethodPost {
			targetURL = r.FormValue("url")
		}
	}
	if targetURL == "" {
		http.Error(w, "url parameter required", 400)
		return
	}

	existing, err := findByURL(db, targetURL)
	if err == nil {
		log.Printf("[shorten] existing code=%s url=%s", existing.Code, targetURL)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"code":"%s","url":"%s","short_url":"%s/%s"}`, existing.Code, html.EscapeString(targetURL), baseURL, existing.Code)
		return
	}

	var code string
	for retry := 0; retry < 10; retry++ {
		if retry == 0 {
			code = generateShortCode(targetURL)
		} else {
			code = createCollisionSafeCode(targetURL, retry)
		}

		exists, _ := linkExists(db, code)
		if !exists {
			break
		}
		if retry == 9 {
			http.Error(w, "failed to generate unique code", 500)
			return
		}
	}

	if err := insertLink(db, code, targetURL); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	cache.Set(r.Context(), code, []byte(targetURL))
	log.Printf("[shorten] created code=%s url=%s", code, targetURL)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"code":"%s","url":"%s","short_url":"%s/%s"}`, code, html.EscapeString(targetURL), baseURL, code)
}

func handleRedirect(w http.ResponseWriter, r *http.Request, code string) {
	val, err := cache.Get(r.Context(), code)
	if err != nil {
		http.Error(w, "short link not found", 404)
		return
	}

	incrementClicks(db, code)
	originalURL := string(val.Bytes())

	log.Printf("[redirect] code=%s → %s", code, originalURL)
	http.Redirect(w, r, originalURL, http.StatusMovedPermanently)
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	stats := cache.Stats()
	stats["node"] = nodeName

	var totalLinks int
	db.QueryRow("SELECT COUNT(*) FROM shortlinks").Scan(&totalLinks)
	stats["total_links"] = totalLinks

	var totalClicks int
	db.QueryRow("SELECT COALESCE(SUM(clicks), 0) FROM shortlinks").Scan(&totalClicks)
	stats["total_clicks"] = totalClicks

	fmt.Fprintf(w, "%+v\n", stats)
}

func portFromRequest(r *http.Request) string {
	idx := strings.LastIndex(r.Host, ":")
	if idx >= 0 {
		return r.Host[idx+1:]
	}
	return "8081"
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
