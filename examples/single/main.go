package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	reachcache "github.com/vernmorn/reachcache"
	_ "modernc.org/sqlite"
)

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

var db *sql.DB

func main() {
	var err error
	dbPath := getEnv("DB_PATH", "./users.db")
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	initUsersDB()

	group := reachcache.NewGroup("users", 64<<20,
		reachcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			id, err := strconv.Atoi(key)
			if err != nil {
				return nil, fmt.Errorf("invalid user id: %s", key)
			}
			log.Printf("[DB] query user id=%d", id)
			var u User
			err = db.QueryRow("SELECT id, name, age FROM users WHERE id = ?", id).Scan(&u.ID, &u.Name, &u.Age)
			if err != nil {
				return nil, fmt.Errorf("user not found: %d", id)
			}
			return json.Marshal(u)
		}),
		reachcache.WithExpiration(10*time.Minute),
	)
	defer reachcache.DestroyGroup("users")

	mux := http.NewServeMux()
	mux.HandleFunc("/user/", func(w http.ResponseWriter, r *http.Request) {
		idStr := strings.TrimPrefix(r.URL.Path, "/user/")
		if idStr == "" {
			http.Error(w, "missing user id", 400)
			return
		}

		switch r.Method {
		case http.MethodGet:
			handleGetUser(w, r, group, idStr)
		case http.MethodPut:
			handleUpdateUser(w, r, group, idStr)
		default:
			http.Error(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(group.Stats())
	})

	addr := getEnv("HTTP_ADDR", ":8080")
	server := &http.Server{Addr: addr, Handler: mux}

	go func() {
		log.Printf("user service on %s (db=%s)", addr, dbPath)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("shutting down...")
	server.Close()
}

func handleGetUser(w http.ResponseWriter, r *http.Request, group *reachcache.Group, idStr string) {
	val, err := group.Get(r.Context(), idStr)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(val.ByteSlice())
}

func handleUpdateUser(w http.ResponseWriter, r *http.Request, group *reachcache.Group, idStr string) {
	name := r.URL.Query().Get("name")
	ageStr := r.URL.Query().Get("age")
	if name == "" || ageStr == "" {
		http.Error(w, "name and age required", 400)
		return
	}
	age, err := strconv.Atoi(ageStr)
	if err != nil {
		http.Error(w, "invalid age", 400)
		return
	}

	id, _ := strconv.Atoi(idStr)
	_, err = db.Exec("UPDATE users SET name = ?, age = ? WHERE id = ?", name, age, id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	u := User{ID: id, Name: name, Age: age}
	data, _ := json.Marshal(u)
	group.Set(r.Context(), idStr, data)
	log.Printf("[Set] cache updated: user id=%d", id)

	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func initUsersDB() {
	_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		age  INTEGER NOT NULL
	)`)

	var count int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if count > 0 {
		log.Printf("users table has %d rows, skip seeding", count)
		return
	}

	log.Println("seeding 50 users...")
	tx, _ := db.Begin()
	for i := 1; i <= 50; i++ {
		tx.Exec("INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
			i, fmt.Sprintf("User-%d", i), 20+(i%30))
	}
	tx.Commit()
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
