package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

type ShortLink struct {
	Code  string `json:"code"`
	URL   string `json:"url"`
	Clicks int   `json:"clicks"`
}

func initShortLinkDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS shortlinks (
		code   TEXT PRIMARY KEY,
		url    TEXT NOT NULL,
		clicks INTEGER DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	if err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	return db, nil
}

func insertLink(db *sql.DB, code, url string) error {
	_, err := db.Exec("INSERT INTO shortlinks (code, url) VALUES (?, ?)", code, url)
	return err
}

func getLink(db *sql.DB, code string) (*ShortLink, error) {
	sl := &ShortLink{}
	err := db.QueryRow("SELECT code, url, clicks FROM shortlinks WHERE code = ?", code).Scan(&sl.Code, &sl.URL, &sl.Clicks)
	if err != nil {
		return nil, err
	}
	return sl, nil
}

func incrementClicks(db *sql.DB, code string) {
	_, _ = db.Exec("UPDATE shortlinks SET clicks = clicks + 1 WHERE code = ?", code)
}

func linkExists(db *sql.DB, code string) (bool, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM shortlinks WHERE code = ?", code).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func findByURL(db *sql.DB, url string) (*ShortLink, error) {
	sl := &ShortLink{}
	err := db.QueryRow("SELECT code, url, clicks FROM shortlinks WHERE url = ?", url).Scan(&sl.Code, &sl.URL, &sl.Clicks)
	if err != nil {
		return nil, err
	}
	return sl, nil
}

func initTestData(db *sql.DB) {
	var count int
	db.QueryRow("SELECT COUNT(*) FROM shortlinks").Scan(&count)
	if count > 0 {
		log.Printf("shortlinks table has %d rows, skip seeding", count)
		return
	}

	log.Println("seeding sample shortlinks...")
	entries := []struct{ code, url string }{
		{"00000001", "https://github.com/vernmorn/reachcache"},
		{"00000002", "https://golang.org"},
		{"00000003", "https://google.com"},
	}
	for _, e := range entries {
		insertLink(db, e.code, e.url)
	}
}
