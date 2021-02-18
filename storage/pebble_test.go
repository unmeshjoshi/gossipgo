package storage

import (
	"log"
	"strings"
	"testing"
)

func TestPebbleDBSetAndGet(t *testing.T) {
	db, err := NewPebbleDB("demo")
	if (err != nil) {
		log.Fatalf("error creating database %q", err)
	}
	db.Put("testKey", "testValue")
	value, _ := db.get("testKey")
	if !strings.Contains(value, "testValue") {
		t.Errorf("expected body to contain %q, got %q", "testValue", value)
	}
}
