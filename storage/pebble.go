package storage

import (
	pebble "github.com/cockroachdb/pebble"
	"log"
)

type PebbleDB struct {
	db *pebble.DB
}

func (d *PebbleDB) Put(key string, value string) error {
	keyBytes := []byte(key)
	if err := d.db.Set(keyBytes, []byte(value), pebble.Sync); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (d *PebbleDB) get(key string) (string, error) {
	keyBytes := []byte(key)
	value, _, err := d.db.Get(keyBytes)
	return string(value), err
}

func NewPebbleDB(dir string) (*PebbleDB, error) {
	pdb, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	r := &PebbleDB{
		db: pdb,
	}
	return r, err
}

