package mbuckets_test

import (
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/abhigupta912/mbuckets"
)

type TestDB struct {
	*mbuckets.DB
}

func NewTestDB() (*TestDB, error) {
	fileName := tempFile()
	db, err := mbuckets.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &TestDB{db}, nil
}

func (db *TestDB) Close() {
	defer os.Remove(db.Path())
	db.DB.Close()
}

func tempFile() string {
	file, err := ioutil.TempFile("", "bolt-")
	if err != nil {
		log.Fatal(err)
	}

	if err := file.Close(); err != nil {
		log.Fatal(err)
	}

	if err := os.Remove(file.Name()); err != nil {
		log.Fatal(err)
	}

	return file.Name()
}

func TestOpen(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()
}
