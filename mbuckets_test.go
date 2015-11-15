package mbuckets_test

import (
	"bytes"
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

func TestInsert(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	key := []byte("key1")
	value := []byte("value1")

	err = bucket.Insert(key, value)
	if err != nil {
		t.Error("Unable to insert key/value in test database. ", err.Error())
	}
}

func TestInsertAll(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make([]mbuckets.Item, 0, 3)

	key1 := []byte("key1")
	value1 := []byte("value1")
	items = append(items, mbuckets.Item{key1, value1})

	key2 := []byte("key2")
	value2 := []byte("value2")
	items = append(items, mbuckets.Item{key2, value2})

	key3 := []byte("key3")
	value3 := []byte("value3")
	items = append(items, mbuckets.Item{key3, value3})

	err = bucket.InsertAll(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}
}

func TestGet(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	key := []byte("key1")
	value := []byte("value1")

	err = bucket.Insert(key, value)
	if err != nil {
		t.Error("Unable to insert key/value in test database. ", err.Error())
	}

	var result []byte
	result, err = bucket.Get(key)
	if err != nil {
		t.Error("Unable to get value for given key from test database. ", err.Error())
	}

	if bytes.Compare(result, value) != 0 {
		t.Error("Value retrieved for given key does not match value set for the same key in test database")
	}
}

func TestGetAll(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make([]mbuckets.Item, 0, 3)

	key1 := []byte("key1")
	value1 := []byte("value1")
	items = append(items, mbuckets.Item{key1, value1})

	key2 := []byte("key2")
	value2 := []byte("value2")
	items = append(items, mbuckets.Item{key2, value2})

	key3 := []byte("key3")
	value3 := []byte("value3")
	items = append(items, mbuckets.Item{key3, value3})

	err = bucket.InsertAll(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}

	results, err := bucket.GetAll()
	if err != nil {
		t.Error("Unable to get value for given key from test database. ", err.Error())
	}

	if len(results) != len(items) {
		t.Error("Unable to retrieve same number of key/value pairs from test database as were inserted")
	}

	numMatches := 0
	for _, result := range results {
		for _, item := range items {
			if bytes.Compare(result.Key, item.Key) == 0 && bytes.Compare(result.Value, item.Value) == 0 {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(results) {
		t.Error("Not all key/value pairs retrieved match the ones inserted into the test database")
	}
}
