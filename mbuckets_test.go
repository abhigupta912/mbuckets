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

func TestCreateBucket(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1)

	bucketName2 := []byte("Bucket1/Bucket2")
	bucket2 := db.Bucket(bucketName2)

	err = bucket1.CreateBucket()
	if err != nil {
		t.Error("Unable to create bucket. ", err.Error())
	}

	err = bucket2.CreateBucket()
	if err != nil {
		t.Error("Unable to create bucket. ", err.Error())
	}

	bucketNames, err := db.GetAllBucketNames()
	if err != nil {
		t.Error("Unable to get bucket names from db", err.Error())
	}

	bucketsExpected := make([][]byte, 0, 2)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Unable to retrieve some bucket names from db")
	}

	numMatches := 0
	for _, bucketName := range bucketNames {
		for _, bucket := range bucketsExpected {
			if bytes.Compare(bucketName, bucket) == 0 {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(bucketsExpected) {
		t.Error("Not all bucket names retrieved match the ones created in db")
	}
}

func TestCreateBucketWithSeparator(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	separator := []byte(":")
	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1).WithSeparator(separator)

	bucketName2 := []byte("Bucket1:Bucket2")
	bucket2 := db.Bucket(bucketName2).WithSeparator(separator)

	err = bucket1.CreateBucket()
	if err != nil {
		t.Error("Unable to create bucket. ", err.Error())
	}

	err = bucket2.CreateBucket()
	if err != nil {
		t.Error("Unable to create bucket. ", err.Error())
	}

	bucketNames, err := db.GetAllBucketNamesWithSeparator(separator)
	if err != nil {
		t.Error("Unable to get bucket names from db", err.Error())
	}

	bucketsExpected := make([][]byte, 0, 2)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Unable to retrieve some bucket names from db")
	}

	numMatches := 0
	for _, bucketName := range bucketNames {
		for _, bucket := range bucketsExpected {
			if bytes.Compare(bucketName, bucket) == 0 {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(bucketsExpected) {
		t.Error("Not all bucket names retrieved match the ones created in db")
	}
}

func TestInsertGet(t *testing.T) {
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

func TestInsertGetString(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	key := "key1"
	value := "value1"

	err = bucket.InsertString(key, value)
	if err != nil {
		t.Error("Unable to insert key/value in test database. ", err.Error())
	}

	var result string
	result, err = bucket.GetString(key)
	if err != nil {
		t.Error("Unable to get value for given key from test database. ", err.Error())
	}

	if value != result {
		t.Error("Value retrieved for given key does not match value set for the same key in test database")
	}
}

func TestInsertGetAll(t *testing.T) {
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
		t.Error("Unable to get values for all keys from test database. ", err.Error())
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

func TestInsertGetAllString(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make(map[string]string, 3)

	key1 := "key1"
	value1 := "value1"
	items[key1] = value1

	key2 := "key2"
	value2 := "value2"
	items[key2] = value2

	key3 := "key3"
	value3 := "value3"
	items[key3] = value3

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}

	results, err := bucket.GetAllString()
	if err != nil {
		t.Error("Unable to get values for all keys from test database. ", err.Error())
	}

	if len(results) != len(items) {
		t.Error("Unable to retrieve same number of key/value pairs from test database as were inserted")
	}

	numMatches := 0
	for resultKey, resultValue := range results {
		for itemKey, itemValue := range items {
			if resultKey == itemKey && resultValue == itemValue {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(results) {
		t.Error("Not all key/value pairs retrieved match the ones inserted into the test database")
	}
}

func TestGetPrefix(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make([]mbuckets.Item, 0, 3)

	prefix := []byte("pfx:")
	prefixItemCount := 0

	prefixCopy1 := make([]byte, len(prefix))
	copy(prefixCopy1, prefix)

	key1Buf := bytes.NewBuffer(prefixCopy1)
	key1Buf.Write([]byte("key1"))
	key1 := key1Buf.Bytes()

	value1 := []byte("value1")
	items = append(items, mbuckets.Item{key1, value1})
	prefixItemCount++

	prefixCopy2 := make([]byte, len(prefix))
	copy(prefixCopy2, prefix)

	key2Buf := bytes.NewBuffer(prefixCopy2)
	key2Buf.Write([]byte("key2"))
	key2 := key2Buf.Bytes()

	value2 := []byte("value2")
	items = append(items, mbuckets.Item{key2, value2})
	prefixItemCount++

	key3 := []byte("key3")
	value3 := []byte("value3")
	items = append(items, mbuckets.Item{key3, value3})

	err = bucket.InsertAll(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}

	results, err := bucket.GetPrefix(prefix)
	if err != nil {
		t.Error("Unable to get results for keys with given prefix from test database. ", err.Error())
	}

	if len(results) != prefixItemCount {
		t.Error("Unable to retrieve same number of key/value pairs from test database as were inserted with given prefix")
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

	if numMatches != prefixItemCount {
		t.Error("Not all key/value pairs retrieved match the ones inserted into the test database with given prefix")
	}
}

func TestGetPrefixString(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make(map[string]string, 3)

	prefix := "pfx:"
	prefixItemCount := 0

	key1Buf := bytes.NewBufferString(prefix)
	key1Buf.WriteString("key1")
	key1 := key1Buf.String()

	value1 := "value1"
	items[key1] = value1
	prefixItemCount++

	key2Buf := bytes.NewBufferString(prefix)
	key2Buf.WriteString("key2")
	key2 := key2Buf.String()

	value2 := "value2"
	items[key2] = value2
	prefixItemCount++

	key3 := "key3"
	value3 := "value3"
	items[key3] = value3

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}

	results, err := bucket.GetPrefixString(prefix)
	if err != nil {
		t.Error("Unable to get results for keys with given prefix from test database. ", err.Error())
	}

	if len(results) != prefixItemCount {
		t.Error("Unable to retrieve same number of key/value pairs from test database as were inserted with given prefix")
	}

	numMatches := 0
	for resultKey, resultValue := range results {
		for itemKey, itemValue := range items {
			if resultKey == itemKey && resultValue == itemValue {
				numMatches++
				break
			}
		}
	}

	if numMatches != prefixItemCount {
		t.Error("Not all key/value pairs retrieved match the ones inserted into the test database with given prefix")
	}
}

func TestGetRange(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make([]mbuckets.Item, 0, 5)

	key1 := []byte("key1")
	value1 := []byte("value1")
	items = append(items, mbuckets.Item{key1, value1})

	key2 := []byte("key2")
	value2 := []byte("value2")
	items = append(items, mbuckets.Item{key2, value2})

	key3 := []byte("key3")
	value3 := []byte("value3")
	items = append(items, mbuckets.Item{key3, value3})

	key4 := []byte("key4")
	value4 := []byte("value4")
	items = append(items, mbuckets.Item{key4, value4})

	key5 := []byte("key5")
	value5 := []byte("value5")
	items = append(items, mbuckets.Item{key5, value5})

	err = bucket.InsertAll(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}

	results, err := bucket.GetRange(key2, key4)
	if err != nil {
		t.Error("Unable to get value for given key range from test database. ", err.Error())
	}

	itemsExpected := make([]mbuckets.Item, 0, 3)
	itemsExpected = append(itemsExpected, mbuckets.Item{key2, value2})
	itemsExpected = append(itemsExpected, mbuckets.Item{key3, value3})
	itemsExpected = append(itemsExpected, mbuckets.Item{key4, value4})

	if len(results) != len(itemsExpected) {
		t.Error("Unable to retrieve same number of key/value pairs from test database as were inserted within the given range")
	}

	numMatches := 0
	for _, result := range results {
		for _, item := range itemsExpected {
			if bytes.Compare(result.Key, item.Key) == 0 && bytes.Compare(result.Value, item.Value) == 0 {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(results) {
		t.Error("Not all key/value pairs retrieved match the ones inserted into the test database within the given range")
	}
}

func TestGetRangeString(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make(map[string]string, 5)

	key1 := "key1"
	value1 := "value1"
	items[key1] = value1

	key2 := "key2"
	value2 := "value2"
	items[key2] = value2

	key3 := "key3"
	value3 := "value3"
	items[key3] = value3

	key4 := "key4"
	value4 := "value4"
	items[key4] = value4

	key5 := "key5"
	value5 := "value5"
	items[key5] = value5

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}

	results, err := bucket.GetRangeString(key2, key4)
	if err != nil {
		t.Error("Unable to get value for given key range from test database. ", err.Error())
	}

	itemsExpected := make(map[string]string, 3)
	itemsExpected[key2] = value2
	itemsExpected[key3] = value3
	itemsExpected[key4] = value4

	if len(results) != len(itemsExpected) {
		t.Error("Unable to retrieve same number of key/value pairs from test database as were inserted within the given range")
	}

	numMatches := 0
	for resultKey, resultValue := range results {
		for itemKey, itemValue := range itemsExpected {
			if resultKey == itemKey && resultValue == itemValue {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(results) {
		t.Error("Not all key/value pairs retrieved match the ones inserted into the test database within the given range")
	}
}

func TestDelete(t *testing.T) {
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

	err = bucket.Delete(key2)
	if err != nil {
		t.Error("Unable to delete value for given key from test database. ", err.Error())
	}

	itemsExpected := make([]mbuckets.Item, 0, 2)
	itemsExpected = append(itemsExpected, mbuckets.Item{key1, value1})
	itemsExpected = append(itemsExpected, mbuckets.Item{key3, value3})

	results, err := bucket.GetAll()
	if err != nil {
		t.Error("Unable to get value for all keys from test database. ", err.Error())
	}

	if len(results) != len(itemsExpected) {
		t.Error("Unable to retrieve same number of key/value pairs from test database as expected after deletion")
	}

	numMatches := 0
	for _, result := range results {
		for _, item := range itemsExpected {
			if bytes.Compare(result.Key, item.Key) == 0 && bytes.Compare(result.Value, item.Value) == 0 {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(results) {
		t.Error("Not all key/value pairs retrieved match the ones as expected after deletion")
	}
}

func TestDeleteString(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName := []byte("Bucket1")
	bucket := db.Bucket(bucketName)

	items := make(map[string]string, 5)

	key1 := "key1"
	value1 := "value1"
	items[key1] = value1

	key2 := "key2"
	value2 := "value2"
	items[key2] = value2

	key3 := "key3"
	value3 := "value3"
	items[key3] = value3

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Error("Unable to insert key/value pairs in test database. ", err.Error())
	}

	err = bucket.DeleteString(key2)
	if err != nil {
		t.Error("Unable to delete value for given key from test database. ", err.Error())
	}

	itemsExpected := make(map[string]string, 2)
	itemsExpected[key1] = value1
	itemsExpected[key3] = value3

	results, err := bucket.GetAllString()
	if err != nil {
		t.Error("Unable to get value for all keys from test database. ", err.Error())
	}

	if len(results) != len(itemsExpected) {
		t.Error("Unable to retrieve same number of key/value pairs from test database as expected after deletion")
	}

	numMatches := 0
	for resultKey, resultValue := range results {
		for itemKey, itemValue := range itemsExpected {
			if resultKey == itemKey && resultValue == itemValue {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(results) {
		t.Error("Not all key/value pairs retrieved match the ones as expected after deletion")
	}
}

func TestGetRootBucketNamesFromDB(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1)

	key1 := []byte("key1")
	value1 := []byte("value1")

	err = bucket1.Insert(key1, value1)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketName2 := []byte("Bucket2")
	bucket2 := db.Bucket(bucketName2)

	key2 := []byte("key2")
	value2 := []byte("value2")

	err = bucket2.Insert(key2, value2)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketName3 := []byte("Bucket1/Bucket3")
	bucket3 := db.Bucket(bucketName3)

	key3 := []byte("key3")
	value3 := []byte("value3")

	err = bucket3.Insert(key3, value3)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketName4 := []byte("Bucket2/Bucket3")
	bucket4 := db.Bucket(bucketName4)

	key4 := []byte("key4")
	value4 := []byte("value4")

	err = bucket4.Insert(key4, value4)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketNames, err := db.GetRootBucketNames()
	if err != nil {
		t.Error("Unable to get root bucket names from db", err.Error())
	}

	bucketsExpected := make([][]byte, 0, 2)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Unable to retrieve some root bucket names from db")
	}

	numMatches := 0
	for _, bucketName := range bucketNames {
		for _, bucket := range bucketsExpected {
			if bytes.Compare(bucketName, bucket) == 0 {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(bucketsExpected) {
		t.Error("Not all root buckets retrieved match the ones created in db")
	}
}

func TestGetAllBucketNamesFromDB(t *testing.T) {
	db, err := NewTestDB()
	if err != nil {
		t.Error("Unable to open test database. ", err.Error())
	}
	defer db.Close()

	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1)

	key1 := []byte("key1")
	value1 := []byte("value1")

	err = bucket1.Insert(key1, value1)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketName2 := []byte("Bucket1/Bucket2")
	bucket2 := db.Bucket(bucketName2)

	key2 := []byte("key2")
	value2 := []byte("value2")

	err = bucket2.Insert(key2, value2)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketName3 := []byte("Bucket1/Bucket2/Bucket3")
	bucket3 := db.Bucket(bucketName3)

	key3 := []byte("key3")
	value3 := []byte("value3")

	err = bucket3.Insert(key3, value3)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketName4 := []byte("Bucket2")
	bucket4 := db.Bucket(bucketName4)

	key4 := []byte("key4")
	value4 := []byte("value4")

	err = bucket4.Insert(key4, value4)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketName5 := []byte("Bucket2/Bucket3")
	bucket5 := db.Bucket(bucketName5)

	key5 := []byte("key5")
	value5 := []byte("value5")

	err = bucket5.Insert(key5, value5)
	if err != nil {
		t.Error("Unable to create bucket and insert key/value pair", err.Error())
	}

	bucketNames, err := db.GetAllBucketNames()
	if err != nil {
		t.Error("Unable to get all bucket names from db", err.Error())
	}

	bucketsExpected := make([][]byte, 0, 3)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)
	bucketsExpected = append(bucketsExpected, bucketName3)
	bucketsExpected = append(bucketsExpected, bucketName4)
	bucketsExpected = append(bucketsExpected, bucketName5)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Unable to retrieve all bucket names from db")
	}

	numMatches := 0
	for _, bucketName := range bucketNames {
		for _, bucket := range bucketsExpected {
			if bytes.Compare(bucketName, bucket) == 0 {
				numMatches++
				break
			}
		}
	}

	if numMatches != len(bucketsExpected) {
		t.Error("Not all buckets retrieved match the ones created in db")
	}
}
