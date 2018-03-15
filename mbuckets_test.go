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
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")
}

func TestCreateBucket(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1)

	t.Logf("Creating Bucket: %s", bucketName1)
	err = bucket1.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName1)

	bucketName2 := []byte("Bucket1/Bucket2")
	bucket2 := db.Bucket(bucketName2)

	t.Logf("Creating Bucket: %s", bucketName2)
	err = bucket2.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName2)

	t.Log("Retrieving all bucket names")
	bucketNames, err := db.GetAllBucketNames()
	if err != nil {
		t.Errorf("Unable to get bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	bucketsExpected := make([][]byte, 0, 2)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Number of buckets in db do not match the expected count")
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
		t.Error("Bucket names in db do not match expected bucket names")
	}
}

func TestCreateBucketWithSeparator(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	separator := []byte(":")
	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1).WithSeparator(separator)

	t.Logf("Creating Bucket: %s", bucketName1)
	err = bucket1.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName1)

	bucketName2 := []byte("Bucket1:Bucket2")
	bucket2 := db.Bucket(bucketName2).WithSeparator(separator)

	t.Logf("Creating Bucket: %s", bucketName2)
	err = bucket2.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName2)

	t.Log("Retrieving all bucket names")
	bucketNames, err := db.GetAllBucketNamesWithSeparator(separator)
	if err != nil {
		t.Errorf("Unable to get bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	bucketsExpected := make([][]byte, 0, 2)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Number of buckets in db do not match the expected count")
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
		t.Error("Bucket names in db do not match expected bucket names")
	}
}

func TestInsertGet(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
	bucket := db.Bucket(bucketName)

	key := []byte("key1")
	value := []byte("value1")

	t.Logf("Inserting Key: %s with Value: %s in bucket: %s", key, value, bucketName)
	err = bucket.Insert(key, value)
	if err != nil {
		t.Errorf("Unable to insert key/value in bucket. Error: %s", err.Error())
	}

	var result []byte
	t.Logf("Retrieving Value for Key: %s from bucket: %s", key, bucketName)
	result, err = bucket.Get(key)
	if err != nil {
		t.Errorf("Unable to retrieve value for given key from bucket. Error: %s", err.Error())
	}

	t.Logf("Value retrieved Value for given key: %s", result)
	if bytes.Compare(result, value) != 0 {
		t.Error("Retrieved value does not match value set for the same key in bucket")
	}
}

func TestInsertGetString(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
	bucket := db.Bucket(bucketName)

	key := "key1"
	value := "value1"

	t.Logf("Inserting Key: %s with Value: %s in bucket: %s", key, value, bucketName)
	err = bucket.InsertString(key, value)
	if err != nil {
		t.Errorf("Unable to insert key/value in bucket. Error: %s", err.Error())
	}

	var result string
	t.Logf("Retrieving Value for Key: %s from bucket: %s", key, bucketName)
	result, err = bucket.GetString(key)
	if err != nil {
		t.Errorf("Unable to retrieve value for given key from bucket. Error: %s", err.Error())
	}

	t.Logf("Value retrieved Value for given key: %s", result)
	if value != result {
		t.Error("Retrieved value does not match value set for the same key in bucket")
	}
}

func TestInsertGetAll(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
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

	t.Log("Inserting items")
	for idx, item := range items {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	err = bucket.InsertAll(items)
	if err != nil {
		t.Errorf("Unable to insert items in bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving items")
	results, err := bucket.GetAll()
	if err != nil {
		t.Errorf("Unable to retrieve items from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieved items")
	for idx, item := range results {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	if len(results) != len(items) {
		t.Error("Number of items retrieved from bucket does not match the number of items inserted")
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
		t.Error("Not all items retrieved from bucket match the ones inserted")
	}
}

func TestInsertGetAllString(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
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

	t.Log("Inserting key/value pairs")
	for key, value := range items {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Errorf("Unable to insert key/value pairs in bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving key/value pairs")
	results, err := bucket.GetAllString()
	if err != nil {
		t.Errorf("Unable to retrieve key/value pairs from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieved key/value pairs")
	for key, value := range results {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	if len(results) != len(items) {
		t.Error("Number of key/value pairs retrieved does not match the number of key/value pairs inserted")
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
		t.Error("Not all key/value pairs retrieved from bucket match the ones inserted")
	}
}

func TestGetPrefix(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
	bucket := db.Bucket(bucketName)

	items := make([]mbuckets.Item, 0, 3)

	prefix := []byte("pfx:")
	prefixItemCount := 0

	t.Logf("Using prefix: %s", prefix)

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

	t.Log("Inserting items")
	for idx, item := range items {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	err = bucket.InsertAll(items)
	if err != nil {
		t.Errorf("Unable to insert items in bucket. Error: %s", err.Error())
	}

	t.Logf("Retrieving items with prefix: %s", prefix)
	results, err := bucket.GetPrefix(prefix)
	if err != nil {
		t.Errorf("Unable to retrieve items with given preifx from bucket. Error: %s", err.Error())
	}

	t.Logf("Retrieved items with prefix: %s", prefix)
	for idx, item := range results {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	if len(results) != prefixItemCount {
		t.Error("Number of items retrieved from bucket with given prefix does not match the number of items inserted")
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
		t.Error("Not all items retrieved from bucket with given prefix match the ones inserted")
	}
}

func TestGetPrefixString(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
	bucket := db.Bucket(bucketName)

	items := make(map[string]string, 3)

	prefix := "pfx:"
	prefixItemCount := 0

	t.Logf("Using prefix: %s", prefix)

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

	t.Log("Inserting key/value pairs")
	for key, value := range items {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Errorf("Unable to key/value pairs in bucket. Error: %s", err.Error())
	}

	t.Logf("Retrieving key/value pairs with prefix: %s", prefix)
	results, err := bucket.GetPrefixString(prefix)
	if err != nil {
		t.Errorf("Unable to retrieve key/value pairs with given preifx from bucket. Error: %s", err.Error())
	}

	t.Logf("Retrieved key/value pairs with prefix: %s", prefix)
	for key, value := range results {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	if len(results) != prefixItemCount {
		t.Error("Number of key/value pairs retrieved from bucket with given prefix does not match the number of key/value pairs inserted")
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
		t.Error("Not all key/value pairs retrieved from bucket with given prefix match the ones inserted")
	}
}

func TestGetRange(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
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

	t.Log("Inserting items")
	for idx, item := range items {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	err = bucket.InsertAll(items)
	if err != nil {
		t.Errorf("Unable to insert items in bucket. Error: %s", err.Error())
	}

	t.Logf("Retrieving items in range: %s - %s", key2, key4)
	results, err := bucket.GetRange(key2, key4)
	if err != nil {
		t.Errorf("Unable to retrieve items for given range from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieved items in given range")
	for idx, item := range results {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	itemsExpected := make([]mbuckets.Item, 0, 3)
	itemsExpected = append(itemsExpected, mbuckets.Item{key2, value2})
	itemsExpected = append(itemsExpected, mbuckets.Item{key3, value3})
	itemsExpected = append(itemsExpected, mbuckets.Item{key4, value4})

	if len(results) != len(itemsExpected) {
		t.Error("Number of items retrieved from bucket in given range does not match the number of items inserted")
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
		t.Error("Not all items retrieved from bucket in given range match the ones inserted")
	}
}

func TestGetRangeString(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
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

	t.Log("Inserting key/value pairs")
	for key, value := range items {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Errorf("Unable to insert key/value pairs in bucket. Error: %s", err.Error())
	}

	t.Logf("Retrieving key/value pairs in range: %s - %s", key2, key4)
	results, err := bucket.GetRangeString(key2, key4)
	if err != nil {
		t.Errorf("Unable to retrieve key/value pairs for given range from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieved key/value pairs in given range")
	for key, value := range results {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	itemsExpected := make(map[string]string, 3)
	itemsExpected[key2] = value2
	itemsExpected[key3] = value3
	itemsExpected[key4] = value4

	if len(results) != len(itemsExpected) {
		t.Error("Number of key/value pairs retrieved from bucket in given range does not match the number of key/value pairs inserted")
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
		t.Error("Not all key/value pairs retrieved from bucket in given range match the ones inserted")
	}
}

func TestDelete(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
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

	t.Log("Inserting items")
	for idx, item := range items {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	err = bucket.InsertAll(items)
	if err != nil {
		t.Errorf("Unable to insert items in bucket. Error: %s", err.Error())
	}

	t.Logf("Deleting item with key: %s", key2)
	err = bucket.Delete(key2)
	if err != nil {
		t.Errorf("Unable to delete item with given key from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving items")
	results, err := bucket.GetAll()
	if err != nil {
		t.Errorf("Unable to retrieve items from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieved items")
	for idx, item := range results {
		t.Logf("Item %d: Key = %s, Value = %s", idx, item.Key, item.Value)
	}

	itemsExpected := make([]mbuckets.Item, 0, 2)
	itemsExpected = append(itemsExpected, mbuckets.Item{key1, value1})
	itemsExpected = append(itemsExpected, mbuckets.Item{key3, value3})

	if len(results) != len(itemsExpected) {
		t.Error("Number of items retrieved from bucket does not match the number of items expected after deletion")
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
		t.Error("Not all items retrieved from bucket match the ones expected after deletion")
	}
}

func TestDeleteString(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName)
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

	t.Log("Inserting key/value pairs")
	for key, value := range items {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	err = bucket.InsertAllString(items)
	if err != nil {
		t.Errorf("Unable to insert key/value pairs in bucket. Error: %s", err.Error())
	}

	t.Logf("Deleting item with key: %s", key2)
	err = bucket.DeleteString(key2)
	if err != nil {
		t.Errorf("Unable to delete item with given key from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving key/value pairs")
	results, err := bucket.GetAllString()
	if err != nil {
		t.Errorf("Unable to retrieve key/value pairs from bucket. Error: %s", err.Error())
	}

	t.Log("Retrieved key/value pairs")
	for key, value := range results {
		t.Logf("Key = %s, Value = %s", key, value)
	}

	itemsExpected := make(map[string]string, 2)
	itemsExpected[key1] = value1
	itemsExpected[key3] = value3

	if len(results) != len(itemsExpected) {
		t.Error("Number of key/value pairs retrieved from bucket does not match the number of key/value pairs expected after deletion")
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
		t.Error("Not all key/value pairs rretrieved from bucket match the ones expected after deletion")
	}
}

func TestDeleteBucket(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1)

	t.Logf("Creating Bucket: %s", bucketName1)
	err = bucket1.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName1)

	bucketName2 := []byte("Bucket1/Bucket2")
	bucket2 := db.Bucket(bucketName2)

	t.Logf("Creating Bucket: %s", bucketName2)
	err = bucket2.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName2)

	bucketName3 := []byte("Bucket1/Bucket2/Bucket3")
	bucket3 := db.Bucket(bucketName3)

	t.Logf("Creating Bucket: %s", bucketName3)
	err = bucket3.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName3)

	t.Log("Retrieving all bucket names before deletion")
	bucketNames, err := db.GetAllBucketNames()
	if err != nil {
		t.Errorf("Unable to get bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	t.Logf("Deleting Bucket: %s", bucketName1)
	err = bucket1.DeleteBucket()
	if err != nil {
		t.Errorf("Unable to delete bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving all bucket names after deletion")
	bucketNames, err = db.GetAllBucketNames()
	if err != nil {
		t.Errorf("Unable to get bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	if len(bucketNames) != 0 {
		t.Error("Number of buckets in db do not match the expected count after deletion of bucket")
	}
}

func TestDeleteNestedBucket(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName1 := []byte("Bucket1")
	bucket1 := db.Bucket(bucketName1)

	t.Logf("Creating Bucket: %s", bucketName1)
	err = bucket1.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName1)

	bucketName2 := []byte("Bucket1/Bucket2")
	bucket2 := db.Bucket(bucketName2)

	t.Logf("Creating Bucket: %s", bucketName2)
	err = bucket2.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName2)

	bucketName3 := []byte("Bucket1/Bucket2/Bucket3")
	bucket3 := db.Bucket(bucketName3)

	t.Logf("Creating Bucket: %s", bucketName3)
	err = bucket3.CreateBucket()
	if err != nil {
		t.Errorf("Unable to create bucket. Error: %s", err.Error())
	}
	t.Logf("Successfully created Bucket: %s", bucketName3)

	t.Log("Retrieving all bucket names before deletion")
	bucketNames, err := db.GetAllBucketNames()
	if err != nil {
		t.Errorf("Unable to get bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	t.Logf("Deleting Bucket: %s", bucketName3)
	err = bucket3.DeleteBucket()
	if err != nil {
		t.Errorf("Unable to delete bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving all bucket names after deletion")
	bucketNames, err = db.GetAllBucketNames()
	if err != nil {
		t.Errorf("Unable to get bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	bucketsExpected := make([][]byte, 0, 2)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Number of buckets in db do not match the expected count after deletion of sub-bucket")
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
		t.Error("Not all buckets retrieved from db match the ones created")
	}
}

func TestGetRootBucketNamesFromDB(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName1 := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName1)
	bucket1 := db.Bucket(bucketName1)

	key1 := []byte("key1")
	value1 := []byte("value1")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key1, value1, bucketName1)
	err = bucket1.Insert(key1, value1)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	bucketName2 := []byte("Bucket2")
	t.Logf("Creating bucket: %s", bucketName2)
	bucket2 := db.Bucket(bucketName2)

	key2 := []byte("key2")
	value2 := []byte("value2")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key2, value2, bucketName2)
	err = bucket2.Insert(key2, value2)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	bucketName3 := []byte("Bucket1/Bucket3")
	t.Logf("Creating bucket: %s", bucketName3)
	bucket3 := db.Bucket(bucketName3)

	key3 := []byte("key3")
	value3 := []byte("value3")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key3, value3, bucketName3)
	err = bucket3.Insert(key3, value3)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	bucketName4 := []byte("Bucket2/Bucket4")
	t.Logf("Creating bucket: %s", bucketName4)
	bucket4 := db.Bucket(bucketName4)

	key4 := []byte("key4")
	value4 := []byte("value4")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key4, value4, bucketName4)
	err = bucket4.Insert(key4, value4)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving root bucket names")
	bucketNames, err := db.GetRootBucketNames()
	if err != nil {
		t.Errorf("Unable to get root bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	bucketsExpected := make([][]byte, 0, 2)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Number of root buckets in db do not match the expected count")
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
		t.Error("Not all root buckets retrieved from db match the ones created")
	}
}

func TestGetAllBucketNamesFromDB(t *testing.T) {
	t.Log("Creating a new test db")
	db, err := NewTestDB()
	if err != nil {
		t.Errorf("Unable to create the test db. Error: %s", err.Error())
	}
	defer db.Close()
	t.Log("Successfully created a new test db")

	bucketName1 := []byte("Bucket1")
	t.Logf("Creating bucket: %s", bucketName1)
	bucket1 := db.Bucket(bucketName1)

	key1 := []byte("key1")
	value1 := []byte("value1")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key1, value1, bucketName1)
	err = bucket1.Insert(key1, value1)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	bucketName2 := []byte("Bucket1/Bucket2")
	t.Logf("Creating bucket: %s", bucketName2)
	bucket2 := db.Bucket(bucketName2)

	key2 := []byte("key2")
	value2 := []byte("value2")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key2, value2, bucketName2)
	err = bucket2.Insert(key2, value2)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	bucketName3 := []byte("Bucket1/Bucket2/Bucket3")
	t.Logf("Creating bucket: %s", bucketName3)
	bucket3 := db.Bucket(bucketName3)

	key3 := []byte("key3")
	value3 := []byte("value3")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key3, value3, bucketName3)
	err = bucket3.Insert(key3, value3)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	bucketName4 := []byte("Bucket4")
	t.Logf("Creating bucket: %s", bucketName4)
	bucket4 := db.Bucket(bucketName4)

	key4 := []byte("key4")
	value4 := []byte("value4")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key4, value4, bucketName4)
	err = bucket4.Insert(key4, value4)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	bucketName5 := []byte("Bucket4/Bucket5")
	t.Logf("Creating bucket: %s", bucketName5)
	bucket5 := db.Bucket(bucketName5)

	key5 := []byte("key5")
	value5 := []byte("value5")

	t.Logf("Inserting Key: %s, Value: %s in bucket: %s", key5, value5, bucketName5)
	err = bucket5.Insert(key5, value5)
	if err != nil {
		t.Errorf("Unable to insert key/value pair in bucket. Error: %s", err.Error())
	}

	t.Log("Retrieving all bucket names")
	bucketNames, err := db.GetAllBucketNames()
	if err != nil {
		t.Errorf("Unable to get bucket names from db. Error: %s", err.Error())
	}

	for _, bucketName := range bucketNames {
		t.Logf("Found bucket: %s", bucketName)
	}

	bucketsExpected := make([][]byte, 0, 3)
	bucketsExpected = append(bucketsExpected, bucketName1)
	bucketsExpected = append(bucketsExpected, bucketName2)
	bucketsExpected = append(bucketsExpected, bucketName3)
	bucketsExpected = append(bucketsExpected, bucketName4)
	bucketsExpected = append(bucketsExpected, bucketName5)

	if len(bucketNames) != len(bucketsExpected) {
		t.Error("Number of buckets in db do not match the expected count")
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
		t.Error("Not all buckets retrieved from db match the ones created")
	}
}
