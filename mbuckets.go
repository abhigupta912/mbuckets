// Package mbuckets provides a thin wrapper over Bolt DB.
// It allows easy operations on nested buckets.
//
// See https://github.com/boltdb/bolt for Bolt DB.

package mbuckets

import (
	"bytes"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

// Represents an mbuckets DB.
// Embeds a bolt.DB struct.
type DB struct {
	*bolt.DB
}

// Creates/Opens a bolt.DB at specified path,
// and returns an mbuckets DB enclosing the same.
func Open(path string) (*DB, error) {
	database, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	return &DB{database}, nil
}

// Closes the embedded bolt.DB
func (db *DB) Close() error {
	return db.DB.Close()
}

// Perform an operation on all root buckets in the DB
func (db *DB) Map(fn func([]byte, *bolt.Bucket) error) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(fn)
	})
}

// Get root bucket names
func (db *DB) GetRootBucketNames() ([][]byte, error) {
	var bucketNames [][]byte

	err := db.Map(func(name []byte, _ *bolt.Bucket) error {
		bucketName := make([]byte, len(name))
		copy(bucketName, name)
		bucketNames = append(bucketNames, bucketName)
		return nil
	})

	return bucketNames, err
}

// Recursively find all bucket names
func (db *DB) GetAllBucketNames() ([][]byte, error) {
	bucketNames, err := db.GetRootBucketNames()
	if err != nil {
		return nil, err
	}

	var allBucketNames [][]byte

	for _, bucketName := range bucketNames {
		bucket := db.Bucket(bucketName)
		subBucketNames, err := bucket.GetAllBucketNames()
		if err != nil {
			return allBucketNames, err
		}
		allBucketNames = append(allBucketNames, subBucketNames...)
	}

	return allBucketNames, nil
}

// Represents an mbuckets Bucket.
type Bucket struct {
	DB *DB

	// Complete hierarchial name of the bucket.
	// Names for buckets contained within other buckets
	// should begin with the top level bucket name and
	// contain all bucket names in hierarchial order
	// separated by the Separator till the desired bucket.
	// Names should not begin with Separator.
	Name []byte

	// The bucket name separator
	Separator []byte
}

// Returns an mbuckets Bucket
func (db *DB) Bucket(name []byte) *Bucket {
	return &Bucket{db, name, []byte("/")}
}

// Returns an mbuckets Bucket with custom seperator
func (db *DB) BucketWithSeparator(name []byte, seperator []byte) *Bucket {
	return &Bucket{db, name, seperator}
}

// Perform an Update operation on the mbuckets Bucket
func (b *Bucket) Update(fn func(*bolt.Bucket, *bolt.Tx) error) error {
	buckets := bytes.Split(b.Name, b.Separator)

	return b.DB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(buckets[0])
		if err != nil {
			return err
		}

		if len(buckets) > 1 {
			for idx, bucketName := range buckets {
				if idx == 0 {
					continue
				}

				subBucket, err := bucket.CreateBucketIfNotExists(bucketName)
				if err != nil {
					return err
				}

				bucket = subBucket
			}
		}

		return fn(bucket, tx)
	})
}

// Perform a View operation on the mbuckets Bucket
func (b *Bucket) View(fn func(*bolt.Bucket, *bolt.Tx) error) error {
	buckets := bytes.Split(b.Name, b.Separator)

	return b.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(buckets[0])
		if bucket == nil {
			return fmt.Errorf("Bucket not found: %s", b.Name)
		}

		if len(buckets) > 1 {
			for idx, bucketName := range buckets {
				if idx == 0 {
					continue
				}

				subBucket := bucket.Bucket(bucketName)
				if subBucket == nil {
					return fmt.Errorf("Bucket not found: %s", b.Name)
				}

				bucket = subBucket
			}
		}

		return fn(bucket, tx)
	})
}

// Delete this mbuckets Bucket
func (b *Bucket) DeleteBucket() error {
	buckets := bytes.Split(b.Name, b.Separator)

	return b.DB.Update(func(tx *bolt.Tx) error {
		if len(buckets) == 1 {
			return tx.DeleteBucket(buckets[0])
		}

		bucket := tx.Bucket(buckets[0])
		if bucket == nil {
			return fmt.Errorf("Bucket not found: %s", b.Name)
		}

		for idx, bucketName := range buckets {
			if idx == 0 {
				continue
			}

			if idx == len(buckets)-1 {
				return bucket.DeleteBucket(buckets[0])
			}

			subBucket := bucket.Bucket(bucketName)
			if subBucket == nil {
				return fmt.Errorf("Bucket not found: %s", b.Name)
			}

			bucket = subBucket
		}

		return nil
	})
}

// Perform an operation on all key value pairs from mbuckets Bucket
// The operation is defined as a function which takes in a key and value
func (b *Bucket) Map(fn func([]byte, []byte) error) error {
	return b.View(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		return bucket.ForEach(fn)
	})
}

// Perform an operation on all key value pairs from mbuckets Bucket with given prefix
// The operation is defined as a function which takes in a key and value
func (b *Bucket) MapPrefix(prefix []byte, fn func([]byte, []byte) error) error {
	return b.View(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			err := fn(k, v)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Perform an operation on all key value pairs from mbuckets Bucket within given range
// The operation is defined as a function which takes in a key and value
func (b *Bucket) MapRange(min []byte, max []byte, fn func([]byte, []byte) error) error {
	return b.View(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		cursor := bucket.Cursor()

		for k, v := cursor.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = cursor.Next() {
			err := fn(k, v)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Holder for key value pair
type Item struct {
	Key   []byte
	Value []byte
}

// Insert a single key/value in a mbuckets Bucket
func (b *Bucket) Insert(key, value []byte) error {
	return b.Update(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		return bucket.Put(key, value)
	})
}

// Insert multiple key/value pairs in a mbuckets Bucket
func (b *Bucket) InsertAll(items []Item) error {
	return b.Update(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		for _, item := range items {
			err := bucket.Put(item.Key, item.Value)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Retrieve value for given key from mbuckets Bucket
func (b *Bucket) Get(key []byte) (value []byte, err error) {
	err = b.View(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		v := bucket.Get(key)
		if v == nil {
			return fmt.Errorf("Key not found: %s", key)
		}

		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})

	return value, err
}

// Retrieve all key value pairs from mbuckets Bucket
func (b *Bucket) GetAll() ([]Item, error) {
	var items []Item
	err := b.Map(func(k, v []byte) error {
		if v != nil {
			key := make([]byte, len(k))
			copy(key, k)
			value := make([]byte, len(v))
			copy(value, v)
			items = append(items, Item{key, value})
		}
		return nil
	})

	return items, err
}

// Retrieve all key value pairs from mbuckets Bucket with given prefix
func (b *Bucket) GetPrefix(prefix []byte) ([]Item, error) {
	var items []Item
	err := b.MapPrefix(prefix, func(k, v []byte) error {
		if v != nil {
			key := make([]byte, len(k))
			copy(key, k)
			value := make([]byte, len(v))
			copy(value, v)
			items = append(items, Item{key, value})
		}
		return nil
	})

	return items, err
}

// Retrieve all key value pairs from mbuckets Bucket within given range
func (b *Bucket) GetRange(min []byte, max []byte) ([]Item, error) {
	var items []Item
	err := b.MapRange(min, max, func(k, v []byte) error {
		if v != nil {
			key := make([]byte, len(k))
			copy(key, k)
			value := make([]byte, len(v))
			copy(value, v)
			items = append(items, Item{key, value})
		}

		return nil
	})

	return items, err
}

// Delete given key from mbuckets Bucket
func (b *Bucket) Delete(key []byte) error {
	return b.Update(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		return bucket.Delete(key)
	})
}

// Get names of Buckets directly under this mbuckets Bucket
func (b *Bucket) GetRootBucketNames() ([][]byte, error) {
	var names [][]byte

	err := b.Map(func(key []byte, value []byte) error {
		if value == nil {
			name := make([]byte, len(key))
			copy(name, key)
			names = append(names, name)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	bucketNames := make([][]byte, 0, len(names))
	for _, name := range names {
		bucketName := bytes.NewBuffer(b.Name)
		bucketName.Write(b.Separator)
		bucketName.Write(name)
		bucketNames = append(bucketNames, bucketName.Bytes())
	}

	return bucketNames, nil
}

// Recursively find all bucket names under this mbuckets Bucket
func (b *Bucket) GetAllBucketNames() ([][]byte, error) {
	bucketNames, err := b.GetRootBucketNames()
	if err != nil {
		return nil, err
	}

	numBucketsToProcess := len(bucketNames)
	var bucketName []byte

	for numBucketsToProcess > 0 {
		bucketName, bucketNames = bucketNames[0], bucketNames[1:]
		numBucketsToProcess--

		bucket := b.DB.BucketWithSeparator(bucketName, b.Separator)
		subBucketNames, err := bucket.GetRootBucketNames()
		if err != nil {
			return bucketNames, err
		}

		if subBucketNames != nil && len(subBucketNames) > 0 {
			numBucketsToProcess = numBucketsToProcess + len(subBucketNames)
			bucketNames = append(bucketNames, subBucketNames...)
		}
	}

	return bucketNames, nil
}
