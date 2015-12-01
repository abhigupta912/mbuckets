/*
Package mbuckets provides a thin wrapper over Bolt DB.
It allows easy operations on multi-level (nested) buckets.

See https://github.com/boltdb/bolt for Bolt DB.
*/
package mbuckets

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

// DB embeds a bolt.DB
type DB struct {
	*bolt.DB
}

// Open creates/opens a bolt.DB at specified path, and returns a DB enclosing the same
func Open(path string) (*DB, error) {
	database, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	return &DB{database}, nil
}

// OpenWith creates/opens a bolt.DB at specified path with given permissions and options, and returns a DB enclosing the same
func OpenWith(path string, mode os.FileMode, options *bolt.Options) (*DB, error) {
	if options == nil {
		options = &bolt.Options{Timeout: 1 * time.Second}
	}

	database, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}

	return &DB{database}, nil
}

// Close closes the embedded bolt.DB
func (db *DB) Close() error {
	return db.DB.Close()
}

// Map applies read only function `fn` on all the top level buckets in this DB
func (db *DB) Map(fn func([]byte, *bolt.Bucket) error) error {
	return db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(fn)
	})
}

// GetRootBucketNames returns all the top level bolt.Bucket names in this DB
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

// GetAllBucketNames recursively finds and returns all the bolt.Bucket names in this DB
func (db *DB) GetAllBucketNames() ([][]byte, error) {
	bucketNames, err := db.GetRootBucketNames()
	if err != nil {
		return nil, err
	}

	var allBucketNames [][]byte

	for _, bucketName := range bucketNames {
		bucket := db.Bucket(bucketName)
		allBucketNames = append(allBucketNames, bucketName)

		subBucketNames, err := bucket.GetAllBucketNames()
		if err != nil {
			return allBucketNames, err
		}
		allBucketNames = append(allBucketNames, subBucketNames...)
	}

	return allBucketNames, nil
}

// GetAllBucketNamesWithSeparator recursively finds and returns all the bolt.Bucket names in this DB using specified separator
func (db *DB) GetAllBucketNamesWithSeparator(separator []byte) ([][]byte, error) {
	bucketNames, err := db.GetRootBucketNames()
	if err != nil {
		return nil, err
	}

	var allBucketNames [][]byte

	for _, bucketName := range bucketNames {
		bucket := db.Bucket(bucketName).WithSeparator(separator)
		allBucketNames = append(allBucketNames, bucketName)

		subBucketNames, err := bucket.GetAllBucketNames()
		if err != nil {
			return allBucketNames, err
		}
		allBucketNames = append(allBucketNames, subBucketNames...)
	}

	return allBucketNames, nil
}

// Bucket represents a logical entity used to access a bolt.Bucket inside a DB
type Bucket struct {
	DB *DB

	// Complete hierarchial name of the Bucket
	Name []byte

	// The Bucket Name separator
	Separator []byte
}

// Bucket returns a pointer to a Bucket in this DB
func (db *DB) Bucket(name []byte) *Bucket {
	return &Bucket{db, name, []byte("/")}
}

// BucketString is a convenience wrapper over Bucket for string name
func (db *DB) BucketString(name string) *Bucket {
	return db.Bucket([]byte(name))
}

// WithSeparator overrides the separator for this Bucket with the given separator and returns a pointer to this Bucket.
//
// The name of this Bucket and all Buckets under it must be separated by the given custom separator.
// Note that it is an error to mix different separators and can lead to unexpected behavior.
func (b *Bucket) WithSeparator(separator []byte) *Bucket {
	b.Separator = separator
	return b
}

// Update performs an update operation specified by function `fn` on this Bucket
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

// View performs a view operation specified by function `fn` on this Bucket
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

// CreateBucket cretes the bolt.Bucket specified by this Bucket
func (b *Bucket) CreateBucket() error {
	return b.Update(func(*bolt.Bucket, *bolt.Tx) error {
		return nil
	})
}

// DeleteBucket deletes the bolt.Bucket specified by this Bucket
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

// Map performs a view operation specified by function `fn` on all key value pairs in this Bucket
func (b *Bucket) Map(fn func([]byte, []byte) error) error {
	return b.View(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		return bucket.ForEach(fn)
	})
}

// MapPrefix performs a view operation specified by function `fn` on all key value pairs in this Bucket with the given prefix
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

// MapRange performs a view operation specified by function `fn` on all key value pairs in this Bucket within the given range
func (b *Bucket) MapRange(min, max []byte, fn func([]byte, []byte) error) error {
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

// Item represents a holder for a key value pair
type Item struct {
	Key   []byte
	Value []byte
}

// Insert puts a single key/value pair in the bolt.Bucket specified by this Bucket
func (b *Bucket) Insert(key, value []byte) error {
	return b.Update(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		return bucket.Put(key, value)
	})
}

// InsertString is a convenience wrapper over Insert for string key value pair
func (b *Bucket) InsertString(key, value string) error {
	return b.Insert([]byte(key), []byte(value))
}

// InsertAll puts multiple key/value pairs in the bolt.Bucket specified by this Bucket
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

// InsertAllString is a convenience method to Insert string key value pairs
func (b *Bucket) InsertAllString(items map[string]string) error {
	return b.Update(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		for key, value := range items {
			err := bucket.Put([]byte(key), []byte(value))
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Get retrieves the value for given a key from the bolt.Bucket specified by this Bucket
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

// GetString is a convenience wrapper over Get for string key value pair
func (b *Bucket) GetString(key string) (value string, err error) {
	err = b.View(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		v := bucket.Get([]byte(key))
		if v == nil {
			return fmt.Errorf("Key not found: %s", key)
		}

		value = string(v)
		return nil
	})

	return value, err
}

// GetAll retrieves all the key/value pairs from the bolt.Bucket specified by this Bucket
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

// GetAllString is a convenience method to GetAll string key value pairs
func (b *Bucket) GetAllString() (map[string]string, error) {
	items := make(map[string]string)
	err := b.Map(func(k, v []byte) error {
		if v != nil {
			items[string(k)] = string(v)
		}
		return nil
	})

	return items, err
}

// GetPrefix retrieves all the key/value pairs from the bolt.Bucket specified by this Bucket with the given prefix
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

// GetPrefixString is a convenience method to GetPrefix for string key value pairs
func (b *Bucket) GetPrefixString(prefix string) (map[string]string, error) {
	items := make(map[string]string)
	err := b.MapPrefix([]byte(prefix), func(k, v []byte) error {
		if v != nil {
			items[string(k)] = string(v)
		}
		return nil
	})

	return items, err
}

// GetRange retrieves all the key/value pairs from the bolt.Bucket specified by this Bucket within the given range
func (b *Bucket) GetRange(min, max []byte) ([]Item, error) {
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

// GetRangeString is a convenience method to GetRange for string key value pairs
func (b *Bucket) GetRangeString(min, max string) (map[string]string, error) {
	items := make(map[string]string)
	err := b.MapRange([]byte(min), []byte(max), func(k, v []byte) error {
		if v != nil {
			items[string(k)] = string(v)
		}

		return nil
	})

	return items, err
}

// Delete removes the given key from the bolt.Bucket specified by this Bucket
func (b *Bucket) Delete(key []byte) error {
	return b.Update(func(bucket *bolt.Bucket, tx *bolt.Tx) error {
		return bucket.Delete(key)
	})
}

// DeleteString is a convenience wrapper over Delete for string key
func (b *Bucket) DeleteString(key string) error {
	return b.Delete([]byte(key))
}

// GetRootBucketNames returns all the top level bolt.Bucket names under the bolt.Bucket specified by this Bucket
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

// GetAllBucketNames recursively finds and returns all the bolt.Bucket names under the bolt.Bucket specified by this Bucket
func (b *Bucket) GetAllBucketNames() ([][]byte, error) {
	var allBucketNames [][]byte

	bucketNames, err := b.GetRootBucketNames()
	if err != nil {
		return nil, err
	}

	numBucketsToProcess := len(bucketNames)
	var bucketName []byte

	for numBucketsToProcess > 0 {
		bucketName, bucketNames = bucketNames[0], bucketNames[1:]
		numBucketsToProcess--
		allBucketNames = append(allBucketNames, bucketName)

		bucket := b.DB.Bucket(bucketName).WithSeparator(b.Separator)
		subBucketNames, err := bucket.GetRootBucketNames()
		if err != nil {
			return allBucketNames, err
		}

		if subBucketNames != nil && len(subBucketNames) > 0 {
			numBucketsToProcess = numBucketsToProcess + len(subBucketNames)
			bucketNames = append(bucketNames, subBucketNames...)
		}
	}

	return allBucketNames, nil
}
