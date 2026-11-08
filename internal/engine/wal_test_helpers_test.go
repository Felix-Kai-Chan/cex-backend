package engine

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

func decodeWALForTest(raw string, entry *WALEntry) error {
	return json.Unmarshal([]byte(raw), entry)
}

func overwriteWALForTest(rdb *redis.Client, key string, index int64, entry WALEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return rdb.LSet(context.Background(), key, index, string(data)).Err()
}
