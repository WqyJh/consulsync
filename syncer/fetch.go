package syncer

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/hashicorp/consul/api"
)

type Fetcher struct {
	consulAddr  string
	consulToken string
	consulPath  string
	localPath   string
}

func NewFetcher(consulAddr, consulToken, consulPath, localPath string) *Fetcher {
	localPath = path.Clean(localPath)
	consulPath = strings.TrimPrefix(path.Clean(consulPath), "/")
	return &Fetcher{
		consulAddr:  consulAddr,
		consulToken: consulToken,
		consulPath:  consulPath,
		localPath:   localPath,
	}
}

func (f *Fetcher) Fetch() error {
	client, err := api.NewClient(&api.Config{
		Address: f.consulAddr,
		Token:   f.consulToken,
	})
	if err != nil {
		return fmt.Errorf("failed to create consul client: %+v", err)
	}

	kv := client.KV()

	// Fetch KV pairs from consul
	return walkKV(kv, f.consulPath, func(pair *api.KVPair) error {
		localPath := toLocalPath(f.localPath, f.consulPath, pair.Key)

		// Create directory if it doesn't exist
		dir := filepath.Dir(localPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %+v", dir, err)
		}

		// Write file
		if err := os.WriteFile(localPath, pair.Value, 0644); err != nil {
			return fmt.Errorf("failed to write local file %s: %+v", localPath, err)
		}

		log.Printf("[%s] fetched to %s", pair.Key, localPath)
		return nil
	})
}
