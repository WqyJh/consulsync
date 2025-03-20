package syncer

import (
	"bytes"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/hashicorp/consul/api"
)

type Syncer struct {
	consulAddr  string
	consulToken string
	consulPath  string
	localPath   string
	casTry      int
}

func NewSyncer(consulAddr, consulToken, consulPath, localPath string, casTry int) *Syncer {
	localPath = strings.TrimPrefix(path.Clean(localPath), "/")
	consulPath = strings.TrimPrefix(path.Clean(consulPath), "/")
	return &Syncer{
		consulAddr:  consulAddr,
		consulToken: consulToken,
		consulPath:  consulPath,
		localPath:   localPath,
		casTry:      casTry,
	}
}

func (s *Syncer) Sync() error {
	client, err := api.NewClient(&api.Config{
		Address: s.consulAddr,
		Token:   s.consulToken,
	})
	if err != nil {
		return fmt.Errorf("failed to create consul client: %+v", err)
	}

	kv := client.KV()

	// set or update kv
	err = filepath.WalkDir(s.localPath, func(filePath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		content, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed to read local file: %s, %+v", filePath, err)
		}

		consulKey := toConsulKey(s.localPath, filePath, s.consulPath)

		err = setKV(kv, consulKey, content, s.casTry)
		if err != nil {
			return fmt.Errorf("failed to set kv: %s, %+v", consulKey, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to walk local file: %+v", err)
	}

	// delete kv
	err = walkKV(kv, s.consulPath, func(pair *api.KVPair) error {
		localPath := toLocalPath(s.localPath, s.consulPath, pair.Key)
		exists, err := FileExists(localPath)
		if err != nil {
			return fmt.Errorf("failed to check local file: %s, %+v", localPath, err)
		}
		if exists {
			// skip for local file exists
			return nil
		}
		// local file not exists, delete remote file
		for i := 0; i < s.casTry; i++ {
			ok, writeMeta, err := kv.DeleteCAS(pair, nil)
			if err != nil {
				return fmt.Errorf("failed to delete kv: %s, %+v", pair.Key, err)
			}
			if ok {
				log.Printf("[%s] delete success (%+v)", pair.Key, writeMeta.RequestTime)
				return nil
			}
			log.Printf("[%s] delete failed, try %d", pair.Key, i+1)
		}
		return fmt.Errorf("failed to delete %s after %d tries", pair.Key, s.casTry)
	})
	if err != nil {
		return fmt.Errorf("failed to walk consul file: %+v", err)
	}

	return nil
}

func walkKV(kv *api.KV, path string, walkFn func(pair *api.KVPair) error) error {
	response, _, err := kv.List(path, nil)
	if err != nil {
		return err
	}

	for _, pair := range response {
		// Skip directory entries (keys ending with slash)
		if strings.HasSuffix(pair.Key, "/") {
			continue
		}
		if err := walkFn(pair); err != nil {
			return err
		}
	}

	return nil
}

func toConsulKey(prefix, localPath, consulPath string) string {
	relativePath := strings.TrimPrefix(localPath, prefix)
	targetPath := path.Join(consulPath, relativePath)
	targetPath = strings.TrimPrefix(targetPath, "/")
	return targetPath
}

func toLocalPath(localPath, consulPath, key string) string {
	relativePath := strings.TrimPrefix(key, consulPath)
	return path.Join(localPath, relativePath)
}

func setKV(kv *api.KV, key string, value []byte, casTry int) error {
	var i int
RETRY:
	response, _, err := kv.Get(key, nil)
	if err != nil {
		return err
	}

	if response == nil {
		writeMeta, err := kv.Put(&api.KVPair{
			Key:   key,
			Value: value,
		}, nil)
		if err != nil {
			return err
		}
		log.Printf("[%s] create success (%+v)", key, writeMeta.RequestTime)
		return nil
	}

	if bytes.Equal(response.Value, value) {
		log.Printf("[%s] value unchanged", key)
		return nil
	}

	ok, writeMeta, err := kv.CAS(&api.KVPair{
		Key:         key,
		Value:       value,
		ModifyIndex: response.ModifyIndex,
	}, nil)
	if err != nil {
		return err
	}
	if ok {
		log.Printf("[%s] update success (%+v)", key, writeMeta.RequestTime)
		return nil
	}
	i++
	if i < casTry {
		log.Printf("[%s] update failed, try %d", key, i)
		goto RETRY
	}

	return fmt.Errorf("failed to update %s after %d tries", key, casTry)
}

func FileExists(file string) (bool, error) {
	_, err := os.Stat(file)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil // file does not exist
	}
	return false, err // file may or may not exist
}
