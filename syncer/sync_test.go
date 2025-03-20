package syncer_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/WqyJh/consulsync/syncer"
	"github.com/docker/go-connections/nat"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/consul"
)

type ConsulContainer struct {
	container testcontainers.Container
}

func (c *ConsulContainer) Stop() error {
	return testcontainers.TerminateContainer(c.container)
}

func setupConsul(ctx context.Context) (*ConsulContainer, error) {
	consulContainer, err := consul.Run(ctx, "hashicorp/consul:latest")
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %s", err)
	}
	return &ConsulContainer{container: consulContainer}, nil
}

func TestSync(t *testing.T) {
	ctx := context.Background()
	consulContainer, err := setupConsul(ctx)
	require.NoError(t, err)
	defer consulContainer.Stop()

	consulAddr, err := consulContainer.container.PortEndpoint(ctx, nat.Port("8500"), "http")
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		sync := syncer.NewSyncer(consulAddr, "", "/", "../testdata/dir1", 3)
		err = sync.Sync()
		require.NoError(t, err)

		client, err := api.NewClient(&api.Config{
			Address: consulAddr,
		})
		require.NoError(t, err)

		kv := client.KV()
		pairs, _, err := kv.List("/", nil)
		require.NoError(t, err)

		require.Equal(t, 3, len(pairs))
		for _, pair := range pairs {
			filePath := filepath.Join("../testdata/dir1", pair.Key)
			content, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, string(content), string(pair.Value))
		}

		sync = syncer.NewSyncer(consulAddr, "", "/", "../testdata/dir2", 3)
		err = sync.Sync()
		require.NoError(t, err)

		pairs, _, err = kv.List("/", nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(pairs))

		for _, pair := range pairs {
			filePath := filepath.Join("../testdata/dir2", pair.Key)
			content, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, string(content), string(pair.Value))
		}

		sync = syncer.NewSyncer(consulAddr, "", "/", "../testdata/dir3", 3)
		err = sync.Sync()
		require.NoError(t, err)

		pairs, _, err = kv.List("/", nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(pairs))
		require.Equal(t, "config_3.ini", pairs[0].Key)

		content, err := os.ReadFile("../testdata/dir3/config_3.ini")
		require.NoError(t, err)
		require.Equal(t, string(content), string(pairs[0].Value))
	}
}

func TestSyncConsulPath(t *testing.T) {
	ctx := context.Background()
	consulContainer, err := setupConsul(ctx)
	require.NoError(t, err)
	defer consulContainer.Stop()

	consulAddr, err := consulContainer.container.PortEndpoint(ctx, nat.Port("8500"), "http")
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		consulPath := fmt.Sprintf("test%d", i)

		sync := syncer.NewSyncer(consulAddr, "", consulPath, "../testdata/dir1", 3)
		err = sync.Sync()
		require.NoError(t, err)

		client, err := api.NewClient(&api.Config{
			Address: consulAddr,
		})
		require.NoError(t, err)

		kv := client.KV()
		pairs, _, err := kv.List(consulPath, nil)
		require.NoError(t, err)

		require.Equal(t, 3, len(pairs))
		for _, pair := range pairs {
			key := strings.TrimPrefix(pair.Key, consulPath)
			filePath := filepath.Join("../testdata/dir1", key)
			content, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, string(content), string(pair.Value))
		}

		sync = syncer.NewSyncer(consulAddr, "", consulPath, "../testdata/dir2", 3)
		err = sync.Sync()
		require.NoError(t, err)

		pairs, _, err = kv.List(consulPath, nil)
		require.NoError(t, err)
		require.Equal(t, 2, len(pairs))

		for _, pair := range pairs {
			key := strings.TrimPrefix(pair.Key, consulPath)
			filePath := filepath.Join("../testdata/dir2", key)
			content, err := os.ReadFile(filePath)
			require.NoError(t, err)
			require.Equal(t, string(content), string(pair.Value))
		}

		sync = syncer.NewSyncer(consulAddr, "", consulPath, "../testdata/dir3", 3)
		err = sync.Sync()
		require.NoError(t, err)

		pairs, _, err = kv.List(consulPath, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(pairs))
		require.Equal(t, consulPath+"/config_3.ini", pairs[0].Key)

		content, err := os.ReadFile("../testdata/dir3/config_3.ini")
		require.NoError(t, err)
		require.Equal(t, string(content), string(pairs[0].Value))
	}
}
