package syncer_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/WqyJh/consulsync/syncer"
	"github.com/docker/go-connections/nat"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
)

func TestFetch(t *testing.T) {
	ctx := context.Background()
	consulContainer, err := setupConsul(ctx)
	require.NoError(t, err)
	defer consulContainer.Stop()

	consulAddr, err := consulContainer.container.PortEndpoint(ctx, nat.Port("8500"), "http")
	require.NoError(t, err)

	// Create a temporary directory for test files
	tmpDir := t.TempDir()
	t.Logf("tmpDir: %s", tmpDir)

	// Setup test data in Consul
	client, err := api.NewClient(&api.Config{
		Address: consulAddr,
	})
	require.NoError(t, err)

	kv := client.KV()
	testData := map[string][]byte{
		"test/config1.ini":        []byte("config1 content"),
		"test/config2.ini":        []byte("config2 content"),
		"test/subdir/config3.ini": []byte("config3 content"),
	}

	for key, value := range testData {
		_, err = kv.Put(&api.KVPair{
			Key:   key,
			Value: value,
		}, nil)
		require.NoError(t, err)
	}

	// Test fetching from root path
	fetcher := syncer.NewFetcher(consulAddr, "", "test", tmpDir)
	err = fetcher.Fetch()
	require.NoError(t, err)

	// Verify files were created correctly
	for key, expectedContent := range testData {
		localPath := filepath.Join(tmpDir, strings.TrimPrefix(key, "test/"))
		content, err := os.ReadFile(localPath)
		require.NoError(t, err)
		require.Equal(t, string(expectedContent), string(content))
	}
}

func TestNestedFetch(t *testing.T) {
	ctx := context.Background()
	consulContainer, err := setupConsul(ctx)
	require.NoError(t, err)
	defer consulContainer.Stop()

	consulAddr, err := consulContainer.container.PortEndpoint(ctx, nat.Port("8500"), "http")
	require.NoError(t, err)

	// Create a temporary directory for test files
	tmpDir := t.TempDir()
	t.Logf("tmpDir: %s", tmpDir)

	// Setup test data in Consul
	client, err := api.NewClient(&api.Config{
		Address: consulAddr,
	})
	require.NoError(t, err)

	kv := client.KV()
	testData := map[string][]byte{
		"test/nested/config1.ini": []byte("config1 content"),
		"test/nested/config2.ini": []byte("config2 content"),
		"test/subdir/config3.ini": []byte("config3 content"),
	}

	for key, value := range testData {
		_, err = kv.Put(&api.KVPair{
			Key:   key,
			Value: value,
		}, nil)
		require.NoError(t, err)
	}

	// Test fetching from root path
	fetcher := syncer.NewFetcher(consulAddr, "", "", tmpDir)
	err = fetcher.Fetch()
	require.NoError(t, err)

	// Verify files were created correctly
	for key, expectedContent := range testData {
		localPath := filepath.Join(tmpDir, key)
		content, err := os.ReadFile(localPath)
		require.NoError(t, err)
		require.Equal(t, string(expectedContent), string(content))
	}
}

func TestFetchWithSubPath(t *testing.T) {
	ctx := context.Background()
	consulContainer, err := setupConsul(ctx)
	require.NoError(t, err)
	defer consulContainer.Stop()

	consulAddr, err := consulContainer.container.PortEndpoint(ctx, nat.Port("8500"), "http")
	require.NoError(t, err)

	// Create a temporary directory for test files
	tmpDir := t.TempDir()

	// Setup test data in Consul
	client, err := api.NewClient(&api.Config{
		Address: consulAddr,
	})
	require.NoError(t, err)

	kv := client.KV()
	testData := map[string][]byte{
		"test/subdir/config1.ini": []byte("config1 content"),
		"test/subdir/config2.ini": []byte("config2 content"),
	}

	for key, value := range testData {
		_, err = kv.Put(&api.KVPair{
			Key:   key,
			Value: value,
		}, nil)
		require.NoError(t, err)
	}

	// Test fetching from subpath
	fetcher := syncer.NewFetcher(consulAddr, "", "test/subdir", tmpDir)
	err = fetcher.Fetch()
	require.NoError(t, err)

	// Verify files were created correctly
	for key, expectedContent := range testData {
		localPath := filepath.Join(tmpDir, strings.TrimPrefix(key, "test/subdir/"))
		content, err := os.ReadFile(localPath)
		require.NoError(t, err)
		require.Equal(t, string(expectedContent), string(content))
	}
}

func TestFetchEmptyPath(t *testing.T) {
	ctx := context.Background()
	consulContainer, err := setupConsul(ctx)
	require.NoError(t, err)
	defer consulContainer.Stop()

	consulAddr, err := consulContainer.container.PortEndpoint(ctx, nat.Port("8500"), "http")
	require.NoError(t, err)

	// Create a temporary directory for test files
	tmpDir := t.TempDir()

	// Test fetching from empty path
	fetcher := syncer.NewFetcher(consulAddr, "", "", tmpDir)
	err = fetcher.Fetch()
	require.NoError(t, err)

	// Verify no files were created
	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}
