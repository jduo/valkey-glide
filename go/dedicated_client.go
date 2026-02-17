package glide

import (
	"context"
)

// DedicatedClient wraps a Client with a dedicated connection handle
// for isolated command execution (e.g., WATCH/transactions)
type DedicatedClient struct {
	client *baseClient
	handle uint64
}

// UsingDedicated creates a new DedicatedClient with isolated connections.
// The caller must call Close() when done to return connections to the pool.
//
// Example:
//
//	dedicatedClient := client.UsingDedicated(ctx)
//	defer dedicatedClient.Close(ctx)
//
//	// WATCH keys for CAS transaction
//	dedicatedClient.Watch(ctx, []string{"key1", "key2"})
//	value := dedicatedClient.Get(ctx, "key1")
//	// ... business logic ...
//	result := dedicatedClient.Exec(ctx, transaction)
func (client *baseClient) UsingDedicated(ctx context.Context) (*DedicatedClient, error) {
	handle, err := client.acquireDedicatedHandle(ctx)
	if err != nil {
		return nil, err
	}
	
	return &DedicatedClient{
		client: client,
		handle: handle,
	}, nil
}

// Close releases the dedicated connection handle back to the pool
func (dc *DedicatedClient) Close(ctx context.Context) error {
	return dc.client.releaseDedicatedHandle(ctx, dc.handle)
}

// Watch marks keys for conditional transaction execution
func (dc *DedicatedClient) Watch(ctx context.Context, keys []string) (string, error) {
	return dc.client.watchWithHandle(ctx, keys, dc.handle)
}

// Get retrieves a value using the dedicated connection
func (dc *DedicatedClient) Get(ctx context.Context, key string) (string, error) {
	return dc.client.getWithHandle(ctx, key, dc.handle)
}

// Set stores a value using the dedicated connection
func (dc *DedicatedClient) Set(ctx context.Context, key string, value string) (string, error) {
	return dc.client.setWithHandle(ctx, key, value, dc.handle)
}

// Exec executes a batch/transaction using the dedicated connection
func (dc *DedicatedClient) Exec(ctx context.Context, batch Batch) ([]interface{}, error) {
	return dc.client.execWithHandle(ctx, batch, dc.handle)
}

// Unwatch clears all watched keys
func (dc *DedicatedClient) Unwatch(ctx context.Context) (string, error) {
	return dc.client.unwatchWithHandle(ctx, dc.handle)
}

// Internal methods to be implemented in base_client.go:
// - acquireDedicatedHandle(ctx) (uint64, error)
// - releaseDedicatedHandle(ctx, handle uint64) error
// - watchWithHandle(ctx, keys []string, handle uint64) (string, error)
// - getWithHandle(ctx, key string, handle uint64) (string, error)
// - setWithHandle(ctx, key, value string, handle uint64) (string, error)
// - execWithHandle(ctx, batch Batch, handle uint64) ([]interface{}, error)
// - unwatchWithHandle(ctx, handle uint64) (string, error)
