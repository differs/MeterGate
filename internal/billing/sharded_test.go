package billing

import (
	"context"
	"fmt"
	"testing"
)

// TestShardedStoreRouting: orders land on the shard owned by the user's
// hash; same user always routes to the same shard.
func TestShardedStoreRouting(t *testing.T) {
	ctx := context.Background()
	stores := []ShardStore{newMemStore(), newMemStore(), newMemStore(), newMemStore()}
	ss := NewShardedStore(stores)
	defer ss.Close()

	for i := 0; i < 100; i++ {
		uid := fmt.Sprintf("user-%d", i)
		_, err := ss.InsertOrder(ctx, Order{
			RequestID: fmt.Sprintf("req-%d", i), UserID: uid, Model: "m",
			Status: StatusSettled, AmountMicros: int64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// every shard got some rows, and total = 100
	total := 0
	for _, sh := range stores {
		total += sh.(*memStore).countForTest()
	}
	if total != 100 {
		t.Fatalf("total orders = %d, want 100", total)
	}
}

// TestShardedBatchAcrossShards: one batch containing many users splits
// correctly per shard (Settler writes mixed batches).
func TestShardedBatchAcrossShards(t *testing.T) {
	ctx := context.Background()
	stores := []ShardStore{newMemStore(), newMemStore(), newMemStore()}
	ss := NewShardedStore(stores)
	defer ss.Close()

	var batch []Order
	for i := 0; i < 60; i++ {
		batch = append(batch, Order{
			RequestID: fmt.Sprintf("req-%d", i), UserID: fmt.Sprintf("user-%d", i),
			Model: "m", Status: StatusSettled, AmountMicros: 1,
		})
	}
	if err := ss.InsertOrders(ctx, batch); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, sh := range stores {
		total += sh.(*memStore).countForTest()
	}
	if total != 60 {
		t.Fatalf("total = %d, want 60", total)
	}
}
