package collect

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRemove(t *testing.T) {
	rcache, err := newRequestCache(1)
	assert.NoError(t, err)
	rcache.AddReqItem(context.Background(), "1", &reqItem{})
	w, ok := rcache.GetReqWorker("1")
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	rcache.RemoveReqItem("1")
	assert.True(t, !rcache.cache.Contains("1"), "unexpected item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}

func TestCancel(t *testing.T) {
	rcache, err := newRequestCache(1)
	assert.NoError(t, err)
	cctx, cc := context.WithCancel(context.Background())
	rcache.AddReqItem(cctx, "1", &reqItem{})
	w, ok := rcache.GetReqWorker("1")
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	cc()
	// wait for close
	time.Sleep(1 * time.Millisecond)
	assert.True(t, !rcache.cache.Contains("1"), "unexpected item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}

func TestEvict(t *testing.T) {
	rcache, err := newRequestCache(1)
	assert.NoError(t, err)
	rcache.AddReqItem(context.Background(), "1", &reqItem{})
	w, ok := rcache.GetReqWorker("1")
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	rcache.AddReqItem(context.Background(), "2", &reqItem{})
	assert.True(t, !rcache.cache.Contains("1"), "unexpected item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}

func TestSamevict(t *testing.T) {
	rcache, err := newRequestCache(2)
	assert.NoError(t, err)
	rcache.AddReqItem(context.Background(), "1", &reqItem{})
	w, ok := rcache.GetReqWorker("1")
	assert.True(t, ok)
	ori := w.onClose
	close := make(chan struct{})
	w.onClose = func() {
		ori()
		close <- struct{}{}
	}
	rcache.AddReqItem(context.Background(), "1", &reqItem{})
	assert.True(t, rcache.cache.Contains("1"), "unexpected nil item")
	select {
	case <-time.After(2 * time.Second):
		assert.Fail(t, "haven't called onClose")
	case <-close:
	}
}
