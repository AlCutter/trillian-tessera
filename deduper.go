package tessera

import (
	"context"
	"time"

	"github.com/globocom/go-buffer"
	"k8s.io/klog/v2"
)

type DeduperStorage interface {
	Set(context.Context, []DedupEntry) error
	Index(context.Context, []byte) (*uint64, error)
}

type Deduper struct {
	ctx     context.Context
	storage DeduperStorage

	buf *buffer.Buffer
}

func NewDeduper(ctx context.Context, s DeduperStorage) *Deduper {
	r := &Deduper{
		ctx:     ctx,
		storage: s,
	}

	r.buf = buffer.New(
		buffer.WithSize(64),
		buffer.WithFlushInterval(200*time.Millisecond),
		buffer.WithFlusher(buffer.FlusherFunc(r.flush)),
	)
	return r
}

type DedupEntry struct {
	ID  []byte
	Idx uint64
}

func (s *Deduper) Index(ctx context.Context, h []byte) (*uint64, error) {
	return s.storage.Index(ctx, h)
}

func (s *Deduper) Set(_ context.Context, h []byte, idx uint64) error {
	return s.buf.Push(DedupEntry{ID: h, Idx: idx})
}

func (s *Deduper) flush(items []interface{}) {
	ctx, c := context.WithTimeout(s.ctx, 5*time.Second)
	defer c()

	entries := make([]DedupEntry, len(items))
	for i := range items {
		entries[i] = items[i].(DedupEntry)
	}

	if err := s.storage.Set(ctx, entries); err != nil {
		klog.Infof("Failed to flush dedup entries: %v", err)
	}
}
