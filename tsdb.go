package synthetis

import (
	"bufio"
	"encoding/json"
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bbvtaev/synthetis/internal/entity"
)

const Version = "1.1.0-alpha"

type seriesID uint64

type series struct {
	metric string
	labels map[string]string
	points []entity.Point
}

const numShards = 128

type seriesShard struct {
	mu     sync.RWMutex
	series map[seriesID]*series
}

type DB struct {
	shards [numShards]seriesShard

	walMu  sync.Mutex
	wal    *os.File
	walBuf *bufio.Writer
	path   string

	walCh   chan walRecord
	walDone chan struct{}
}

type walRecord struct {
	Type   string            `json:"type"`
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Points []entity.Point    `json:"points"`
}

func Open(path string) (*DB, error) {
	if path == "" {
		return nil, errors.New("empty path")
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	db := &DB{
		wal:     f,
		walBuf:  bufio.NewWriterSize(f, 1<<20), // 1 MiB
		path:    path,
		walCh:   make(chan walRecord, 4096),
		walDone: make(chan struct{}),
	}

	for i := range db.shards {
		db.shards[i].series = make(map[seriesID]*series)
	}

	if err := db.replayWAL(); err != nil {
		_ = f.Close()
		return nil, err
	}

	go db.walLoop()

	return db, nil
}

func (db *DB) shardFor(id seriesID) *seriesShard {
	return &db.shards[uint64(id)%numShards]
}

func (db *DB) Close() error {
	db.walMu.Lock()
	if db.walCh != nil {
		close(db.walCh)
	}
	db.walMu.Unlock()

	if db.walDone != nil {
		<-db.walDone
	}

	db.walMu.Lock()
	defer db.walMu.Unlock()

	if db.walBuf != nil {
		_ = db.walBuf.Flush()
	}
	if db.wal == nil {
		return nil
	}

	err := db.wal.Close()
	db.wal = nil
	db.walBuf = nil
	return err
}

func (db *DB) Write(batch []entity.WriteSeries) error {
	if len(batch) == 0 {
		return nil
	}

	for _, s := range batch {
		if len(s.Points) == 0 {
			continue
		}

		labelsCopy := cloneLabels(s.Labels)

		pointsCopy := make([]entity.Point, len(s.Points))
		copy(pointsCopy, s.Points)

		rec := walRecord{
			Type:   "write",
			Metric: s.Metric,
			Labels: labelsCopy,
			Points: pointsCopy,
		}

		db.walCh <- rec

		id := hashSeries(s.Metric, labelsCopy)
		sh := db.shardFor(id)

		sh.mu.Lock()
		ser, ok := sh.series[id]
		if !ok {
			ser = &series{
				metric: s.Metric,
				labels: labelsCopy,
				points: make([]entity.Point, 0, len(s.Points)),
			}
			sh.series[id] = ser
		}
		for _, p := range s.Points {
			insertPointSorted(&ser.points, p)
		}
		sh.mu.Unlock()
	}

	return nil
}

func (db *DB) Query(opts entity.QueryOptions) ([]entity.SeriesResult, error) {
	if opts.Metric == "" {
		return nil, errors.New("metric is required")
	}
	if opts.From > opts.To {
		return nil, errors.New("from > to")
	}

	var res []entity.SeriesResult

	for i := range db.shards {
		sh := &db.shards[i]
		sh.mu.RLock()
		for _, s := range sh.series {
			if s.metric != opts.Metric {
				continue
			}
			if !labelsMatch(opts.Labels, s.labels) {
				continue
			}

			points := filterPointsByTime(s.points, opts.From, opts.To)
			if len(points) == 0 {
				continue
			}

			res = append(res, entity.SeriesResult{
				Metric: s.metric,
				Labels: cloneLabels(s.labels),
				Points: points,
			})
		}
		sh.mu.RUnlock()
	}

	return res, nil
}

func (db *DB) walLoop() {
	defer close(db.walDone)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case rec, ok := <-db.walCh:
			if !ok {
				db.flushWAL()
				return
			}
			db.writeWALRecord(rec)
		case <-ticker.C:
			db.flushWAL()
		}
	}
}

func (db *DB) writeWALRecord(rec walRecord) {
	db.walMu.Lock()
	defer db.walMu.Unlock()

	if db.wal == nil || db.walBuf == nil {
		return
	}

	b, err := json.Marshal(rec)
	if err != nil {
		return
	}

	_, _ = db.walBuf.Write(b)
	_ = db.walBuf.WriteByte('\n')
}

func (db *DB) flushWAL() {
	db.walMu.Lock()
	defer db.walMu.Unlock()

	if db.walBuf != nil {
		_ = db.walBuf.Flush()
	}
	if db.wal != nil {
		_ = db.wal.Sync()
	}
}

func (db *DB) replayWAL() error {
	f, err := os.Open(db.path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return err
		}
		if rec.Type != "write" {
			continue
		}
		db.applyRecord(rec)
	}
	return scanner.Err()
}

func (db *DB) applyRecord(rec walRecord) {
	id := hashSeries(rec.Metric, rec.Labels)
	sh := db.shardFor(id)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	ser, ok := sh.series[id]
	if !ok {
		ser = &series{
			metric: rec.Metric,
			labels: cloneLabels(rec.Labels),
			points: make([]entity.Point, 0, len(rec.Points)),
		}
		sh.series[id] = ser
	}

	for _, p := range rec.Points {
		insertPointSorted(&ser.points, p)
	}
}

func hashSeries(metric string, labels map[string]string) seriesID {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := fnv.New64a()
	_, _ = h.Write([]byte(metric))
	_, _ = h.Write([]byte{0})

	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte("="))
		_, _ = h.Write([]byte(labels[k]))
		_, _ = h.Write([]byte{0})
	}

	return seriesID(h.Sum64())
}

func labelsMatch(filter, actual map[string]string) bool {
	if len(filter) == 0 {
		return true
	}
	for k, v := range filter {
		if actual[k] != v {
			return false
		}
	}
	return true
}

func insertPointSorted(points *[]entity.Point, p entity.Point) {
	ps := *points
	n := len(ps)

	if n == 0 || ps[n-1].Timestamp <= p.Timestamp {
		*points = append(ps, p)
		return
	}

	i := sort.Search(n, func(i int) bool {
		return ps[i].Timestamp >= p.Timestamp
	})

	if i == n {
		*points = append(ps, p)
		return
	}

	ps = append(ps, entity.Point{})
	copy(ps[i+1:], ps[i:])
	ps[i] = p
	*points = ps
}

func filterPointsByTime(points []entity.Point, from, to int64) []entity.Point {
	if len(points) == 0 {
		return nil
	}

	start := sort.Search(len(points), func(i int) bool {
		return points[i].Timestamp >= from
	})
	if start == len(points) {
		return nil
	}

	end := start
	for end < len(points) && points[end].Timestamp <= to {
		end++
	}
	if end <= start {
		return nil
	}

	out := make([]entity.Point, end-start)
	copy(out, points[start:end])
	return out
}

func cloneLabels(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
