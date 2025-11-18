package synthetis_test

import (
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	tsdb "github.com/bbvtaev/synthetis"
	"github.com/bbvtaev/synthetis/internal/entity"
)

func newTestDB(t *testing.T) *tsdb.DB {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.wal")

	db, err := tsdb.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestWriteAndQuerySingleSeries(t *testing.T) {
	db := newTestDB(t)

	err := db.Write([]entity.WriteSeries{
		{
			Metric: "cpu_usage",
			Labels: map[string]string{"host": "a", "dc": "eu"},
			Points: []entity.Point{
				{Timestamp: 1, Value: 0.5},
				{Timestamp: 2, Value: 0.7},
				{Timestamp: 3, Value: 0.9},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	res, err := db.Query(entity.QueryOptions{
		Metric: "cpu_usage",
		Labels: map[string]string{"host": "a"},
		From:   1,
		To:     3,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res))
	}

	series := res[0]
	if series.Metric != "cpu_usage" {
		t.Errorf("expected metric cpu_usage, got %s", series.Metric)
	}
	if series.Labels["host"] != "a" || series.Labels["dc"] != "eu" {
		t.Errorf("unexpected labels: %v", series.Labels)
	}

	if len(series.Points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(series.Points))
	}

	wantTs := []int64{1, 2, 3}
	wantVal := []float64{0.5, 0.7, 0.9}
	for i, p := range series.Points {
		if p.Timestamp != wantTs[i] {
			t.Errorf("point[%d].Timestamp = %d, want %d", i, p.Timestamp, wantTs[i])
		}
		if p.Value != wantVal[i] {
			t.Errorf("point[%d].Value = %f, want %f", i, p.Value, wantVal[i])
		}
	}
}

func TestLabelFiltering(t *testing.T) {
	db := newTestDB(t)

	err := db.Write([]entity.WriteSeries{
		{
			Metric: "cpu_usage",
			Labels: map[string]string{"host": "a"},
			Points: []entity.Point{{Timestamp: 1, Value: 0.1}},
		},
		{
			Metric: "cpu_usage",
			Labels: map[string]string{"host": "b"},
			Points: []entity.Point{{Timestamp: 1, Value: 0.2}},
		},
	})
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	resA, err := db.Query(entity.QueryOptions{
		Metric: "cpu_usage",
		Labels: map[string]string{"host": "a"},
		From:   0,
		To:     10,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resA) != 1 {
		t.Fatalf("expected 1 series for host=a, got %d", len(resA))
	}
	if resA[0].Labels["host"] != "a" {
		t.Errorf("expected host=a, got %s", resA[0].Labels["host"])
	}

	resB, err := db.Query(entity.QueryOptions{
		Metric: "cpu_usage",
		Labels: map[string]string{"host": "b"},
		From:   0,
		To:     10,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resB) != 1 {
		t.Fatalf("expected 1 series for host=b, got %d", len(resB))
	}
	if resB[0].Labels["host"] != "b" {
		t.Errorf("expected host=b, got %s", resB[0].Labels["host"])
	}

	resAll, err := db.Query(entity.QueryOptions{
		Metric: "cpu_usage",
		Labels: nil,
		From:   0,
		To:     10,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(resAll) != 2 {
		t.Fatalf("expected 2 series without label filter, got %d", len(resAll))
	}
}

func TestTimeRangeFiltering(t *testing.T) {
	db := newTestDB(t)

	err := db.Write([]entity.WriteSeries{
		{
			Metric: "temp",
			Labels: map[string]string{"sensor": "s1"},
			Points: []entity.Point{
				{Timestamp: 10, Value: 1.0},
				{Timestamp: 20, Value: 2.0},
				{Timestamp: 30, Value: 3.0},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	res, err := db.Query(entity.QueryOptions{
		Metric: "temp",
		Labels: map[string]string{"sensor": "s1"},
		From:   15,
		To:     25,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected 1 series, got %d", len(res))
	}
	if len(res[0].Points) != 1 {
		t.Fatalf("expected 1 point in range, got %d", len(res[0].Points))
	}
	if res[0].Points[0].Timestamp != 20 {
		t.Errorf("expected point with ts=20, got %d", res[0].Points[0].Timestamp)
	}
}

func TestWALReplay(t *testing.T) {

	dir := t.TempDir()
	path := filepath.Join(dir, "tsdb.wal")

	db, err := tsdb.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	err = db.Write([]entity.WriteSeries{
		{
			Metric: "disk_usage",
			Labels: map[string]string{"host": "a"},
			Points: []entity.Point{
				{Timestamp: 100, Value: 42.0},
			},
		},
	})
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db2, err := tsdb.Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db2.Close()

	res, err := db2.Query(entity.QueryOptions{
		Metric: "disk_usage",
		Labels: map[string]string{"host": "a"},
		From:   0,
		To:     200,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}

	if len(res) != 1 {
		t.Fatalf("expected 1 series after replay, got %d", len(res))
	}
	if len(res[0].Points) != 1 {
		t.Fatalf("expected 1 point after replay, got %d", len(res[0].Points))
	}
	if res[0].Points[0].Value != 42.0 {
		t.Errorf("expected value 42.0 after replay, got %f", res[0].Points[0].Value)
	}
}

func TestQueryValidationErrors(t *testing.T) {
	db := newTestDB(t)

	_, err := db.Query(entity.QueryOptions{
		Metric: "",
		From:   0,
		To:     10,
	})
	if err == nil {
		t.Fatalf("expected error for empty metric, got nil")
	}

	_, err = db.Query(entity.QueryOptions{
		Metric: "cpu",
		From:   10,
		To:     5,
	})
	if err == nil {
		t.Fatalf("expected error for from > to, got nil")
	}
}

func TestWriteEmptyBatch(t *testing.T) {
	db := newTestDB(t)

	if err := db.Write(nil); err != nil {
		t.Fatalf("Write(nil) error = %v", err)
	}
	if err := db.Write([]entity.WriteSeries{}); err != nil {
		t.Fatalf("Write(empty slice) error = %v", err)
	}
}
func newBenchDB(b *testing.B) *tsdb.DB {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "tsdb.wal")

	db, err := tsdb.Open(path)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func BenchmarkWriteThroughput(b *testing.B) {
	db := newBenchDB(b)

	const pointsPerWrite = 8

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	_ = db.Write([]entity.WriteSeries{
		{
			Metric: "warmup",
			Labels: map[string]string{"sensor": "init"},
			Points: []entity.Point{
				{Timestamp: time.Now().Unix(), Value: 0.0},
			},
		},
	})

	b.ResetTimer()
	start := time.Now()
	totalPoints := 0

	for i := 0; i < b.N; i++ {
		tsBase := time.Now().Unix()

		points := make([]entity.Point, 0, pointsPerWrite)
		for j := 0; j < pointsPerWrite; j++ {
			points = append(points, entity.Point{
				Timestamp: tsBase + int64(j),
				Value:     rnd.Float64() * 100,
			})
		}

		err := db.Write([]entity.WriteSeries{
			{
				Metric: "load",
				Labels: map[string]string{"sensor": "A1"},
				Points: points,
			},
		})
		if err != nil {
			b.Fatalf("Write error: %v", err)
		}

		totalPoints += pointsPerWrite
	}

	elapsed := time.Since(start)
	opsPerSec := float64(b.N) / elapsed.Seconds()
	pointsPerSec := float64(totalPoints) / elapsed.Seconds()

	b.ReportMetric(opsPerSec, "ops/s")
	b.ReportMetric(pointsPerSec, "points/s")

	b.Logf("pointsPerWrite=%d, totalOps=%d, totalPoints=%d, elapsed=%s",
		pointsPerWrite, b.N, totalPoints, elapsed)
}
