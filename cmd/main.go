package main

import (
	"fmt"
	"log/slog"
	"time"

	synthetis "github.com/bbvtaev/synthetis"
	"github.com/bbvtaev/synthetis/internal/entity"
)

// In main func we have nothing to initialize,
// but we should create test user that writes,
// reads and does stuff. That will be localed
// here, in main.

func main() {
	start := time.Now()

	db, err := synthetis.Open("./data/mtx.wal")
	if err != nil {
		slog.Info("Error occured", "err", err)
		return
	}
	defer db.Close()

	for i := 0; i < 100000; i++ {
		if err = db.Write("some_mtx", map[string]string{"host": "a"}, i); err != nil {
			slog.Info("Error occured", "err", err)
			return
		}
	}

	res, err := db.Query(entity.QueryOptions{
		Metric: "some_mtx",
		Labels: map[string]string{"host": "a"},
		From:   1,
		To:     2,
	})
	if err != nil {
		slog.Info("Error occured", "err", err)
		return
	}

	fmt.Println(res, time.Since(start))
}
