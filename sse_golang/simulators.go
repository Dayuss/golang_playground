package main

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// startStocks simulates a random-walk stock price every 2s.
func startStocks(ctx context.Context, b *Broker) {
	go func() {
		price := 100 + rand.Float64()*50
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				price = max(1, price+(rand.Float64()-0.5)*3)
				b.Publish("stocks", "price", map[string]any{
					"symbol": "GOGL",
					"price":  math.Round(price*100) / 100,
				})
			}
		}
	}()
}

// startMetrics fakes system metrics every 5s.
func startMetrics(ctx context.Context, b *Broker) {
	go func() {
		start := time.Now()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.Publish("metrics", "metrics", map[string]any{
					"cpu_percent": math.Round(rand.Float64()*850) / 10,
					"mem_percent": 40 + math.Round(rand.Float64()*400)/10,
					"uptime_secs": int(time.Since(start).Seconds()),
				})
			}
		}
	}()
}

// startNotifications spouts a fake ops notification every ~8s.
func startNotifications(ctx context.Context, b *Broker) {
	messages := []string{
		"deploy finished", "new user signed up", "backup completed",
		"payment received", "cache invalidated", "cron job finished",
	}
	go func() {
		t := time.NewTicker(8 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.Publish("notifications", "notification", map[string]any{
					"level":   []string{"info", "info", "warn"}[rand.IntN(3)],
					"message": messages[rand.IntN(len(messages))],
				})
			}
		}
	}()
}
