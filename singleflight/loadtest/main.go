package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

func main() {
	target := flag.String("target", "sf", "endpoint to hit: sf | plain")
	scenario := flag.String("scenario", "burst", "load shape: burst | ramp")
	baseURL := flag.String("base-url", "http://localhost:3000", "server base url")
	userID := flag.String("user-id", "123", "user id hit by every request (same id = dedup-able)")
	flag.Parse()

	path := "/sf/user/" + *userID
	if *target == "plain" {
		path = "/user/" + *userID
	}
	url := *baseURL + path

	targeter := vegeta.NewStaticTargeter(vegeta.Target{
		Method: "GET",
		URL:    url,
	})

	var results vegeta.Results

	switch *scenario {
	case "burst":
		// fire fast enough that many requests for the same key overlap
		// while the (2s) simulated DB call is still in flight
		log.Printf("burst: firing at 200 req/s for 3s (same key, so requests overlap)")
		rate := vegeta.Rate{Freq: 200, Per: time.Second}
		attacker := vegeta.NewAttacker()
		for res := range attacker.Attack(targeter, rate, 3*time.Second, "burst") {
			results = append(results, *res)
		}

	case "ramp":
		// mirrors the k6 "ramp" scenario: 0->50 over 10s, hold 50 for 20s, 50->0 over 10s
		// note: a fresh vegeta.Attacker is created per stage — reusing one Attacker
		// across multiple sequential Attack() calls causes later stages to stop firing early.
		stages := []struct {
			label string
			rate  int
			dur   time.Duration
		}{
			{"ramp-up 0->50rps", 25, 10 * time.Second},
			{"hold 50rps", 50, 20 * time.Second},
			{"ramp-down 50->0rps", 25, 10 * time.Second},
		}
		for _, s := range stages {
			log.Printf("%s for %s", s.label, s.dur)
			rate := vegeta.Rate{Freq: s.rate, Per: time.Second}
			attacker := vegeta.NewAttacker()
			for res := range attacker.Attack(targeter, rate, s.dur, s.label) {
				results = append(results, *res)
			}
		}

	default:
		log.Fatalf("unknown scenario %q (want burst|ramp)", *scenario)
	}

	printSummary(url, *scenario, results)
}

func printSummary(url, scenario string, results vegeta.Results) {
	var metrics vegeta.Metrics
	for _, r := range results {
		metrics.Add(&r)
	}
	metrics.Close()

	fmt.Println()
	fmt.Printf("Target:       %s\n", url)
	fmt.Printf("Scenario:     %s\n", scenario)
	fmt.Printf("Requests:     %d\n", metrics.Requests)
	fmt.Printf("Success rate: %.2f%%\n", metrics.Success*100)
	fmt.Printf("Throughput:   %.2f req/s\n", metrics.Throughput)
	fmt.Printf("Latency avg:  %s\n", metrics.Latencies.Mean)
	fmt.Printf("Latency p50:  %s\n", metrics.Latencies.P50)
	fmt.Printf("Latency p95:  %s\n", metrics.Latencies.P95)
	fmt.Printf("Latency p99:  %s\n", metrics.Latencies.P99)
	fmt.Printf("Latency max:  %s\n", metrics.Latencies.Max)
	if len(metrics.StatusCodes) > 0 {
		fmt.Printf("Status codes: %v\n", metrics.StatusCodes)
	}
	if len(metrics.Errors) > 0 {
		fmt.Printf("Errors:       %v\n", metrics.Errors)
	}
}
