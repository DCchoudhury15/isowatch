package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// load sends concurrent requests and then prints what the database ended up
// with, next to what it should have ended up with.
func load(args []string) error {
	fs := flag.NewFlagSet("load", flag.ContinueOnError)
	base := fs.String("url", "http://localhost:8090", "demo server")
	requests := fs.Int("requests", 200, "deposit requests to send")
	workers := fs.Int("workers", 20, "requests in flight at once")
	safe := fs.Bool("safe", false, "use the fixed endpoints")
	auto := fs.Bool("auto", false, "use the buggy endpoints, protected by IsoWatch once it sees a problem (embedded mode)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	client := &http.Client{Timeout: 30 * time.Second}
	post := func(path string) bool {
		resp, err := client.Post(*base+path, "", nil)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode < 300
	}
	if !post("/reset") {
		return fmt.Errorf("cannot reach %s, is the demo running?", *base)
	}

	suffix := ""
	switch {
	case *safe && *auto:
		return fmt.Errorf("choose -safe or -auto, not both")
	case *safe:
		suffix = "-safe"
	case *auto:
		suffix = "-auto"
	}

	// Deposits of 1 to account 1.
	var ok atomic.Int64
	jobs := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				if post("/deposit" + suffix + "?account=1&amount=1") {
					ok.Add(1)
				}
			}
		}()
	}
	start := time.Now()
	for i := 0; i < *requests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()

	// Both doctors try to leave at the same moment.
	var lwg sync.WaitGroup
	for _, d := range []string{"alice", "bob"} {
		lwg.Add(1)
		go func(d string) {
			defer lwg.Done()
			post("/leave" + suffix + "?doctor=" + d)
		}(d)
	}
	lwg.Wait()

	// Give the checker (or the collector) a moment to see the last spans.
	time.Sleep(2 * time.Second)
	resp, err := client.Get(*base + "/state")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var st stateJSON
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return err
	}

	fmt.Printf("\n%d deposits of 1 succeeded in %s\n", ok.Load(), time.Since(start).Round(time.Millisecond))
	fmt.Printf("  balance should be: %d\n", ok.Load())
	fmt.Printf("  balance is:        %d\n", st.Balances[1])
	fmt.Printf("  deposits lost:     %d\n", ok.Load()-int64(st.Balances[1]))
	fmt.Printf("\ndoctors still on call: %v (should never be empty)\n", st.OnCall)
	if st.Mode == "embedded" {
		fmt.Printf("\nIsoWatch saw: %v\n", st.Anomalies)
		fmt.Printf("IsoWatch counted lost writes: %d\n", st.LostWrite)
		if len(st.Capture.Protected) > 0 {
			fmt.Printf("IsoWatch protected: %v (retries: %d)\n", st.Capture.Protected, st.Capture.Retried)
		}
	} else {
		fmt.Println("\nIsoWatch runs in the collector in this mode: check the collector output.")
	}
	return nil
}
