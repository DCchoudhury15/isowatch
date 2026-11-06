package isowatchtest_test

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DCchoudhury15/isowatch/isowatchtest"
)

// No call may start before every goroutine of its round exists and waits at
// the barrier. So when any call starts, all n goroutines are alive.
func TestConcurrentlyStartsTogether(t *testing.T) {
	const n = 300
	base := runtime.NumGoroutine()
	var minSeen atomic.Int64
	minSeen.Store(1 << 30)
	var entered atomic.Int64
	all := make(chan struct{})
	isowatchtest.Concurrently(t, n, func(int) error {
		g := int64(runtime.NumGoroutine())
		for {
			cur := minSeen.Load()
			if g >= cur || minSeen.CompareAndSwap(cur, g) {
				break
			}
		}
		// Hold every call until all have started, so none exits early.
		if entered.Add(1) == n {
			close(all)
		}
		select {
		case <-all:
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("not every call started")
		}
	})
	// A little slack for unrelated goroutines that exit meanwhile.
	if got, want := minSeen.Load(), int64(base+n-10); got < want {
		t.Fatalf("a call started when only %d goroutines were running, want at least %d: the calls did not start together", got, want)
	}
}

func TestConcurrentlyFailsOnErrors(t *testing.T) {
	var calls atomic.Int64
	reached := false
	ft := runFake(t, func(ft *fakeT) {
		isowatchtest.Concurrently(ft, 8, func(i int) error {
			calls.Add(1)
			if i%4 == 0 {
				return fmt.Errorf("boom %d", i)
			}
			return nil
		}, isowatchtest.Repeat(3))
		reached = true
	})
	if reached {
		t.Fatal("Concurrently should stop the test when calls fail")
	}
	if len(ft.fatals) != 1 {
		t.Fatalf("want one fatal message, got %q", ft.fatals)
	}
	mustContain(t, ft.fatals[0], "2 of 8 calls failed in round 1 of 3", "call 0: boom 0", "call 4: boom 4")
	if calls.Load() != 8 {
		t.Fatalf("no round should run after a failed one: %d calls", calls.Load())
	}
}

func TestConcurrentlyRecoversPanics(t *testing.T) {
	ft := runFake(t, func(ft *fakeT) {
		isowatchtest.Concurrently(ft, 4, func(i int) error {
			if i == 3 {
				panic("kaboom")
			}
			return nil
		})
	})
	mustContain(t, ft.all(), "1 of 4 calls failed", "call 3: panic: kaboom")
}

func TestRepeatRunsRoundsOneAfterAnother(t *testing.T) {
	const n, rounds = 5, 4
	var mu sync.Mutex
	seen := map[int]int{}
	var finished atomic.Int64
	isowatchtest.Concurrently(t, n, func(i int) error {
		if round := i / n; finished.Load() < int64(round*n) {
			return fmt.Errorf("call %d of round %d started before round %d finished", i, round, round-1)
		}
		mu.Lock()
		seen[i]++
		mu.Unlock()
		time.Sleep(time.Millisecond)
		finished.Add(1)
		return nil
	}, isowatchtest.Repeat(rounds))
	for i := 0; i < n*rounds; i++ {
		if seen[i] != 1 {
			t.Fatalf("call %d ran %d times, want once (all: %v)", i, seen[i], seen)
		}
	}
}

func TestConcurrentlyRejectsZeroCalls(t *testing.T) {
	ft := runFake(t, func(ft *fakeT) {
		isowatchtest.Concurrently(ft, 0, func(int) error { return nil })
	})
	mustContain(t, ft.all(), "n must be at least 1")
}
