package isowatchtest_test

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// fakeT is a testing.TB that records failures instead of failing the real
// test, so the tests can check what isowatchtest reports. Methods it does
// not override (Name, TempDir, Context...) go to the real test.
type fakeT struct {
	testing.TB

	mu       sync.Mutex
	errors   []string
	fatals   []string
	logs     []string
	cleanups []func()
	failed   bool
}

func (f *fakeT) Helper() {}

func (f *fakeT) Fail() {
	f.mu.Lock()
	f.failed = true
	f.mu.Unlock()
}

func (f *fakeT) Failed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failed
}

func (f *fakeT) Error(args ...any) { f.addError(fmt.Sprint(args...)) }

func (f *fakeT) Errorf(format string, args ...any) { f.addError(fmt.Sprintf(format, args...)) }

func (f *fakeT) addError(msg string) {
	f.mu.Lock()
	f.errors = append(f.errors, msg)
	f.failed = true
	f.mu.Unlock()
}

// Fatal records the message and stops the calling goroutine, like the real
// FailNow does.
func (f *fakeT) Fatal(args ...any) { f.fatal(fmt.Sprint(args...)) }

func (f *fakeT) Fatalf(format string, args ...any) { f.fatal(fmt.Sprintf(format, args...)) }

func (f *fakeT) FailNow() { f.fatal("FailNow") }

func (f *fakeT) fatal(msg string) {
	f.mu.Lock()
	f.fatals = append(f.fatals, msg)
	f.failed = true
	f.mu.Unlock()
	runtime.Goexit()
}

func (f *fakeT) Log(args ...any) { f.addLog(fmt.Sprint(args...)) }

func (f *fakeT) Logf(format string, args ...any) { f.addLog(fmt.Sprintf(format, args...)) }

func (f *fakeT) addLog(msg string) {
	f.mu.Lock()
	f.logs = append(f.logs, msg)
	f.mu.Unlock()
}

func (f *fakeT) Cleanup(fn func()) {
	f.mu.Lock()
	f.cleanups = append(f.cleanups, fn)
	f.mu.Unlock()
}

// all returns every error and fatal message, joined.
func (f *fakeT) all() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(append(append([]string(nil), f.errors...), f.fatals...), "\n---\n")
}

// runFake runs body as if it were a test function of its own: on its own
// goroutine, so Fatal stops only body, and then its cleanups, last first.
func runFake(t *testing.T, body func(ft *fakeT)) *fakeT {
	t.Helper()
	ft := &fakeT{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		body(ft)
	}()
	<-done
	for i := len(ft.cleanups) - 1; i >= 0; i-- {
		fn := ft.cleanups[i]
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			fn()
		}()
		<-finished
	}
	return ft
}
