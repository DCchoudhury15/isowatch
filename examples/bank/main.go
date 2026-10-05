// Command bank is a tiny demo service with two classic concurrency bugs,
// and fixed versions of both, so you can watch IsoWatch catch them.
//
//	go run . serve -dsn postgres://postgres:postgres@localhost:55432/postgres
//	go run . load
//
// Endpoints:
//
//	POST /deposit       read balance, add, write it back   (lost update bug)
//	POST /deposit-safe  same, but with SELECT ... FOR UPDATE (fixed)
//	POST /deposit-auto  the buggy code, protected by IsoWatch once it sees a problem
//	POST /leave         a doctor leaves if someone else is on call (write skew bug)
//	POST /leave-safe    same, but SERIALIZABLE with retry       (fixed)
//	POST /leave-auto    the buggy code, protected by IsoWatch once it sees a problem
//	GET  /state         balances, doctors on call, and IsoWatch counters
//	POST /reset         put the demo data back
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:])
	case "load":
		err = load(os.Args[2:])
	default:
		usage()
	}
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, flag.ErrHelp) {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  bank serve [-dsn URL] [-addr :8090] [-mode embedded|otlp|both] [-otlp localhost:4318]
  bank load  [-url http://localhost:8090] [-requests 200] [-workers 20] [-safe | -auto]`)
	os.Exit(2)
}
