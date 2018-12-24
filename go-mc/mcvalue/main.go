package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/felixhao/overlord-benchmark/go-mc/conn"
)

var (
	addr string
	key  string
)

func init() {
	flag.StringVar(&addr, "addr", "", "addr")
	flag.StringVar(&key, "key", "", "key")
}

func main() {
	flag.Parse()
	conn, err := conn.Dial("tcp", addr, time.Second, time.Second, time.Second)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r, err := conn.Get(key)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	io.Copy(os.Stdout, bytes.NewBuffer(r.Value))
}
