package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	mrand "math/rand"
	"time"

	"github.com/felixhao/overlord-benchmark/go-mc/conn"
)

const (
	cmdSet  = 1
	cmdGet  = 1 << 1
	cmdMGet = 1 << 2
)

var (
	concurrency int
	requests    int
	size        int
	cmd         int
	addr        string
)

type stat struct {
	f  int
	n  int32
	ts int32
}

func init() {
	flag.IntVar(&concurrency, "c", 1, "concurrency")
	flag.IntVar(&requests, "n", 1, "requests")
	flag.IntVar(&size, "s", 256, "bytes size")
	flag.IntVar(&cmd, "f", 3, "command flag. bit: 1Set 10Get 100MGet.")
	flag.StringVar(&addr, "addr", "", "addr")
}

func main() {
	flag.Parse()

	ssCh := make(chan []*stat, concurrency)
	for i := 0; i < concurrency; i++ {
		go exec(requests/concurrency, ssCh)
	}
	ss := make([]*stat, 0, concurrency*3)
	for i := 0; i < concurrency; i++ {
		tmp := <-ssCh
		ss = append(ss, tmp...)
	}
	setS := &stat{}
	getS := &stat{}
	mgetS := &stat{}
	for _, s := range ss {
		if s.f == cmdSet {
			setS.n += s.n
			setS.ts += s.ts
		} else if s.f == cmdGet {
			getS.n += s.n
			getS.ts += s.ts
		} else if s.f == cmdMGet {
			mgetS.n += s.n
			mgetS.ts += s.ts
		}
	}
	if cmd&cmdSet > 0 {
		fmt.Printf("SET %d %.6f", setS.n, float32(setS.ts)/float32(setS.n))
	} else if cmd&cmdGet > 0 {
		fmt.Printf("GET %d %.6f", getS.n, float32(getS.ts)/float32(getS.n))
	} else if cmd&cmdMGet > 0 {
		fmt.Printf("MGET %d %.6f", mgetS.n, float32(mgetS.ts)/float32(mgetS.n))
	}
}

func exec(n int, ssCh chan []*stat) {
	c, err := conn.Dial("tcp", addr, time.Second, time.Second, time.Second)
	if err != nil {
		println("exec cmd error:", err.Error())
	}
	alloc := [300]string{}
	keys := alloc[:0]
	ks := 10
	s1 := &stat{}
	s2 := &stat{}
	s3 := &stat{}
	for i := 0; i < n; i++ {
		key := randKey()
		item := &conn.Item{
			Key: key,
		}
		if cmd&cmdSet > 0 {
			item.Value = randValue()
			start := time.Now()
			c.Set(item)
			tc := int32(time.Since(start) / time.Millisecond)
			s1.f = cmdSet
			s1.ts += tc
			s1.n++
		}
		if cmd&cmdGet > 0 {
			start := time.Now()
			c.Get(key)
			tc := int32(time.Since(start) / time.Millisecond)
			s2.f = cmdGet
			s2.ts += tc
			s2.n++
		}
		if cmd&cmdMGet > 0 {
			keys = append(keys, key)
			if len(keys) >= ks {
				start := time.Now()
				c.GetMulti(keys)
				tc := int32(time.Since(start) / time.Millisecond)
				s3.f = cmdGet
				s3.ts += tc
				s3.n++
				keys = alloc[:0]
				ks = mrand.Intn(290) + 10
			}
		}
	}
	ssCh <- []*stat{s1, s2, s3}
}

func randKey() string {
	bs := make([]byte, 16)
	rand.Read(bs)
	return hex.EncodeToString(bs)
}

func randValue() []byte {
	bs := make([]byte, size)
	rand.Read(bs)
	wr := base64.StdEncoding.EncodeToString(bs)
	return []byte(wr[:size])
}
