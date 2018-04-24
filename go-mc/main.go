package main

import (
	"bytes"
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
	mkeys       int
	cmd         int
	tm          string
	always      bool
	addr        string

	cs []*errConn
)

type stat struct {
	f     int
	n, en int32
	ts    int32
	// equal err
	ee int32
	// multi equal err
	mee int32
}

func init() {
	flag.IntVar(&concurrency, "c", 1, "concurrency")
	flag.IntVar(&requests, "n", 100, "requests")
	flag.IntVar(&size, "s", 256, "bytes size")
	flag.IntVar(&mkeys, "k", 0, "multi keys")
	flag.IntVar(&cmd, "f", 3, "command flag. bit: 1Set 10Get 100MGet.")
	flag.StringVar(&tm, "t", "", "enough duration.")
	flag.BoolVar(&always, "a", false, "always")
	flag.StringVar(&addr, "addr", "", "addr")
}

func main() {
	flag.Parse()
	ch := make(chan struct{}, 1)
	tc := time.After(time.Hour) // NOTE: enough long
	tt := false
	if tm != "" {
		ts, err := time.ParseDuration(tm)
		if err != nil {
			panic(err)
		}
		tc = time.After(ts)
		tt = true
	}
	cs = make([]*errConn, concurrency)
	for i := 0; i < concurrency; i++ {
		ec := &errConn{}
		ec.reconn()
		cs[i] = ec
	}
	concur(ch)
	for {
		select {
		case <-ch:
			if !tt {
				return
			}
			concur(ch)
		case <-tc:
			return
		}
	}
}

type errConn struct {
	conn *conn.Conn
	err  error
}

func (ec *errConn) reconn() {
	conn, err := conn.Dial("tcp", addr, time.Second, time.Second, time.Second)
	if err == nil {
		ec.conn = conn
		ec.err = nil
	}
}

func concur(ch chan<- struct{}) {
	ssCh := make(chan []*stat, concurrency)
	for i := 0; i < concurrency; i++ {
		go exec(cs[i], requests/concurrency, ssCh)
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
			setS.en += s.en
		} else if s.f == cmdGet {
			getS.n += s.n
			getS.ts += s.ts
			getS.en += s.en
			getS.ee += s.ee
		} else if s.f == cmdMGet {
			mgetS.n += s.n
			mgetS.ts += s.ts
			mgetS.en += s.en
			mgetS.ee += s.ee
			mgetS.mee += s.mee
		}
	}
	if cmd&cmdSet > 0 {
		fmt.Printf("SET Success:%d Failure:%d Time:%.6f\n", setS.n, setS.en, float32(setS.ts)/float32(setS.n))
	}
	if cmd&cmdGet > 0 {
		fmt.Printf("GET Success:%d Failure:%d NotEqual:%d Time:%.6f\n", getS.n, getS.en, getS.ee, float32(getS.ts)/float32(getS.n))
	}
	if cmd&cmdMGet > 0 {
		fmt.Printf("MGET Success:%d Failure:%d NotEqual:%d NotResult:%d Time:%.6f\n", mgetS.n, mgetS.en, mgetS.ee, mgetS.mee, float32(mgetS.ts)/float32(mgetS.n))
	}
	ch <- struct{}{}
}

func exec(c *errConn, n int, ssCh chan []*stat) {
	allocK := [300]string{}
	keys := allocK[:0]
	items := map[string]*conn.Item{}
	ks := mkeys
	if ks == 0 {
		ks = 10
	}
	s1 := &stat{}
	s2 := &stat{}
	s3 := &stat{}
	for i := 0; i < n || always; i++ {
		if c.err != nil {
			c.reconn()
			continue
		}
		key := randKey()
		item := &conn.Item{
			Key: key,
		}
		if cmd&cmdSet > 0 {
			item.Value = randValue()
			start := time.Now()
			err := c.conn.Set(item)
			tc := int32(time.Since(start) / time.Millisecond)
			s1.f = cmdSet
			if err != nil {
				s1.en++
				println("SET:", err.Error())
				c.reconn()
			} else {
				s1.ts += tc
				s1.n++
			}
		}
		if cmd&cmdGet > 0 {
			start := time.Now()
			r, err := c.conn.Get(key)
			tc := int32(time.Since(start) / time.Millisecond)
			s2.f = cmdGet
			if err != nil {
				s2.en++
				println("GET:", err.Error())
				c.reconn()
			} else {
				s2.ts += tc
				s2.n++
			}
			if r == nil || !bytes.Equal(r.Value, item.Value) {
				s2.ee++
			}
		}
		if cmd&cmdMGet > 0 {
			keys = append(keys, key)
			items[key] = item
			if len(keys) >= ks {
				start := time.Now()
				res, err := c.conn.GetMulti(keys)
				tc := int32(time.Since(start) / time.Millisecond)
				s3.f = cmdMGet
				if err != nil {
					s3.en++
					println("MGET:", err.Error())
					c.reconn()
				} else {
					s3.ts += tc
					s3.n++
				}
				if cmd&cmdSet > 0 && res != nil {
					for _, key := range keys {
						i := items[key]
						if i == nil {
							s3.mee++
							continue
						}
						if r := res[key]; r != nil {
							if !bytes.Equal(r.Value, i.Value) {
								s3.ee++
							}
						} else {
							s3.mee++
						}
					}
				}
				keys = allocK[:0]
				if mkeys > 0 {
					ks = mkeys
				} else {
					ks = mrand.Intn(290) + 10
				}
				items = map[string]*conn.Item{}
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
	ss := base64.StdEncoding.EncodeToString(bs)
	return []byte(ss)
}
