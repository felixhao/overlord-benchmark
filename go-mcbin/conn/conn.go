package conn

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	magicReq  byte = 0x80
	magicResp byte = 0x81

	zeroByte byte = 0x00
)

var (
	zeroTwoBytes   = []byte{0x00, 0x00}
	zeroOneBytes   = []byte{0x00, 0x01}
	zeroFourBytes  = []byte{0x00, 0x00, 0x00, 0x00}
	zeroEightBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
)

const (
	RequestTypeGet      byte = 0x00
	RequestTypeSet      byte = 0x01
	RequestTypeAdd      byte = 0x02
	RequestTypeReplace  byte = 0x03
	RequestTypeDelete   byte = 0x04
	RequestTypeIncr     byte = 0x05
	RequestTypeDecr     byte = 0x06
	RequestTypeGetQ     byte = 0x09
	RequestTypeNoop     byte = 0x0a
	RequestTypeGetK     byte = 0x0c
	RequestTypeGetKQ    byte = 0x0d
	RequestTypeAppend   byte = 0x0e
	RequestTypePrepend  byte = 0x0f
	RequestTypeSetQ     byte = 0x11
	RequestTypeAddQ     byte = 0x12
	RequestTypeReplaceQ byte = 0x13
	RequestTypeIncrQ    byte = 0x15
	RequestTypeDecrQ    byte = 0x16
	RequestTypeAppendQ  byte = 0x19
	RequestTypePrependQ byte = 0x1a
	RequestTypeTouch    byte = 0x1c
	RequestTypeGat      byte = 0x1d
	RequestTypeGatQ     byte = 0x1e
	RequestTypeUnknown  byte = 0xff
)

var (
	// ErrNotFound not found
	ErrNotFound = errors.New("memcache: key not found")
	// ErrExists exists
	ErrExists = errors.New("memcache: key exists")
	// ErrNotStored not stored
	ErrNotStored = errors.New("memcache: key not stored")
	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrPoolExhausted is returned from a pool connection method (Store, Get,
	// Delete, IncrDecr, Err) when the maximum number of database connections
	// in the pool has been reached.
	ErrPoolExhausted = errors.New("memcache: connection pool exhausted")
	// ErrPoolClosed pool closed
	ErrPoolClosed = errors.New("memcache: connection pool closed")
	// ErrConnClosed conn closed
	ErrConnClosed = errors.New("memcache: connection closed")
	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("memcache: malformed key is too long or contains invalid characters")
	// ErrValueSize item value size must less than 1mb
	ErrValueSize = errors.New("memcache: item value size must not greater than 1mb")
	// ErrStat stat error for monitor
	ErrStat = errors.New("memcache unexpected errors")
	// ErrItem item nil.
	ErrItem = errors.New("memcache: item object nil")
	// ErrItemObject object type Assertion failed
	ErrItemObject = errors.New("memcache: item object protobuf type assertion failed")
	// ErrNotResponse not response
	ErrNotResponse = errors.New("memcache: not response")
)

const (
	// Flag, 15(encoding) bit+ 17(compress) bit

	// FlagRAW default flag.
	FlagRAW = uint32(0)
	// FlagGOB gob encoding.
	FlagGOB = uint32(1) << 0
	// FlagJSON json encoding.
	FlagJSON = uint32(1) << 1

	_flagEncoding = uint32(0xFFFF8000)

	// FlagGzip gzip compress.
	FlagGzip = uint32(1) << 15

	// left mv 31??? not work!!!
	flagLargeValue = uint32(1) << 30
)

const (
	_encodeBuf = 4096 // 4kb
	// 1024*1024 - 1, set error???
	_largeValue = 1000 * 1000 // 1MB
)

// Item is an reply to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string
	// Value is the Item's value.
	Value []byte
	// Object is the Item's object for use codec.
	Object interface{}
	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32
	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32
	// Compare and swap ID.
	cas uint64
}

type reader struct {
	io.Reader
}

func (r *reader) Reset(rd io.Reader) {
	r.Reader = rd
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("memcache: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

// Conn is the low-level implementation of Conn
type Conn struct {
	// Shared
	mu   sync.Mutex
	err  error
	conn net.Conn
	// Read & Write
	readTimeout  time.Duration
	writeTimeout time.Duration
	rw           *bufio.ReadWriter
	// Item Reader
	ir bytes.Reader
	// Compress
	gr gzip.Reader
	gw gzip.Writer
	cb bytes.Buffer
	// Encoding
	edb bytes.Buffer
	// json
	jr reader
	jd *json.Decoder
	je *json.Encoder
}

// Dial connects to the Memcache server at the given network and
// address using the specified options.
func Dial(network, address string, dialTimeout, readTimeout, writeTimeout time.Duration) (*Conn, error) {
	netConn, err := net.DialTimeout(network, address, dialTimeout)
	if err != nil {
		return nil, err
	}
	return NewConn(netConn, readTimeout, writeTimeout), nil
}

// NewConn returns a new memcache connection for the given net connection.
func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) *Conn {
	if writeTimeout <= 0 || readTimeout <= 0 {
		panic("must config memcache timeout")
	}
	c := &Conn{
		conn: netConn,
		rw: bufio.NewReadWriter(bufio.NewReader(netConn),
			bufio.NewWriter(netConn)),
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
	c.jd = json.NewDecoder(&c.jr)
	c.je = json.NewEncoder(&c.edb)
	c.edb.Grow(_encodeBuf)
	return c
}

func (c *Conn) Close() error {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("memcache: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *Conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		// Close connection to force errors on subsequent calls and to unblock
		// other reader or writer.
		c.conn.Close()
	}
	c.mu.Unlock()
	return c.err
}

func (c *Conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *Conn) Add(item *Item) error {
	return c.populate(RequestTypeAdd, item)
}

func (c *Conn) Set(item *Item) error {
	return c.populate(RequestTypeSet, item)
}

func (c *Conn) Replace(item *Item) error {
	return c.populate(RequestTypeReplace, item)
}

func (c *Conn) populate(cmd byte, item *Item) (err error) {
	var res []byte
	if res, err = c.encode(item); err != nil {
		return
	}
	l := len(res)
	count := l/(_largeValue) + 1
	if count == 1 {
		item.Value = res
		return c.populateOne(cmd, item)
	}
	nItem := &Item{
		Key:        item.Key,
		Value:      []byte(strconv.Itoa(l)),
		Expiration: item.Expiration,
		Flags:      item.Flags | flagLargeValue,
	}
	err = c.populateOne(cmd, nItem)
	if err != nil {
		return
	}
	k := item.Key
	nItem.Flags = item.Flags
	for i := 1; i <= count; i++ {
		if i == count {
			nItem.Value = res[_largeValue*(count-1):]
		} else {
			nItem.Value = res[_largeValue*(i-1) : _largeValue*i]
		}
		nItem.Key = fmt.Sprintf("%s%d", k, i)
		if err = c.populateOne(cmd, nItem); err != nil {
			return
		}
	}
	return
}

func (c *Conn) populateOne(cmd byte, item *Item) (err error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	kl := make([]byte, 2)
	binary.BigEndian.PutUint16(kl, uint16(len(item.Key)))

	fl := make([]byte, 4)
	binary.BigEndian.PutUint32(fl, uint32(item.Flags))
	el := make([]byte, 4)
	binary.BigEndian.PutUint32(el, uint32(item.Expiration))
	total := len(item.Key) + len(item.Value) + 4 + 4 //NOTE: flag+expiration
	bl := make([]byte, 4)
	binary.BigEndian.PutUint32(bl, uint32(total))
	cl := make([]byte, 8)
	binary.BigEndian.PutUint64(cl, item.cas)

	c.rw.WriteByte(magicReq)
	c.rw.WriteByte(cmd)
	c.rw.Write(kl)
	c.rw.WriteByte(0x08)
	c.rw.WriteByte(zeroByte)
	c.rw.Write(zeroTwoBytes)
	c.rw.Write(bl)
	c.rw.Write(zeroFourBytes)
	c.rw.Write(cl)
	c.rw.Write(fl)
	c.rw.Write(el)
	c.rw.Write([]byte(item.Key))
	c.rw.Write(item.Value)

	if err != nil {
		return c.fatal(err)
	}
	if err = c.rw.Flush(); err != nil {
		return c.fatal(err)
	}
	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	resp := make([]byte, 24)
	n, err := c.rw.Read(resp)
	if err != nil || n != 24 {
		return c.fatal(err)
	}
	if resp[0] != magicResp {
		return ErrNotResponse
	}
	ss := resp[6:8]
	if !bytes.Equal(ss, zeroTwoBytes) {
		tls := resp[8:12]
		tl := binary.BigEndian.Uint32(tls)

		if tl > 0 {
			body := make([]byte, tl)

			if n, err = c.rw.Read(body); err != nil || n != int(tl) {
				return c.fatal(fmt.Errorf("response error(%s)", body))
			}
		}

		return fmt.Errorf("response error(%x)", ss)
	}
	return nil
}

func (c *Conn) Get(key string) (r *Item, err error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	kl := make([]byte, 2)
	binary.BigEndian.PutUint16(kl, uint16(len(key)))

	bl := make([]byte, 4)
	binary.BigEndian.PutUint32(bl, uint32(len(key)))

	c.rw.WriteByte(magicReq)
	c.rw.WriteByte(RequestTypeGetK)
	c.rw.Write(kl)
	c.rw.WriteByte(zeroByte)
	c.rw.WriteByte(zeroByte)
	c.rw.Write(zeroTwoBytes)
	c.rw.Write(bl)
	c.rw.Write(zeroFourBytes)
	c.rw.Write(zeroEightBytes)
	c.rw.Write([]byte(key))

	if err = c.rw.Flush(); err != nil {
		return nil, c.fatal(err)
	}
	if err = c.parseGetReply(func(it *Item) {
		r = it
	}); err != nil {
		return
	}
	if r == nil {
		err = ErrNotFound
		return
	}
	if r.Flags&flagLargeValue != flagLargeValue {
		return
	}
	if r, err = c.getLargeValue(r); err != nil {
		return
	}
	return
}

func (c *Conn) GetMulti(keys []string) (res map[string]*Item, err error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	for i, key := range keys {
		kl := make([]byte, 2)
		binary.BigEndian.PutUint16(kl, uint16(len(key)))

		cmd := RequestTypeGetKQ
		if i == len(keys)-1 {
			cmd = RequestTypeGetK
		}

		bl := make([]byte, 4)
		binary.BigEndian.PutUint32(bl, uint32(len(key)))

		c.rw.WriteByte(magicReq)
		c.rw.WriteByte(cmd)
		c.rw.Write(kl)
		c.rw.WriteByte(zeroByte)
		c.rw.WriteByte(zeroByte)
		c.rw.Write(zeroTwoBytes)
		c.rw.Write(bl)
		c.rw.Write(zeroFourBytes)
		c.rw.Write(zeroEightBytes)
		c.rw.Write([]byte(key))
	}

	if err = c.rw.Flush(); err != nil {
		return nil, c.fatal(err)
	}
	res = make(map[string]*Item, len(keys))
	if err = c.parseGetReply(func(it *Item) {
		res[it.Key] = it
	}); err != nil {
		return
	}
	for k, v := range res {
		if v.Flags&flagLargeValue != flagLargeValue {
			continue
		}
		r, err := c.getLargeValue(v)
		if err != nil {
			return res, err
		}
		res[k] = r
	}
	return
}

func (c *Conn) getMulti(keys []string) (res map[string]*Item, err error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	for i, key := range keys {
		kl := make([]byte, 2)
		binary.BigEndian.PutUint16(kl, uint16(len(key)))

		cmd := RequestTypeGetKQ
		if i == len(keys)-1 {
			cmd = RequestTypeGetK
		}

		bl := make([]byte, 4)
		binary.BigEndian.PutUint32(bl, uint32(len(key)))

		c.rw.WriteByte(magicReq)
		c.rw.WriteByte(cmd)
		c.rw.Write(kl)
		c.rw.WriteByte(zeroByte)
		c.rw.WriteByte(zeroByte)
		c.rw.Write(zeroTwoBytes)
		c.rw.Write(bl)
		c.rw.Write(zeroFourBytes)
		c.rw.Write(zeroEightBytes)
		c.rw.Write([]byte(key))
	}
	if err = c.rw.Flush(); err != nil {
		return nil, c.fatal(err)
	}
	res = make(map[string]*Item, len(keys))
	err = c.parseGetReply(func(it *Item) {
		res[it.Key] = it
	})
	return
}

func (c *Conn) getLargeValue(it *Item) (r *Item, err error) {
	l, err := strconv.Atoi(string(it.Value))
	if err != nil {
		return
	}
	count := l/_largeValue + 1
	keys := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		keys = append(keys, fmt.Sprintf("%s%d", it.Key, i))
	}
	items, err := c.getMulti(keys)
	if err != nil {
		return
	}
	if len(items) < count {
		err = ErrNotFound
		return
	}
	v := make([]byte, 0, l)
	for _, k := range keys {
		if items[k] == nil || items[k].Value == nil {
			err = ErrNotFound
			return
		}
		v = append(v, items[k].Value...)
	}
	it.Value = v
	it.Flags = it.Flags ^ flagLargeValue
	r = it
	return
}

func (c *Conn) parseGetReply(f func(*Item)) error {
	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	for {
		resp := make([]byte, 24)
		n, err := c.rw.Read(resp)
		if err != nil || n != 24 {
			return c.fatal(err)
		}
		if resp[0] != magicResp {
			return ErrNotResponse
		}
		ss := resp[6:8]
		if bytes.Equal(ss, zeroOneBytes) {
			return ErrNotFound
		}
		if !bytes.Equal(ss, zeroTwoBytes) {
			return fmt.Errorf("response error(%x)", ss)
		}
		it := new(Item)

		cs := resp[16:24]
		it.cas = binary.BigEndian.Uint64(cs)

		fs := make([]byte, 4)
		n, err = c.rw.Read(fs) // NOTE: extra
		if err != nil || n != 4 {
			return c.fatal(fmt.Errorf("response read flag len(%d) error(%x)", n, err))
		}
		it.Flags = binary.BigEndian.Uint32(fs)

		kls := resp[2:4]
		kl := binary.BigEndian.Uint16(kls)
		if kl > 0 {
			ks := make([]byte, kl)
			if n, err = c.rw.Read(ks); err != nil || n != int(kl) {
				return c.fatal(fmt.Errorf("response read key len(%d) expect(%d) error(%v)", n, kl, err))
			}
			it.Key = string(ks)
		}

		bls := resp[8:12]
		bl := binary.BigEndian.Uint32(bls)
		if bl > 0 {
			bbl := int(bl) - int(kl) - 4 // NOTE: 4=flag len
			vs := make([]byte, bbl)
			n, err = c.rw.Read(vs)
			if err != nil || n != bbl {
				return c.fatal(fmt.Errorf("response read value len(%d) expect(%d) error(%v)", n, bbl, err))
			}
			it.Value = vs
		}
		f(it)
		if resp[1] == RequestTypeGetK {
			return nil
		}
	}
}

func (c *Conn) Touch(key string, expire int32) (err error) {

	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	kl := make([]byte, 2)
	binary.BigEndian.PutUint16(kl, uint16(len(key)))

	el := make([]byte, 4)
	binary.BigEndian.PutUint32(el, uint32(expire))

	tl := make([]byte, 4)
	binary.BigEndian.PutUint32(tl, uint32(len(key))+4)

	c.rw.WriteByte(magicReq)
	c.rw.WriteByte(RequestTypeTouch)
	c.rw.Write(kl)
	c.rw.WriteByte(zeroByte)
	c.rw.WriteByte(zeroByte)
	c.rw.Write(zeroTwoBytes)
	c.rw.Write(tl)
	c.rw.Write(zeroFourBytes)
	c.rw.Write(zeroEightBytes)
	c.rw.Write(el)
	c.rw.Write([]byte(key))

	if err := c.rw.Flush(); err != nil {
		return c.fatal(err)
	}
	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	resp := make([]byte, 24)
	n, err := c.rw.Read(resp)
	if err != nil || n != 24 {
		return c.fatal(err)
	}
	if resp[0] != magicResp {
		return ErrNotResponse
	}
	ss := resp[6:8]
	if !bytes.Equal(ss, zeroTwoBytes) {
		return fmt.Errorf("response error(%x)", ss)
	}
	return nil
}

func (c *Conn) Increment(key string, delta uint64) (uint64, error) {
	return c.incrDecr(RequestTypeIncr, key, delta)
}

func (c *Conn) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr(RequestTypeDecr, key, delta)
}

func (c *Conn) incrDecr(cmd byte, key string, delta uint64) (uint64, error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	kl := make([]byte, 2)
	binary.BigEndian.PutUint16(kl, uint16(len(key)))

	dl := make([]byte, 8)
	binary.BigEndian.PutUint64(dl, uint64(delta))

	tl := make([]byte, 4)
	binary.BigEndian.PutUint32(tl, uint32(len(key))+20)

	c.rw.WriteByte(magicReq)
	c.rw.WriteByte(cmd)
	c.rw.Write(kl)
	c.rw.WriteByte(0x14)
	c.rw.WriteByte(zeroByte)
	c.rw.Write(zeroTwoBytes)
	c.rw.Write(tl)
	c.rw.Write(zeroFourBytes)
	c.rw.Write(zeroEightBytes)
	c.rw.Write(dl)
	c.rw.Write(zeroEightBytes)
	c.rw.Write(zeroFourBytes)
	c.rw.Write([]byte(key))

	if err := c.rw.Flush(); err != nil {
		return 0, c.fatal(err)
	}
	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	resp := make([]byte, 24)
	n, err := c.rw.Read(resp)
	if err != nil || n != 24 {
		return 0, c.fatal(err)
	}
	if resp[0] != magicResp {
		return 0, ErrNotResponse
	}
	ss := resp[6:8]
	if !bytes.Equal(ss, zeroTwoBytes) {
		return 0, fmt.Errorf("response error(%x)", ss)
	}

	bls := resp[8:12]
	bl := binary.BigEndian.Uint32(bls)

	if bl > 0 {
		vl := int(bl) - 20 // NOTE: 20 is extra len
		vs := make([]byte, vl)
		n, err := c.rw.Read(vs)
		if err != nil || vl != n {
			return 0, c.fatal(err)
		}
		return binary.BigEndian.Uint64(vs), nil
	}
	return 0, nil
}

func (c *Conn) Delete(key string) (err error) {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	kl := make([]byte, 2)
	binary.BigEndian.PutUint16(kl, uint16(len(key)))

	bl := make([]byte, 4)
	binary.BigEndian.PutUint32(bl, uint32(len(key)))

	c.rw.WriteByte(magicReq)
	c.rw.WriteByte(RequestTypeDelete)
	c.rw.Write(kl)
	c.rw.WriteByte(zeroByte)
	c.rw.WriteByte(zeroByte)
	c.rw.Write(zeroTwoBytes)
	c.rw.Write(bl)
	c.rw.Write(zeroFourBytes)
	c.rw.Write(zeroEightBytes)
	c.rw.Write([]byte(key))

	if err = c.rw.Flush(); err != nil {
		return c.fatal(err)
	}
	if c.readTimeout != 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	resp := make([]byte, 24)
	n, err := c.rw.Read(resp)
	if err != nil || n != 24 {
		return c.fatal(err)
	}
	if resp[0] != magicResp {
		return ErrNotResponse
	}
	ss := resp[6:8]
	if bytes.Equal(ss, zeroOneBytes) {
		return ErrNotFound
	}
	if !bytes.Equal(ss, zeroTwoBytes) {
		return fmt.Errorf("response error(%x)", ss)
	}

	return nil
}

func (c *Conn) Scan(item *Item, v interface{}) (err error) {
	c.ir.Reset(item.Value)
	if item.Flags&FlagGzip == FlagGzip {
		if err = c.gr.Reset(&c.ir); err != nil {
			return
		}
		if err = c.decode(&c.gr, item, v); err != nil {
			return
		}
		err = c.gr.Close()
	} else {
		err = c.decode(&c.ir, item, v)
	}
	return
}

func (c *Conn) encode(item *Item) (data []byte, err error) {
	if (item.Flags | _flagEncoding) == _flagEncoding {
		if item.Value == nil {
			return nil, ErrItem
		}
	} else if item.Object == nil {
		return nil, ErrItem
	}
	// encoding
	switch {
	case item.Flags&FlagGOB == FlagGOB:
		c.edb.Reset()
		if err = gob.NewEncoder(&c.edb).Encode(item.Object); err != nil {
			return
		}
		data = c.edb.Bytes()
	case item.Flags&FlagJSON == FlagJSON:
		c.edb.Reset()
		if err = c.je.Encode(item.Object); err != nil {
			return
		}
		data = c.edb.Bytes()
	default:
		data = item.Value
	}
	// compress
	if item.Flags&FlagGzip == FlagGzip {
		c.cb.Reset()
		c.gw.Reset(&c.cb)
		if _, err = c.gw.Write(data); err != nil {
			return
		}
		if err = c.gw.Close(); err != nil {
			return
		}
		data = c.cb.Bytes()
	}
	if len(data) > 8000000 {
		err = ErrValueSize
	}
	return
}

func (c *Conn) decode(rd io.Reader, item *Item, v interface{}) (err error) {
	var data []byte
	switch {
	case item.Flags&FlagGOB == FlagGOB:
		err = gob.NewDecoder(rd).Decode(v)
	case item.Flags&FlagJSON == FlagJSON:
		c.jr.Reset(rd)
		err = c.jd.Decode(v)
	default:
		data = item.Value
		if item.Flags&FlagGzip == FlagGzip {
			c.edb.Reset()
			if _, err = io.Copy(&c.edb, rd); err != nil {
				return
			}
			data = c.edb.Bytes()
		}
		switch v.(type) {
		case *[]byte:
			d := v.(*[]byte)
			*d = data
		case *string:
			d := v.(*string)
			*d = string(data)
		case interface{}:
			err = json.Unmarshal(data, v)
		}
	}
	return
}
