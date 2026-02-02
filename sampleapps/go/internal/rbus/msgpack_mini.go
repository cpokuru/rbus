// Minimal MessagePack encoding for rbus get request/response (no external dependency).
package rbus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// msgpack type markers
const (
	mpFixStr  = 0xa0
	mpStr8    = 0xd9
	mpInt32   = 0xd2
	mpTrue    = 0xc3
	mpFalse   = 0xc2
	mpFloat64 = 0xcb
)

type mpEnc struct{ buf *bytes.Buffer }

func (e *mpEnc) writeStrWithNull(s string) {
	b := make([]byte, len(s)+1)
	copy(b, s)
	b[len(s)] = 0
	e.writeStrBytes(b)
}

func (e *mpEnc) writeStrBytes(b []byte) {
	n := len(b)
	if n <= 31 {
		e.buf.WriteByte(mpFixStr + byte(n))
	} else {
		e.buf.WriteByte(mpStr8)
		e.buf.WriteByte(byte(n))
	}
	e.buf.Write(b)
}

func (e *mpEnc) writeInt32(v int32) {
	e.buf.WriteByte(mpInt32)
	binary.Write(e.buf, binary.BigEndian, v)
}


func (e *mpEnc) writeUint16(v uint16) {
	e.buf.WriteByte(0xCD)
	e.buf.WriteByte(byte(v >> 8))
	e.buf.WriteByte(byte(v))
}

func (e *mpEnc) writeBin(b []byte) {
	n := len(b)
	if n <= 255 {
		e.buf.WriteByte(0xC4)
		e.buf.WriteByte(byte(n))
	} else {
		e.buf.WriteByte(0xC5)
		e.buf.WriteByte(byte(n >> 8))
		e.buf.WriteByte(byte(n))
	}
	e.buf.Write(b)
}

// writeSmallInt uses fixint for 0-127, otherwise int32
// This matches rbuscli's compact encoding
func (e *mpEnc) writeSmallInt(v int32) {
	if v >= 0 && v <= 127 {
		e.buf.WriteByte(byte(v))
	} else {
		e.buf.WriteByte(mpInt32) // 0xd2
		binary.Write(e.buf, binary.BigEndian, v)
	}
}

func (e *mpEnc) writeBool(v bool) {
	if v {
		e.buf.WriteByte(mpTrue)
	} else {
		e.buf.WriteByte(mpFalse)
	}
}

func (e *mpEnc) writeFloat64(v float64) {
	e.buf.WriteByte(mpFloat64)
	binary.Write(e.buf, binary.BigEndian, math.Float64bits(v))
}

type mpDec struct {
	r   io.Reader
	buf [8]byte
}

func (d *mpDec) readByte() (byte, error) {
	_, err := io.ReadFull(d.r, d.buf[:1])
	return d.buf[0], err
}

func (d *mpDec) readStr() (string, error) {
	b, err := d.readStrBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (d *mpDec) readStrBytes() ([]byte, error) {
	t, err := d.readByte()
	if err != nil {
		return nil, err
	}
	var n int
	
	// Handle fixstr (0xa0-0xbf)
	if t >= mpFixStr && t <= mpFixStr+31 {
		n = int(t - mpFixStr)
	} else if t == mpStr8 {  // str8 (0xd9)
		b, _ := d.readByte()
		n = int(b)
	} else if t == 0xc4 {  // bin8 (NEW: handle binary format)
		b, _ := d.readByte()
		n = int(b)
	} else if t == 0xc5 {  // bin16 (NEW: handle binary format)
		_, err := io.ReadFull(d.r, d.buf[:2])
		if err != nil {
			return nil, err
		}
		n = int(binary.BigEndian.Uint16(d.buf[:2]))
	} else if t == 0xc6 {  // bin32 (NEW: handle binary format)
		_, err := io.ReadFull(d.r, d.buf[:4])
		if err != nil {
			return nil, err
		}
		n = int(binary.BigEndian.Uint32(d.buf[:4]))
	} else {
		return nil, fmt.Errorf("msgpack: unexpected str type 0x%02x", t)
	}
	
	b := make([]byte, n)
	_, err = io.ReadFull(d.r, b)
	return b, err
}
func (d *mpDec) readInt32() (int32, error) {
	t, err := d.readByte()
	if err != nil {
		return 0, err
	}
	// MessagePack: fixint, int8/16/32/64, uint8/16/32 (providers may use any for status)
	if t <= 0x7f {
		return int32(t), nil
	}
	if t >= 0xe0 && t <= 0xff {
		return int32(int8(t)), nil
	}
	switch t {
	case 0xcc: // uint8
		_, err = io.ReadFull(d.r, d.buf[:1])
		if err != nil {
			return 0, err
		}
		return int32(d.buf[0]), nil
	case 0xcd: // uint16
		_, err = io.ReadFull(d.r, d.buf[:2])
		if err != nil {
			return 0, err
		}
		return int32(binary.BigEndian.Uint16(d.buf[:2])), nil
	case 0xce: // uint32
		_, err = io.ReadFull(d.r, d.buf[:4])
		if err != nil {
			return 0, err
		}
		return int32(binary.BigEndian.Uint32(d.buf[:4])), nil
	case 0xd0: // int8
		_, err = io.ReadFull(d.r, d.buf[:1])
		if err != nil {
			return 0, err
		}
		return int32(int8(d.buf[0])), nil
	case 0xd1: // int16
		_, err = io.ReadFull(d.r, d.buf[:2])
		if err != nil {
			return 0, err
		}
		return int32(int16(binary.BigEndian.Uint16(d.buf[:2]))), nil
	case mpInt32: // 0xd2 int32
		_, err = io.ReadFull(d.r, d.buf[:4])
		if err != nil {
			return 0, err
		}
		return int32(binary.BigEndian.Uint32(d.buf[:4])), nil
	case 0xd3: // int64
		_, err = io.ReadFull(d.r, d.buf[:8])
		if err != nil {
			return 0, err
		}
		return int32(binary.BigEndian.Uint64(d.buf[:8])), nil
	default:
		return 0, fmt.Errorf("msgpack: expected int32, got 0x%02x", t)
	}
}

func (d *mpDec) readBool() (bool, error) {
	t, err := d.readByte()
	if err != nil {
		return false, err
	}
	switch t {
	case mpTrue:
		return true, nil
	case mpFalse:
		return false, nil
	case 0xc4: // bin8 - some components send "true"/"false" as binary
		length, err := d.readByte()
		if err != nil {
			return false, err
		}
		b := make([]byte, length)
		_, err = io.ReadFull(d.r, b)
		if err != nil {
			return false, err
		}
		s := trimNull(string(b))
		return s == "true" || s == "1", nil
	case 0xc5: // bin16
		_, err := io.ReadFull(d.r, d.buf[:2])
		if err != nil {
			return false, err
		}
		length := binary.BigEndian.Uint16(d.buf[:2])
		b := make([]byte, length)
		_, err = io.ReadFull(d.r, b)
		if err != nil {
			return false, err
		}
		s := trimNull(string(b))
		return s == "true" || s == "1", nil
	default:
		return false, fmt.Errorf("msgpack: expected bool, got 0x%02x", t)
	}
}

func (d *mpDec) readFloat64() (float64, error) {
	t, err := d.readByte()
	if err != nil {
		return 0, err
	}
	if t != mpFloat64 {
		return 0, fmt.Errorf("msgpack: expected float64, got 0x%02x", t)
	}
	_, err = io.ReadFull(d.r, d.buf[:8])
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(d.buf[:8])), nil
}
