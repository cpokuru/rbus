// Package rbus implements RTMessage wire format and rbus control/application messages.
package rbus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	HeaderMarker       = 0xAAAA
	HeaderVersion      = 2
	MaxTopicLength     = 256
	rtMessageFlagsReq  = 0x01
	rtMessageFlagsResp = 0x02
	rtMessageFlagsRaw  = 0x10
)

// RTMessageHeader is the binary header (without payload).
type RTMessageHeader struct {
	Version       uint16
	HeaderLength  uint16
	Sequence      uint32
	Flags         uint32
	ControlData   uint32
	PayloadLength uint32
	Topic         string
	ReplyTopic    string
}

// RoundtripSize is the optional T1–T5 block size (20 bytes) for MSG_ROUNDTRIP_TIME builds.
// Sending it ensures compatibility with RDK/system rtrouted built with MSG_ROUNDTRIP_TIME.
const RoundtripSize = 20

// EncodeHeader encodes the header to w (big-endian). Includes optional roundtrip block (T1–T5 = 0)
// so rtrouted built with MSG_ROUNDTRIP_TIME can decode correctly.
func EncodeHeader(w io.Writer, h *RTMessageHeader) error {
	topicLen := uint32(len(h.Topic))
	replyLen := uint32(len(h.ReplyTopic))
	h.HeaderLength = 32 + RoundtripSize + uint16(topicLen+replyLen)

	if err := binary.Write(w, binary.BigEndian, uint16(HeaderMarker)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, h.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, h.HeaderLength); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, h.Sequence); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, h.Flags); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, h.ControlData); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, h.PayloadLength); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, topicLen); err != nil {
		return err
	}
	if _, err := w.Write([]byte(h.Topic)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, replyLen); err != nil {
		return err
	}
	if _, err := w.Write([]byte(h.ReplyTopic)); err != nil {
		return err
	}
	// Optional roundtrip block (T1–T5) for MSG_ROUNDTRIP_TIME builds; use zeros.
	for i := 0; i < RoundtripSize/4; i++ {
		if err := binary.Write(w, binary.BigEndian, uint32(0)); err != nil {
			return err
		}
	}
	return binary.Write(w, binary.BigEndian, uint16(HeaderMarker))
}

// DecodeHeader reads and decodes the header from r. Payload (h.PayloadLength bytes) must be read by caller.
func DecodeHeader(r io.Reader, h *RTMessageHeader) error {
	var marker uint16
	if err := binary.Read(r, binary.BigEndian, &marker); err != nil {
		return err
	}
	if marker != HeaderMarker {
		return fmt.Errorf("invalid header marker: 0x%x", marker)
	}
	if err := binary.Read(r, binary.BigEndian, &h.Version); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &h.HeaderLength); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &h.Sequence); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &h.Flags); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &h.ControlData); err != nil {
		return err
	}
	if err := binary.Read(r, binary.BigEndian, &h.PayloadLength); err != nil {
		return err
	}
	var topicLen uint32
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return err
	}
	if topicLen > MaxTopicLength {
		return errors.New("topic length exceeds max")
	}
	topic := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topic); err != nil {
		return err
	}
	h.Topic = string(topic)
	var replyLen uint32
	if err := binary.Read(r, binary.BigEndian, &replyLen); err != nil {
		return err
	}
	if replyLen > MaxTopicLength {
		return errors.New("reply_topic length exceeds max")
	}
	reply := make([]byte, replyLen)
	if _, err := io.ReadFull(r, reply); err != nil {
		return err
	}
	h.ReplyTopic = string(reply)
	// We've read 30 + topicLen + replyLen so far. Header ends with optional T1–T5 (20) then end marker (2).
	// So remaining = header_length - (30 + topicLen + replyLen). Skip all but last 2 bytes (end marker).
	remaining := int(h.HeaderLength) - 30 - int(topicLen) - int(replyLen)
	if remaining > 2 {
		io.CopyN(io.Discard, r, int64(remaining-2))
	}
	var endMarker uint16
	if err := binary.Read(r, binary.BigEndian, &endMarker); err != nil {
		return err
	}
	if endMarker != HeaderMarker {
		return fmt.Errorf("invalid end marker: 0x%x", endMarker)
	}
	return nil
}

// IsRequest returns true if the message is a request.
func (h *RTMessageHeader) IsRequest() bool { return h.Flags&rtMessageFlagsReq != 0 }

// IsResponse returns true if the message is a response.
func (h *RTMessageHeader) IsResponse() bool { return h.Flags&rtMessageFlagsResp != 0 }

// IsRawBinary returns true if payload is MessagePack (rbus application).
func (h *RTMessageHeader) IsRawBinary() bool { return h.Flags&rtMessageFlagsRaw != 0 }
