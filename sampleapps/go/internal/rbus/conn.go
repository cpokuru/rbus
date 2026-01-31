package rbus

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultSocketPath = "/tmp/rtrouted"

// Conn is a connection to rtrouted over Unix domain socket.
type Conn struct {
	conn           net.Conn
	appName        string
	inbox          string
	seq            uint32
	mu             sync.Mutex
	responses      map[uint32]chan []byte
	controlPending map[uint32]chan []byte // for discovery / control request-response
	onMessage      func(h *RTMessageHeader, payload []byte)
}

// NewConn connects to rtrouted and registers the inbox. appName is used for inbox: appName.prog.INBOX.pid
func NewConn(appName string, socketPath string) (*Conn, error) {
	if socketPath == "" {
		socketPath = defaultSocketPath
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", socketPath, err)
	}
	
	pid := os.Getpid()
	var inbox string
	var shortName string
	
	// Check if this is rbuscli mode
	isRbuscli := strings.HasPrefix(appName, "rbuscli-") && len(appName) > len("rbuscli-")
	if isRbuscli {
		if _, err := strconv.Atoi(appName[len("rbuscli-"):]); err == nil {
			// rbuscli mode: use long format as inbox, but also register short name
			shortName = appName
			inbox = fmt.Sprintf("rbus.rbuscli.INBOX.%d", pid)
		} else {
			inbox = fmt.Sprintf("%s.INBOX.%d", appName, pid)
		}
	} else {
		inbox = fmt.Sprintf("%s.INBOX.%d", appName, pid)
	}
	
	c := &Conn{
		conn:           conn,
		appName:        appName,
		inbox:          inbox,
		responses:      make(map[uint32]chan []byte),
		controlPending: make(map[uint32]chan []byte),
	}
	
	// Some rtrouted builds expect HELLO first to register the client; payload is JSON {"inbox": "<our inbox>"} + null.
	helloPayload, _ := json.Marshal(map[string]string{"inbox": inbox})
	helloPayload = append(helloPayload, 0)
	if err := c.sendMessage(HelloControlTopic, "", rtMessageFlagsReq, helloPayload); err != nil {
		conn.Close()
		return nil, fmt.Errorf("hello: %w", err)
	}
	
	// Subscribe our inbox so we can receive responses (route_id 1)
	if err := c.subscribe(inbox, 1); err != nil {
		conn.Close()
		return nil, fmt.Errorf("subscribe inbox: %w", err)
	}
	
	// For rbuscli mode, also subscribe to the short name
	// This makes us appear as both "rbus.rbuscli.INBOX.<pid>" and "rbuscli-<pid>" in discallcomponents
	if shortName != "" && shortName != inbox {
		if err := c.subscribe(shortName, 1); err != nil {
			conn.Close()
			return nil, fmt.Errorf("subscribe shortName: %w", err)
		}
	}
	
	// Create rbus file (some providers like PAM check this for authentication)
	createRbusFile()
	
	go c.readLoop()
	return c, nil
}

// createRbusFile creates /tmp/.rbus/<pid>_1 that PAM and other providers may check
func createRbusFile() {
	pid := os.Getpid()
	dir := "/tmp/.rbus"
	os.MkdirAll(dir, 0755)
	filename := fmt.Sprintf("%s/%d_1", dir, pid)
	if f, err := os.Create(filename); err == nil {
		f.Close()
	}
}

// SubscribeTopic adds an alias to the same route (route_id 1) so we receive messages for this topic.
func (c *Conn) SubscribeTopic(topic string) error {
	return c.subscribe(topic, 1)
}

// SubscribeControlTopic is the topic used to send subscribe/unsubscribe control messages to rtrouted.
const SubscribeControlTopic = "_RTROUTED.INBOX.SUBSCRIBE"

// HelloControlTopic is the topic used to register the client's inbox with rtrouted (some builds expect this first).
const HelloControlTopic = "_RTROUTED.INBOX.HELLO"

// subscribe sends a control message TO rtrouted at _RTROUTED.INBOX.SUBSCRIBE; payload JSON has "topic": <expression we want to receive>.
// rtrouted uses cJSON_Parse() which expects a null-terminated string, so we append \0.
func (c *Conn) subscribe(expression string, routeID int) error {
	payload := map[string]interface{}{
		"add":      1,
		"topic":    expression,
		"route_id": routeID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	body = append(body, 0) // cJSON_Parse expects null-terminated string
	return c.sendMessage(SubscribeControlTopic, "", rtMessageFlagsReq, body)
}

func (c *Conn) sendMessage(topic, replyTopic string, flags uint32, payload []byte) error {
	c.mu.Lock()
	seq := atomic.AddUint32(&c.seq, 1)
	c.mu.Unlock()

	h := &RTMessageHeader{
		Version:       HeaderVersion,
		Sequence:      seq,
		Flags:         flags,
		PayloadLength: uint32(len(payload)),
		Topic:         topic,
		ReplyTopic:    replyTopic,
	}
	// Build header + payload into one buffer so we can send in one write
	buf := make([]byte, 0, 512+len(payload))
	hw := &bufferWriter{b: &buf}
	if err := EncodeHeader(hw, h); err != nil {
		return err
	}
	buf = append(buf, payload...)

	if debugLog() {
		log.Printf("rbus: send topic=%q reply=%q flags=0x%x seq=%d len=%d", topic, replyTopic, flags, seq, len(payload))
	}
	c.mu.Lock()
	_, err := c.conn.Write(buf)
	c.mu.Unlock()
	return err
}

// SendGetRequest sends an RBus get request (MessagePack payload) to topic (element name), reply goes to our inbox.
func (c *Conn) SendGetRequest(topic string, componentName string, paramName string, payload []byte) (seq uint32, err error) {
	c.mu.Lock()
	seq = atomic.AddUint32(&c.seq, 1)
	c.mu.Unlock()

	h := &RTMessageHeader{
		Version:       HeaderVersion,
		Sequence:      seq,
		Flags:         rtMessageFlagsReq | rtMessageFlagsRaw,
		PayloadLength: uint32(len(payload)),
		Topic:         topic,
		ReplyTopic:    c.inbox,
	}
	buf := make([]byte, 0, 512+len(payload))
	hw := &bufferWriter{b: &buf}
	if err := EncodeHeader(hw, h); err != nil {
		return 0, err
	}
	buf = append(buf, payload...)

	if debugLog() {
		log.Printf("rbus: send get topic=%q reply=%q flags=0x11 seq=%d", topic, c.inbox, seq)
	}
	c.mu.Lock()
	_, err = c.conn.Write(buf)
	c.mu.Unlock()
	return seq, err
}

// SendSetRequest sends an RBus set request (MessagePack payload) to topic (parameter name).
func (c *Conn) SendSetRequest(topic string, payload []byte) (seq uint32, err error) {
	c.mu.Lock()
	seq = atomic.AddUint32(&c.seq, 1)
	c.mu.Unlock()

	h := &RTMessageHeader{
		Version:       HeaderVersion,
		Sequence:      seq,
		Flags:         rtMessageFlagsReq | rtMessageFlagsRaw,
		PayloadLength: uint32(len(payload)),
		Topic:         topic,
		ReplyTopic:    c.inbox,
	}
	buf := make([]byte, 0, 512+len(payload))
	hw := &bufferWriter{b: &buf}
	if err := EncodeHeader(hw, h); err != nil {
		return 0, err
	}
	buf = append(buf, payload...)

	if debugLog() {
		log.Printf("rbus: send set topic=%q reply=%q flags=0x11 seq=%d", topic, c.inbox, seq)
	}
	c.mu.Lock()
	_, err = c.conn.Write(buf)
	c.mu.Unlock()
	return seq, err
}

// SendMethodInvoke sends an RBus method invoke request (e.g. for session creation).
func (c *Conn) SendMethodInvoke(topic string, payload []byte) (seq uint32, err error) {
	c.mu.Lock()
	seq = atomic.AddUint32(&c.seq, 1)
	c.mu.Unlock()

	h := &RTMessageHeader{
		Version:       HeaderVersion,
		Sequence:      seq,
		Flags:         rtMessageFlagsReq | rtMessageFlagsRaw,
		PayloadLength: uint32(len(payload)),
		Topic:         topic,
		ReplyTopic:    c.inbox,
	}
	buf := make([]byte, 0, 512+len(payload))
	hw := &bufferWriter{b: &buf}
	if err := EncodeHeader(hw, h); err != nil {
		return 0, err
	}
	buf = append(buf, payload...)

	if debugLog() {
		log.Printf("rbus: send method invoke topic=%q reply=%q flags=0x11 seq=%d", topic, c.inbox, seq)
	}
	c.mu.Lock()
	_, err = c.conn.Write(buf)
	c.mu.Unlock()
	return seq, err
}

// SendResponse sends an RBus response to replyTopic (from request header).
func (c *Conn) SendResponse(replyTopic string, requestTopic string, sequence uint32, payload []byte) error {
	h := &RTMessageHeader{
		Version:       HeaderVersion,
		Sequence:      sequence,
		Flags:         rtMessageFlagsResp | rtMessageFlagsRaw,
		PayloadLength: uint32(len(payload)),
		Topic:         replyTopic,
		ReplyTopic:    requestTopic,
	}
	buf := make([]byte, 0, 512+len(payload))
	hw := &bufferWriter{b: &buf}
	if err := EncodeHeader(hw, h); err != nil {
		return err
	}
	buf = append(buf, payload...)
	c.mu.Lock()
	_, err := c.conn.Write(buf)
	c.mu.Unlock()
	return err
}

// SetOnMessage sets callback for received messages (used by provider to handle get requests).
func (c *Conn) SetOnMessage(fn func(h *RTMessageHeader, payload []byte)) {
	c.mu.Lock()
	c.onMessage = fn
	c.mu.Unlock()
}

// Inbox returns the inbox topic for this connection.
func (c *Conn) Inbox() string { return c.inbox }

func (c *Conn) readLoop() {
	for {
		var h RTMessageHeader
		if err := DecodeHeader(c.conn, &h); err != nil {
			if debugLog() {
				log.Printf("rbus: readLoop exit (DecodeHeader): %v", err)
			}
			return
		}
		var payload []byte
		if h.PayloadLength > 0 {
			payload = make([]byte, h.PayloadLength)
			if _, err := io.ReadFull(c.conn, payload); err != nil {
				if debugLog() {
					log.Printf("rbus: readLoop exit (ReadFull payload): %v", err)
				}
				return
			}
		}
		if debugLog() {
			log.Printf("rbus: recv topic=%q reply=%q flags=0x%x seq=%d len=%d", h.Topic, h.ReplyTopic, h.Flags, h.Sequence, h.PayloadLength)
		}
		// Control request-response: deliver response to waiter
		if h.IsResponse() {
			c.mu.Lock()
			ch, ok := c.controlPending[h.Sequence]
			delete(c.controlPending, h.Sequence)
			c.mu.Unlock()
			if ok && ch != nil {
				select {
				case ch <- payload:
				default:
				}
			}
		}
		c.mu.Lock()
		fn := c.onMessage
		c.mu.Unlock()
		if fn != nil {
			fn(&h, payload)
		}
	}
}

// SendControlRequest sends a control message (JSON, flags=Request) and waits for the JSON response.
// Used for discovery (e.g. _RTROUTED.INBOX.QUERY, _registered_components). Payload should be JSON + null.
func (c *Conn) SendControlRequest(topic string, payload []byte, timeout time.Duration) ([]byte, error) {
	c.mu.Lock()
	seq := atomic.AddUint32(&c.seq, 1)
	ch := make(chan []byte, 1)
	c.controlPending[seq] = ch
	c.mu.Unlock()

	h := &RTMessageHeader{
		Version:       HeaderVersion,
		Sequence:      seq,
		Flags:         rtMessageFlagsReq,
		PayloadLength: uint32(len(payload)),
		Topic:         topic,
		ReplyTopic:    c.inbox,
	}
	buf := make([]byte, 0, 512+len(payload))
	hw := &bufferWriter{b: &buf}
	if err := EncodeHeader(hw, h); err != nil {
		c.mu.Lock()
		delete(c.controlPending, seq)
		c.mu.Unlock()
		return nil, err
	}
	buf = append(buf, payload...)
	c.mu.Lock()
	_, err := c.conn.Write(buf)
	c.mu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.controlPending, seq)
		c.mu.Unlock()
		return nil, err
	}

	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.controlPending, seq)
		c.mu.Unlock()
		return nil, fmt.Errorf("control request timeout")
	}
}

func debugLog() bool {
	return os.Getenv("RBUS_DEBUG") == "1"
}

// Close closes the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

type bufferWriter struct{ b *[]byte }

func (w *bufferWriter) Write(p []byte) (n int, err error) {
	*w.b = append(*w.b, p...)
	return len(p), nil
}
