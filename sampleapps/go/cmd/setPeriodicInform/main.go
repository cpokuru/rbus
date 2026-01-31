// setPeriodicInform sets Device.ManagementServer.PeriodicInformInterval using the rbus wire protocol only:
//   - Connect to rtrouted (Unix socket), send HELLO, SUBSCRIBE to our inbox
//   - Optionally create session (METHOD_RPC to req_new_s), then set with that sessionId
//   - Send set: topic = element name, payload = MessagePack METHOD_SETPARAMETERVALUES (sessionId, componentName, needRollBack, paramCount, name, type, value, commit, meta)
//   - No rbuscli/dmcli; pure wire protocol (RTMessage + MessagePack) per rbus C API behavior.
//
// Usage:
//   ./setPeriodicInform [value]
//   RBUS_SOCKET_PATH=/tmp/rtrouted ./setPeriodicInform 152   # socket (default /tmp/rtrouted)
//   RBUS_SET_TOPIC=eRT.com.cisco.spvtg.ccsp.tr069pa ./setPeriodicInform 152   # set destination (default: same)
//   RBUS_SET_COMPONENT=SomeName ./setPeriodicInform 152   # requesting component in payload
//
// Connects to rtrouted at RBUS_SOCKET_PATH. Sends set to topic RBUS_SET_TOPIC (default = element name; rtrouted routes to eRT.com.cisco.spvtg.ccsp.tr069pa).
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"sampleapps/go/internal/rbus"
)


const (
	paramName    = "Device.ManagementServer.PeriodicInformInterval"
	defaultValue = 152
	// Set destination: element name (rbus sends to element; rtrouted routes to provider eRT.com.cisco.spvtg.ccsp.tr069pa). Override with RBUS_SET_TOPIC.
	defaultSetTopic = paramName
)

func main() {
	value := uint32(defaultValue)
	if len(os.Args) > 1 {
		v, err := strconv.ParseUint(os.Args[1], 10, 32)
		if err != nil {
			log.Fatalf("invalid value %q: %v", os.Args[1], err)
		}
		value = uint32(v)
	}

	// Connection identity (inbox, HELLO, subscribe)
	componentName := "rbus.setPeriodicInform"
	// Requesting component in set payload. TR-069 PA on many devices only accepts "rbuscli-<pid>"; use that so set succeeds. Override with RBUS_SET_COMPONENT.
	requestingComponent := os.Getenv("RBUS_SET_COMPONENT")
	if requestingComponent == "" {
		requestingComponent = fmt.Sprintf("rbuscli-%d", os.Getpid())
	}
	socketPath := os.Getenv("RBUS_SOCKET_PATH")
	if socketPath == "" {
		socketPath = "/tmp/rtrouted"
	}
	setDestination := os.Getenv("RBUS_SET_TOPIC")
	if setDestination == "" {
		setDestination = defaultSetTopic
	}
	log.Printf("setPeriodicInform: connect %s, set %s = %d (topic %q, requesting as %q)", socketPath, paramName, value, setDestination, requestingComponent)

	conn, err := rbus.NewConn(componentName, socketPath)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	var (
		mu      sync.Mutex
		pending = make(map[uint32]chan []byte)
	)
	conn.SetOnMessage(func(h *rbus.RTMessageHeader, payload []byte) {
		if !h.IsResponse() || !h.IsRawBinary() {
			return
		}
		mu.Lock()
		ch, ok := pending[h.Sequence]
		delete(pending, h.Sequence)
		mu.Unlock()
		if ok && ch != nil {
			select {
			case ch <- payload:
			default:
			}
		}
	})

	// Create session (rbus_createSession) so set request carries a valid sessionId; TR-069 PA may require it.
	sessionId := int32(0)
	reqSession, err := rbus.EncodeMethodInvokeRequest(rbus.SessionMgrRequestSession)
	if err != nil {
		log.Fatalf("encode session request: %v", err)
	}
	seqSession, err := conn.SendGetRequest(rbus.SessionMgrRequestSession, componentName, rbus.SessionMgrRequestSession, reqSession)
	if err != nil {
		log.Printf("create session send failed (continuing with sessionId 0): %v", err)
	} else {
		mu.Lock()
		chSession := make(chan []byte, 1)
		pending[seqSession] = chSession
		mu.Unlock()
		select {
		case respSession := <-chSession:
			if len(respSession) == 0 {
				log.Printf("create session: empty response (continuing with sessionId 0)")
			} else {
				rc, sid, errDec := rbus.DecodeSessionResponse(respSession)
				if errDec != nil {
					log.Printf("create session: decode response failed: %v (continuing with sessionId 0)", errDec)
				} else if rc != rbus.RBUS_ERROR_SUCCESS || sid == 0 {
					log.Printf("create session: returnCode=%d sessionId=%d (continuing with sessionId 0)", rc, sid)
				} else {
					sessionId = sid
					log.Printf("session created: %d", sessionId)
				}
			}
		case <-time.After(2 * time.Second):
			log.Printf("create session timeout (continuing with sessionId 0)")
		}
		mu.Lock()
		delete(pending, seqSession)
		mu.Unlock()
	}

	req, err := rbus.EncodeSetRequest(sessionId, requestingComponent, paramName, rbus.RBUS_UINT32, value)
	if err != nil {
		log.Fatalf("encode set request: %v", err)
	}
	seq, err := conn.SendGetRequest(setDestination, componentName, paramName, req)
	if err != nil {
		log.Fatalf("send set: %v", err)
	}

	mu.Lock()
	ch := make(chan []byte, 1)
	pending[seq] = ch
	mu.Unlock()

	select {
	case resp := <-ch:
		status, err := rbus.DecodeSetResponse(resp)
		if err != nil {
			log.Fatalf("decode response: %v", err)
		}
		if status != rbus.RBUS_ERROR_SUCCESS && status != 100 {
			hint := ""
			if status == 9003 {
				hint = " (parameter may be read-only on this device)"
			}
			log.Fatalf("set failed with status %d%s", status, hint)
		}
		fmt.Printf("OK: %s = %d\n", paramName, value)
	case <-time.After(5 * time.Second):
		log.Fatalf("set %s timeout", paramName)
	}

	mu.Lock()
	delete(pending, seq)
	mu.Unlock()

	log.Println("setPeriodicInform: done")
}
