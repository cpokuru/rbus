// getUserPassword gets (and optionally sets) Device.Users.User.2.X_CISCO_COM_Password via rbus.
//
// Connects to rtrouted (Unix socket), sends get/set using the wire protocol only:
// RTMessage (topic, reply topic, binary payload) + MessagePack (METHOD_GETPARAMETERVALUES /
// METHOD_SETPARAMETERVALUES). No rbuscli; pure API: NewConn, EncodeGetRequest, EncodeSetRequest,
// SendGetRequest, DecodeGetResponse, DecodeSetResponse.
//
// Usage:
//   ./getUserPassword           # get and print current value
//   ./getUserPassword newpass  # set then get to confirm
//
//   RBUS_SOCKET_PATH=/tmp/rtrouted ./getUserPassword
//   RBUS_SET_COMPONENT=rbuscli-12345 ./getUserPassword newpass   # override requesting component for set
//   RBUS_SET_TOPIC=eRT.com.cisco.spvtg.ccsp.CR ./getUserPassword newpass   # send set to component (default: element name)
//
// Requires rtrouted running. If set returns 9003, the provider (CR) may only accept set from rbuscli/dmcli on this device.
package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"sampleapps/go/internal/rbus"
)

const paramName = "Device.Users.User.2.X_CISCO_COM_Password"

func main() {
	doSet := len(os.Args) > 1
	// When doing a set, match rbuscli exactly: strace shows rbuscli (1) subscribes to rbus.rbuscli.INBOX.<pid>,
	// (2) subscribes to rbuscli-<pid>, (3) sends set with ReplyTopic = rbus.rbuscli.INBOX.<pid>. So we use
	// appName "rbus.rbuscli" so our inbox/ReplyTopic is rbus.rbuscli.INBOX.<pid>, then add second subscription rbuscli-<pid>.
	appName := "rbus.getUserPassword"
	if doSet {
		appName = "rbus.rbuscli"
	}
	socketPath := os.Getenv("RBUS_SOCKET_PATH")
	if socketPath == "" {
		socketPath = "/tmp/rtrouted"
	}
	log.Printf("getUserPassword: connecting to %s as %s", socketPath, appName)

	conn, err := rbus.NewConn(appName, socketPath)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer conn.Close()

	if doSet {
		// Second subscription like rbuscli (strace: add topic "rbuscli-<pid>" after "rbus.rbuscli.INBOX.<pid>").
		if err := conn.SubscribeTopic(fmt.Sprintf("rbuscli-%d", os.Getpid())); err != nil {
			log.Printf("subscribe rbuscli-<pid> failed: %v (continuing)", err)
		}
	}

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

	doGet := func() (string, error) {
		req, err := rbus.EncodeGetRequest(appName, paramName)
		if err != nil {
			return "", fmt.Errorf("encode request: %w", err)
		}
		seq, err := conn.SendGetRequest(paramName, appName, paramName, req)
		if err != nil {
			return "", fmt.Errorf("send get: %w", err)
		}
		mu.Lock()
		ch := make(chan []byte, 1)
		pending[seq] = ch
		mu.Unlock()
		defer func() {
			mu.Lock()
			delete(pending, seq)
			mu.Unlock()
		}()
		select {
		case resp := <-ch:
			status, _, value, err := rbus.DecodeGetResponse(resp)
			if err != nil {
				return "", fmt.Errorf("decode response: %w", err)
			}
			if status != rbus.RBUS_ERROR_SUCCESS && status != 100 {
				return "", fmt.Errorf("get failed with status %d", status)
			}
			s, _ := value.(string)
			return s, nil
		case <-time.After(5 * time.Second):
			return "", fmt.Errorf("get %s timeout", paramName)
		}
	}

	if doSet {
		newVal := os.Args[1]
		requestingComponent := os.Getenv("RBUS_SET_COMPONENT")
		if requestingComponent == "" {
			requestingComponent = fmt.Sprintf("rbuscli-%d", os.Getpid())
		}
		sessionId := int32(0)
		reqSession, err := rbus.EncodeMethodInvokeRequest(rbus.SessionMgrRequestSession)
		if err == nil {
			seqSession, errSend := conn.SendGetRequest(rbus.SessionMgrRequestSession, appName, rbus.SessionMgrRequestSession, reqSession)
			if errSend == nil {
				mu.Lock()
				chSession := make(chan []byte, 1)
				pending[seqSession] = chSession
				mu.Unlock()
				select {
				case respSession := <-chSession:
					if len(respSession) > 0 {
						rc, sid, errDec := rbus.DecodeSessionResponse(respSession)
						if errDec == nil && rc == rbus.RBUS_ERROR_SUCCESS && sid != 0 {
							sessionId = sid
							log.Printf("session created: %d", sessionId)
						}
					}
				case <-time.After(2 * time.Second):
				}
				mu.Lock()
				delete(pending, seqSession)
				mu.Unlock()
			}
		}
		req, err := rbus.EncodeSetRequest(sessionId, requestingComponent, paramName, rbus.RBUS_STRING, newVal)
		if err != nil {
			log.Fatalf("encode set request: %v", err)
		}
		setTopic := os.Getenv("RBUS_SET_TOPIC")
		if setTopic == "" {
			setTopic = paramName
		}
		seq, err := conn.SendGetRequest(setTopic, appName, paramName, req)
		if err != nil {
			log.Fatalf("send set: %v", err)
		}
		mu.Lock()
		ch := make(chan []byte, 1)
		pending[seq] = ch
		mu.Unlock()
		select {
		case resp := <-ch:
			mu.Lock()
			delete(pending, seq)
			mu.Unlock()
			status, err := rbus.DecodeSetResponse(resp)
			if err != nil {
				log.Fatalf("decode set response: %v", err)
			}
			if status != rbus.RBUS_ERROR_SUCCESS && status != 100 {
				log.Fatalf("set failed with status %d", status)
			}
			log.Printf("set OK: %s = %q", paramName, newVal)
		case <-time.After(5 * time.Second):
			mu.Lock()
			delete(pending, seq)
			mu.Unlock()
			log.Fatalf("set %s timeout", paramName)
		}
	}

	val, err := doGet()
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Println(val)
	log.Println("getUserPassword: done")
}
