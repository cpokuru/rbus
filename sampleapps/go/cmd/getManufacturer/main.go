// getManufacturer is a minimal Go consumer that gets Device.DeviceInfo.Manufacturer from rbus.
//
// Usage:
//   go run .   # or ./getManufacturer
//
// Requires rtrouted running. The parameter is served by the platform provider (e.g. PAM).
package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"sampleapps/go/internal/rbus"
)

const paramName = "Device.DeviceInfo.Manufacturer"

func main() {
	appName := "rbus.getManufacturer"
	log.Println("getManufacturer: connecting to rtrouted")

	conn, err := rbus.NewConn(appName, "")
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

	req, err := rbus.EncodeGetRequest(appName, paramName)
	if err != nil {
		log.Fatalf("encode request: %v", err)
	}
	seq, err := conn.SendGetRequest(paramName, appName, paramName, req)
	if err != nil {
		log.Fatalf("send get: %v", err)
	}

	mu.Lock()
	ch := make(chan []byte, 1)
	pending[seq] = ch
	mu.Unlock()

	select {
	case resp := <-ch:
		status, _, value, err := rbus.DecodeGetResponse(resp)
		if err != nil {
			log.Fatalf("decode response: %v", err)
		}
		if status != rbus.RBUS_ERROR_SUCCESS && status != 100 {
			log.Fatalf("get failed with status %d", status)
		}
		fmt.Println(value)
	case <-time.After(5 * time.Second):
		log.Fatalf("get %s timeout", paramName)
	}

	mu.Lock()
	delete(pending, seq)
	mu.Unlock()

	log.Println("getManufacturer: done")
}
