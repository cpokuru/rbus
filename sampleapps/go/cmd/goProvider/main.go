// goProvider is a minimal rbus sample provider in Go.
// It registers the same 6 parameters as rbusSampleProvider and responds to get requests.
// Run rtrouted first, then: go run ./cmd/goProvider
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"sampleapps/go/internal/rbus"
)

var paramValues = map[string]interface{}{
	"Device.DeviceInfo.SampleProvider.Manufacturer":    "COMCAST",
	"Device.DeviceInfo.SampleProvider.ModelName":       "XB3",
	"Device.DeviceInfo.SampleProvider.SoftwareVersion": float32(2.14),
	"Device.SampleProvider.SampleData.IntData":         int32(0),
	"Device.SampleProvider.SampleData.BoolData":        false,
	"Device.SampleProvider.SampleData.UIntData":        uint32(0),
}

var paramTypes = map[string]int32{
	"Device.DeviceInfo.SampleProvider.Manufacturer":    rbus.RBUS_STRING,
	"Device.DeviceInfo.SampleProvider.ModelName":       rbus.RBUS_STRING,
	"Device.DeviceInfo.SampleProvider.SoftwareVersion": rbus.RBUS_SINGLE,
	"Device.SampleProvider.SampleData.IntData":         rbus.RBUS_INT32,
	"Device.SampleProvider.SampleData.BoolData":        rbus.RBUS_BOOLEAN,
	"Device.SampleProvider.SampleData.UIntData":        rbus.RBUS_UINT32,
}

func main() {
	log.Println("goProvider: start")

	conn, err := rbus.NewConn("rbus.goProvider", "")
	if err != nil {
		log.Fatalf("goProvider: connect failed: %v", err)
	}
	defer conn.Close()

	// Subscribe to each element topic (same route_id = 1, so rtrouted delivers get requests to us)
	for topic := range paramValues {
		if err := conn.SubscribeTopic(topic); err != nil {
			log.Printf("goProvider: subscribe %s: %v", topic, err)
		}
	}
	log.Println("goProvider: subscribed to element topics")

	conn.SetOnMessage(func(h *rbus.RTMessageHeader, payload []byte) {
		// Debug: uncomment to see every message the provider receives
		// log.Printf("goProvider: recv topic=%q flags=0x%x payload_len=%d", h.Topic, h.Flags, h.PayloadLength)
		if !h.IsRequest() || !h.IsRawBinary() {
			return
		}
		component, parameter, err := rbus.DecodeGetRequest(payload)
		if err != nil {
			log.Printf("goProvider: decode get request: %v", err)
			return
		}
		_ = component
		val, ok := paramValues[parameter]
		if !ok {
			log.Printf("goProvider: unknown parameter %s", parameter)
			resp, _ := rbus.EncodeGetResponse(5, parameter, rbus.RBUS_STRING, "") // DESTINATION_NOT_FOUND
			_ = conn.SendResponse(h.ReplyTopic, h.Topic, h.Sequence, resp)
			return
		}
		vt := paramTypes[parameter]
		resp, err := rbus.EncodeGetResponse(rbus.RBUS_ERROR_SUCCESS, parameter, vt, val)
		if err != nil {
			log.Printf("goProvider: encode response: %v", err)
			return
		}
		if err := conn.SendResponse(h.ReplyTopic, h.Topic, h.Sequence, resp); err != nil {
			log.Printf("goProvider: send response: %v", err)
		} else {
			log.Printf("goProvider: get [%s] -> %v", parameter, val)
		}
	})

	log.Println("goProvider: ready. Press Ctrl+C to exit.")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("goProvider: exit")
}
