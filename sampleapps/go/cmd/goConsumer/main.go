// goConsumer is a minimal rbus sample consumer in Go.
// With no args: gets the same 6 parameters as rbusSampleConsumer.
// With args: gets each parameter by name (like rbuscli getvalues), e.g.:
//   ./goConsumer Device.DeviceInfo.Manufacturer
//   ./goConsumer Device.DeviceInfo.Manufacturer Device.DeviceInfo.ModelName
package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"sampleapps/go/internal/rbus"
)

var defaultParamNames = []string{
	"Device.DeviceInfo.SampleProvider.Manufacturer",
	"Device.DeviceInfo.SampleProvider.ModelName",
	"Device.DeviceInfo.SampleProvider.SoftwareVersion",
	"Device.SampleProvider.SampleData.IntData",
	"Device.SampleProvider.SampleData.BoolData",
	"Device.SampleProvider.SampleData.UIntData",
}

func valueTypeString(v interface{}) string {
	switch v.(type) {
	case string:
		return "string"
	case int32:
		return "int32"
	case uint32:
		return "uint32"
	case bool:
		return "boolean"
	case float32:
		return "float"
	case float64:
		return "double"
	default:
		return "unknown"
	}
}

func main() {
	var paramNames []string
	if len(os.Args) > 1 {
		paramNames = os.Args[1:]
	} else {
		paramNames = defaultParamNames
	}

	log.Println("goConsumer: start")

	conn, err := rbus.NewConn("rbus.goConsumer", "")
	if err != nil {
		log.Fatalf("goConsumer: connect failed: %v", err)
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

	componentName := "rbus.goConsumer"
	for i, name := range paramNames {
		req, err := rbus.EncodeGetRequest(componentName, name)
		if err != nil {
			log.Printf("goConsumer: encode get request: %v", err)
			continue
		}
		seq, err := conn.SendGetRequest(name, componentName, name, req)
		if err != nil {
			log.Printf("goConsumer: send get [%s]: %v", name, err)
			continue
		}

		mu.Lock()
		ch := make(chan []byte, 1)
		pending[seq] = ch
		mu.Unlock()

		select {
		case resp := <-ch:
			status, paramName, value, err := rbus.DecodeGetResponse(resp)
			if err != nil {
				log.Printf("goConsumer: decode response [%s]: %v", name, err)
				continue
			}
			if status != rbus.RBUS_ERROR_SUCCESS && status != 100 {
				log.Printf("goConsumer: get [%s] status=%d", name, status)
				continue
			}
			// rbuscli-style output
			if len(paramNames) == len(defaultParamNames) && paramNames[0] == defaultParamNames[0] {
				// Original 6-param mode: short labels
				switch i {
				case 0:
					fmt.Printf("Manufacturer name = [%v]\n", value)
				case 1:
					fmt.Printf("Model name = [%v]\n", value)
				case 2:
					fmt.Printf("Software Version = [%v]\n", value)
				case 3:
					fmt.Printf("IntData = [%v]\n", value)
				case 4:
					fmt.Printf("BoolData = [%v]\n", value)
				case 5:
					fmt.Printf("UIntData = [%v]\n", value)
				default:
					fmt.Printf("Parameter %d:\n\tName  : %s\n\tType  : %s\n\tValue : %v\n", i+1, paramName, valueTypeString(value), value)
				}
			} else {
				fmt.Printf("Parameter  %d:\n              Name  : %s\n              Type  : %s\n              Value : %v\n", i+1, paramName, valueTypeString(value), value)
			}
		case <-time.After(5 * time.Second):
			log.Printf("goConsumer: get [%s] timeout", name)
		}

		mu.Lock()
		delete(pending, seq)
		mu.Unlock()
	}

	log.Println("goConsumer: exit")
}
