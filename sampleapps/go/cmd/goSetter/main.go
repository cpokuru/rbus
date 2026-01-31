package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
	"sampleapps/go/internal/rbus"
)

func createSession(conn *rbus.Conn) (int32, error) {
	// Encode req_new_s method invoke to _rbus_session_mgr
	payload := encodeSessionRequest()
	
	seq, err := conn.SendMethodInvoke("_rbus_session_mgr", payload)
	if err != nil {
		return 0, fmt.Errorf("send session request: %w", err)
	}
	
	// Wait for response
	respChan := make(chan []byte, 1)
	conn.SetOnMessage(func(h *rbus.RTMessageHeader, payload []byte) {
		if h.Sequence == seq && h.IsResponse() {
			respChan <- payload
		}
	})
	
	select {
	case resp := <-respChan:
		sessionId, err := decodeSessionResponse(resp)
		if err != nil {
			return 0, fmt.Errorf("decode session response: %w", err)
		}
		log.Printf("Created session ID: %d", sessionId)
		return sessionId, nil
	case <-time.After(5 * time.Second):
		return 0, fmt.Errorf("timeout waiting for session response")
	}
}

func encodeSessionRequest() []byte {
	// MessagePack encode: "req_new_s" + "" + "" + int32(0)
	// \xAA (fixstr 10) + "req_new_s" + \0 + \xA1 (fixstr 1) + \0 + \xA1 + \0 + \xD2\0\0\0\0
	payload := []byte{
		0xAA, // fixstr length 10
		'r', 'e', 'q', '_', 'n', 'e', 'w', '_', 's', 0x00,
		0xA1, 0x00, // fixstr(1) empty string
		0xA1, 0x00, // fixstr(1) empty string
		0xD2, 0x00, 0x00, 0x00, 0x00, // int32(0)
	}
	return payload
}

func decodeSessionResponse(payload []byte) (int32, error) {
	// Response format: sessionId (int32)
	// Skip method response wrapper, find the int32 sessionId
	if len(payload) < 4 {
		return 0, fmt.Errorf("response too short: %d bytes", len(payload))
	}
	
	// Look for int32 marker (0xD2) followed by 4 bytes
	for i := 0; i < len(payload)-4; i++ {
		if payload[i] == 0xD2 {
			sessionId := int32(payload[i+1])<<24 | int32(payload[i+2])<<16 | 
			             int32(payload[i+3])<<8 | int32(payload[i+4])
			return sessionId, nil
		}
		// Also check for small positive fixint (0x00-0x7F)
		if payload[i] >= 0x00 && payload[i] <= 0x7F && i == len(payload)-1 {
			return int32(payload[i]), nil
		}
	}
	
	return 0, fmt.Errorf("no sessionId found in response")
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <parameter> <type> <value>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Types: string, int32, uint32, bool, float\n")
		fmt.Fprintf(os.Stderr, "\nExample:\n")
		fmt.Fprintf(os.Stderr, "  %s Device.Users.User.2.X_CISCO_COM_Password string mypassword\n", os.Args[0])
		os.Exit(1)
	}

	paramName := os.Args[1]
	valueType := os.Args[2]
	valueStr := os.Args[3]

	// Parse value based on type
	var rbusType int32
	var value interface{}

	switch valueType {
	case "string":
		rbusType = rbus.RBUS_STRING
		value = valueStr
	case "int32":
		v, err := strconv.ParseInt(valueStr, 10, 32)
		if err != nil {
			log.Fatalf("Invalid int32 value: %v", err)
		}
		rbusType = rbus.RBUS_INT32
		value = int32(v)
	case "uint32":
		v, err := strconv.ParseUint(valueStr, 10, 32)
		if err != nil {
			log.Fatalf("Invalid uint32 value: %v", err)
		}
		rbusType = rbus.RBUS_UINT32
		value = uint32(v)
	case "bool":
		v, err := strconv.ParseBool(valueStr)
		if err != nil {
			log.Fatalf("Invalid bool value: %v", err)
		}
		rbusType = rbus.RBUS_BOOLEAN
		value = v
	case "float":
		v, err := strconv.ParseFloat(valueStr, 32)
		if err != nil {
			log.Fatalf("Invalid float value: %v", err)
		}
		rbusType = rbus.RBUS_SINGLE
		value = float32(v)
	default:
		log.Fatalf("Unknown type: %s (use: string, int32, uint32, bool, float)", valueType)
	}

	log.Printf("goSetter: setting %s = %v (%s)", paramName, value, valueType)

	// Create connection with rbuscli naming
	appName := fmt.Sprintf("rbuscli-%d", os.Getpid())
	conn, err := rbus.NewConn(appName, "")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Create session for password/sensitive parameters
        sessionId := int32(0)

	// Component name matches rbuscli
	componentName := appName

	// Encode SET request with sessionId
	payload, err := rbus.EncodeSetRequest(sessionId, componentName, paramName, rbusType, value)
	if err != nil {
		log.Fatalf("Encode SET request: %v", err)
	}

	// Send SET request
	seq, err := conn.SendSetRequest(paramName, payload)
	if err != nil {
		log.Fatalf("Send SET request: %v", err)
	}

	// Wait for response
	respChan := make(chan []byte, 1)
	conn.SetOnMessage(func(h *rbus.RTMessageHeader, payload []byte) {
		if h.Sequence == seq && h.IsResponse() {
			respChan <- payload
		}
	})

	select {
	case resp := <-respChan:
		status, err := rbus.DecodeSetResponse(resp)
		if err != nil {
			log.Fatalf("Decode response: %v", err)
		}

		// Status 0 or 100 = success
		if status == 0 || status == 100 {
			fmt.Printf("SUCCESS: Set %s = %v\n", paramName, value)
		} else {
			fmt.Printf("FAILED: Set %s returned status %d (0x%x)\n", paramName, status, status)
			os.Exit(1)
		}

	case <-time.After(5 * time.Second):
		log.Fatal("Timeout waiting for SET response")
	}
}
