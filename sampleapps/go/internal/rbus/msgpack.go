package rbus

import (
	"bytes"
	"fmt"
	"io"
)

// RBus value type IDs (from rbus_value.h)
const (
	RBUS_STRING  = 0x50E
	RBUS_INT32   = 0x507
	RBUS_UINT32  = 0x508
	RBUS_BOOLEAN = 0x500
	RBUS_SINGLE  = 0x50B
)

const methodGetParameterValues = "METHOD_GETPARAMETERVALUES"
const methodSetParameterValues = "METHOD_SETPARAMETERVALUES"
const methodResponse = "METHOD_RESPONSE"
const methodRPC = "METHOD_RPC"

const RBUS_ERROR_SUCCESS = 0

// Session manager method (destination topic) for create session.
const SessionMgrRequestSession = "req_new_s"

// EncodeGetRequest builds MessagePack payload for get request.
func EncodeGetRequest(componentName, paramName string) ([]byte, error) {
	var buf bytes.Buffer
	e := &mpEnc{buf: &buf}

	e.writeStrWithNull(componentName)
	e.writeInt32(1)
	e.writeStrWithNull(paramName)

	metaStart := buf.Len()
	e.writeStrWithNull(methodGetParameterValues)
	e.writeStrWithNull("")
	e.writeStrWithNull("")
	e.writeInt32(int32(metaStart))

	return buf.Bytes(), nil
}

// DecodeGetRequest parses get request: component, param_size, parameter.
func DecodeGetRequest(payload []byte) (component, parameter string, err error) {
	d := &mpDec{r: bytes.NewReader(payload)}

	comp, err := d.readStr()
	if err != nil {
		return "", "", err
	}
	component = trimNull(comp)
	_, err = d.readInt32()
	if err != nil {
		return "", "", err
	}
	param, err := d.readStr()
	if err != nil {
		return "", "", err
	}
	parameter = trimNull(param)
	return component, parameter, nil
}

func trimNull(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return s[:i]
		}
	}
	return s
}

// EncodeGetResponse builds MessagePack payload for get response.
func EncodeGetResponse(status int32, paramName string, valueType int32, value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	e := &mpEnc{buf: &buf}

	e.writeInt32(status)
	if status == 0 || status == 100 {
		e.writeInt32(1)
		e.writeStrWithNull(paramName)
		e.writeInt32(valueType)
		if err := encodeRbusValue(e, valueType, value); err != nil {
			return nil, err
		}
	} else {
		e.writeInt32(0)
	}

	metaStart := buf.Len()
	e.writeStrWithNull(methodResponse)
	e.writeStrWithNull("")
	e.writeStrWithNull("")
	e.writeInt32(int32(metaStart))

	return buf.Bytes(), nil
}

func encodeRbusValue(e *mpEnc, valueType int32, value interface{}) error {
	switch valueType {
	case RBUS_STRING:
		s, _ := value.(string)
		b := make([]byte, len(s)+1)
		copy(b, s)
		b[len(s)] = 0
		e.writeBin(b)
		return nil
	case RBUS_INT32:
		v, _ := value.(int32)
		e.writeInt32(v)
		return nil
	case RBUS_UINT32:
		v, _ := value.(uint32)
		e.writeInt32(int32(v))
		return nil
	case RBUS_BOOLEAN:
		v, _ := value.(bool)
		e.writeBool(v)
		return nil
	case RBUS_SINGLE:
		v, _ := value.(float32)
		e.writeFloat64(float64(v))
		return nil
	default:
		return fmt.Errorf("unsupported value type %d", valueType)
	}
}

// DecodeGetResponse parses get response: status, has_data, name, type, value.
func DecodeGetResponse(payload []byte) (status int32, paramName string, value interface{}, err error) {
	d := &mpDec{r: bytes.NewReader(payload)}

	status, err = d.readInt32()
	if err != nil {
		return 0, "", nil, err
	}
	hasData, err := d.readInt32()
	if err != nil {
		return 0, "", nil, err
	}
	if hasData == 0 {
		return status, "", nil, nil
	}
	name, err := d.readStr()
	if err != nil {
		return 0, "", nil, err
	}
	paramName = trimNull(name)
	vt, err := d.readInt32()
	if err != nil {
		return 0, "", nil, err
	}
	value, err = decodeRbusValue(d, vt)
	if err != nil {
		return 0, "", nil, err
	}
	return status, paramName, value, nil
}

func decodeRbusValue(d *mpDec, valueType int32) (interface{}, error) {
	switch valueType {
	case 0, RBUS_STRING:
		// Some providers (e.g. PAM) send type 0 for string; standard is RBUS_STRING (0x50E).
		s, err := d.readStr()
		return trimNull(s), err
	case RBUS_INT32:
		return d.readInt32()
	case RBUS_UINT32:
		v, err := d.readInt32()
		return uint32(v), err
	case RBUS_BOOLEAN:
		return d.readBool()
	case RBUS_SINGLE:
		v, err := d.readFloat64()
		return float32(v), err
	default:
		return nil, fmt.Errorf("unsupported value type %d", valueType)
	}
}

// EncodeSetRequest builds MessagePack payload for set request (single param).
// Uses compact encoding (fixint for small values) to match rbuscli's format.
// Matches C: sessionId, componentName, needRollBack, paramCount, then for each param: name, type, value; then commit; then meta.
func EncodeSetRequest(sessionId int32, componentName, paramName string, valueType int32, value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	e := &mpEnc{buf: &buf}

	e.writeSmallInt(sessionId)              // sessionId (compact: fixint for 0-127)
	e.writeStrWithNull(componentName)       // component that invokes the set
	e.writeSmallInt(1)                      // param count (compact)
	e.writeStrWithNull(paramName)           // param name
	e.writeUint16(uint16(valueType))
	if err := encodeRbusValue(e, valueType, value); err != nil {
		return nil, err
	}
	e.writeStrWithNull("TRUE") // commit

	metaStart := buf.Len()
	e.writeStrWithNull(methodSetParameterValues)
	e.writeStrWithNull("")
	e.writeStrWithNull("")
	e.writeInt32(int32(metaStart))

	return buf.Bytes(), nil
}

// DecodeSetResponse parses set response: int32 status; optionally string error reason on failure.
func DecodeSetResponse(payload []byte) (status int32, err error) {
	d := &mpDec{r: bytes.NewReader(payload)}
	status, err = d.readInt32()
	return status, err
}

// EncodeMethodInvokeRequest builds payload for method invoke (e.g. session manager req_new_s).
// Matches C: int32(0), string(methodName), int32(paramCount); meta METHOD_RPC.
func EncodeMethodInvokeRequest(methodName string) ([]byte, error) {
	var buf bytes.Buffer
	e := &mpEnc{buf: &buf}
	e.writeInt32(0)
	e.writeStrWithNull(methodName)
	e.writeInt32(0) // no in params
	metaStart := buf.Len()
	e.writeStrWithNull(methodRPC)
	e.writeStrWithNull("")
	e.writeStrWithNull("")
	e.writeInt32(int32(metaStart))
	return buf.Bytes(), nil
}

// DecodeSessionResponse parses session manager response: int32 returnCode, then fixmap with return_value, sessionid.
func DecodeSessionResponse(payload []byte) (returnCode int32, sessionId int32, err error) {
	d := &mpDec{r: bytes.NewReader(payload)}
	returnCode, err = d.readInt32()
	if err != nil {
		return 0, 0, err
	}
	t, err := d.readByte()
	if err != nil || t != 0x82 { // fixmap 2
		return returnCode, 0, fmt.Errorf("msgpack: expected fixmap 2, got 0x%02x", t)
	}
	for i := 0; i < 2; i++ {
		key, err := d.readStr()
		if err != nil {
			return returnCode, 0, err
		}
		val, err := d.readInt32()
		if err != nil {
			return returnCode, 0, err
		}
		if trimNull(key) == "sessionid" {
			sessionId = val
		}
	}
	return returnCode, sessionId, nil
}

// getMetadataOffset reads the last int32 (metadata offset) from payload.
func getMetadataOffset(payload []byte) (int32, error) {
	if len(payload) < 5 {
		return 0, fmt.Errorf("payload too short")
	}
	// Last value is int32: 0xd2 + 4 bytes
	r := bytes.NewReader(payload)
	r.Seek(int64(len(payload)-5), io.SeekStart)
	d := &mpDec{r: r}
	return d.readInt32()
}
