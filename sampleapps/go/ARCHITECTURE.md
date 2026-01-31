# RBus Go Sample Apps – Architecture

This document describes the architecture of the Go sample provider and consumer and how they communicate over the rbus wire protocol.

---

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           RBus messaging system                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────┐                    ┌─────────────────┐                │
│   │  goProvider     │                    │  goConsumer     │                │
│   │  (sampleapps/  │                    │  (sampleapps/   │                │
│   │   go)          │                    │   go)           │                │
│   └────────┬───────┘                    └────────┬────────┘                │
│            │                                    │                          │
│            │  Unix domain socket                │                          │
│            │  /tmp/rtrouted                     │                          │
│            │                                    │                          │
│            └────────────────┬───────────────────┘                          │
│                              │                                              │
│                     ┌────────▼────────┐                                      │
│                     │   rtrouted     │   (broker daemon; must run first)   │
│                     │   (C daemon)   │                                      │
│                     └────────────────┘                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

- **rtrouted**: Central message broker. All apps connect to it over a single Unix socket. It routes by **topic** (element name or control topic).
- **goProvider**: Registers element topics (e.g. `Device.DeviceInfo.SampleProvider.Manufacturer`), receives get requests, returns values.
- **goConsumer**: Subscribes to its inbox, sends get requests to element topics, receives responses on the inbox.

There is **no direct connection** between provider and consumer; all traffic goes through rtrouted.

---

## 2. Go Package Layout

```
sampleapps/go/
├── go.mod
├── README.md
├── ARCHITECTURE.md          ← this file
├── cmd/
│   ├── goProvider/
│   │   └── main.go          # Provider: subscribe to elements, handle get, respond
│   └── goConsumer/
│       └── main.go          # Consumer: send get for 6 params, print values
└── internal/
    └── rbus/
        ├── rtmessage.go     # RTMessage header encode/decode (wire format)
        ├── conn.go          # Connection to rtrouted, HELLO/subscribe/send/readLoop
        ├── msgpack.go       # Get request/response encode/decode (RBus app messages)
        └── msgpack_mini.go  # Minimal MessagePack (str, int32, bool, float64)
```

| Package / file      | Responsibility |
|---------------------|----------------|
| **internal/rbus**   | Wire protocol and connection: RTMessage header, socket, HELLO/SUBSCRIBE, get request/response MessagePack. No external deps. |
| **cmd/goProvider**  | Sample provider: connect, HELLO, subscribe inbox + 6 element topics, on-message handler for get → encode response → send to reply_topic. |
| **cmd/goConsumer**  | Sample consumer: connect, HELLO, subscribe inbox, send 6 get requests, match responses by sequence, decode and print. |

---

## 3. Wire Protocol Layers

All messages use the same **RTMessage** envelope; the payload distinguishes control vs application.

```
┌──────────────────────────────────────────────────────────────────┐
│  Application (cmd/goProvider, cmd/goConsumer)                    │
│  - Get request/response (MessagePack)                             │
├──────────────────────────────────────────────────────────────────┤
│  RBus application messages (internal/rbus/msgpack*.go)            │
│  - Get request:  component, param_size, parameter + metadata      │
│  - Get response: status, has_data, name, type, value + metadata  │
├──────────────────────────────────────────────────────────────────┤
│  RTMessage envelope (internal/rbus/rtmessage.go)                  │
│  - Header: marker, version, header_length, sequence, flags,       │
│            payload_length, topic, reply_topic, [T1–T5], marker   │
│  - Payload: control (JSON) or application (MessagePack)          │
├──────────────────────────────────────────────────────────────────┤
│  Transport (internal/rbus/conn.go)                                │
│  - Unix stream socket to /tmp/rtrouted                            │
└──────────────────────────────────────────────────────────────────┘
```

- **Control** (HELLO, SUBSCRIBE): RTMessage with **topic** = `_RTROUTED.INBOX.HELLO` or `_RTROUTED.INBOX.SUBSCRIBE`, **flags** = 0, **payload** = JSON (+ null for cJSON).
- **Application** (get): RTMessage with **topic** = element name, **reply_topic** = requester inbox, **flags** = 0x11 (Request + RawBinary), **payload** = MessagePack get request/response.

---

## 4. Connection and Subscription Flow

```
  goProvider                                    rtrouted                    goConsumer

       │                                             │                             │
       │  connect Unix /tmp/rtrouted                 │                             │
       │────────────────────────────────────────────►│                             │
       │                                             │                             │
       │  HELLO   topic=_RTROUTED.INBOX.HELLO        │                             │
       │          payload={"inbox":"rbus.goProvider.INBOX.<pid>"}\0                  │
       │────────────────────────────────────────────►│                             │
       │                                             │  (register client)          │
       │  SUBSCRIBE topic=_RTROUTED.INBOX.SUBSCRIBE  │                             │
       │          payload={"add":1,"topic":"rbus.goProvider.INBOX.<pid>","route_id":1}\0
       │────────────────────────────────────────────►│                             │
       │                                             │  (route 1 = inbox)          │
       │  SUBSCRIBE (x6) topic=Device....            │                             │
       │          payload={"add":1,"topic":"Device....","route_id":1}\0             │
       │────────────────────────────────────────────►│                             │
       │                                             │  (alias route 1)             │
       │  readLoop: wait for messages                │                             │
       │                                             │                             │
       │                                             │     connect, HELLO,         │
       │                                             │     SUBSCRIBE inbox         │
       │                                             │◄────────────────────────────│
       │                                             │                             │
       │                                             │     GET Device....          │
       │                                             │     reply=rbus.goConsumer.INBOX.<pid>
       │                                             │◄────────────────────────────│
       │  recv GET Device....                        │                             │
       │◄────────────────────────────────────────────│                             │
       │  send RESPONSE to rbus.goConsumer.INBOX.<pid>                               │
       │────────────────────────────────────────────►│────────────────────────────►│
       │                                             │                             │  recv response
```

- Each process opens **one** connection to rtrouted.
- **HELLO** registers the client’s inbox (needed for some rtrouted builds).
- **SUBSCRIBE** with `_RTROUTED.INBOX.SUBSCRIBE` and JSON payload registers the topic (inbox or element name) on route_id 1; further SUBSCRIBE with the same route_id add aliases (e.g. multiple element names).
- Provider receives get requests because it subscribed those element topics; consumer receives responses because it subscribed its inbox.

---

## 5. Get Request / Response Flow

```
  goConsumer                rtrouted                goProvider

       │                         │                         │
       │  RTMessage               │                         │
       │  topic=Device.DeviceInfo.SampleProvider.Manufacturer
       │  reply_topic=rbus.goConsumer.INBOX.<pid>           │
       │  flags=0x11 (Request|RawBinary)                     │
       │  payload=MessagePack(component, param_size=1, parameter) + metadata
       │─────────────────────────►│                         │
       │                         │  forward by topic        │
       │                         │─────────────────────────►│
       │                         │                         │  decode get request
       │                         │                         │  lookup param, encode response
       │                         │  RTMessage               │
       │                         │  topic=rbus.goConsumer.INBOX.<pid>
       │                         │  reply_topic=Device....
       │                         │  flags=0x12 (Response|RawBinary)
       │                         │  payload=MessagePack(status, has_data, name, type, value) + metadata
       │                         │◄─────────────────────────│
       │  RTMessage (response)    │  forward to inbox       │
       │◄─────────────────────────│                         │
       │  decode response,       │                         │
       │  match by sequence,     │                         │
       │  print value            │                         │
```

- **Request**: topic = element name, reply_topic = consumer inbox, payload = MessagePack get request + metadata (method name, offset).
- **Response**: topic = consumer inbox (from request’s reply_topic), payload = MessagePack get response (status, has_data, name, type, value) + metadata.
- Consumer matches responses to requests by **sequence** number.

---

## 6. RTMessage Header (Compatibility)

The system rtrouted is often built with **MSG_ROUNDTRIP_TIME**. The header then includes an optional **roundtrip block** (T1–T5, 20 bytes) between reply_topic and the end marker:

```
  Offset   Size   Field
  ─────────────────────────────────────
  0        2      Header marker (0xAAAA)
  2        2      Version (2)
  4        2      Header length
  6        4      Sequence number
  10       4      Flags (e.g. 0x01 Request, 0x10 RawBinary)
  14       4      Control data
  18       4      Payload length
  22       4      Topic length
  26       N      Topic (UTF-8)
  26+N     4      Reply topic length
  30+N     M      Reply topic (UTF-8)
  30+N+M   20     T1–T5 (optional; zeros if not used)
  50+N+M   2      Header marker (0xAAAA)
  ────────
  Payload  P      Application or control payload
```

- **Encode**: We always send the 20-byte roundtrip block (zeros) so rtrouted built with MSG_ROUNDTRIP_TIME can decode.
- **Decode**: We use `header_length` to skip the roundtrip block (if present) before reading the end marker, so we stay in sync with both build types.

---

## 7. Key Types and Files

| Concept           | Where                | Description |
|-------------------|----------------------|-------------|
| **Conn**          | internal/rbus/conn.go | Connection to rtrouted: HELLO, subscribe, SendGetRequest, SendResponse, readLoop, SetOnMessage. |
| **RTMessageHeader** | internal/rbus/rtmessage.go | Version, sequence, flags, topic, reply_topic, payload_length. |
| **EncodeHeader / DecodeHeader** | internal/rbus/rtmessage.go | Big-endian encode/decode including optional T1–T5. |
| **EncodeGetRequest / DecodeGetRequest** | internal/rbus/msgpack.go | MessagePack get request (component, param_size, parameter + metadata). |
| **EncodeGetResponse / DecodeGetResponse** | internal/rbus/msgpack.go | MessagePack get response (status, has_data, name, type, value + metadata). |
| **RBUS_STRING, RBUS_INT32, …** | internal/rbus/msgpack.go | RBus value type IDs for encoding. |

---

## 8. Debugging

- **RBUS_DEBUG=1**: Logs every send and recv (topic, reply_topic, flags, seq, len) and readLoop exit reason.
- Ensure **rtrouted** is running first; use the same socket path (`/tmp/rtrouted` by default).
- If consumer times out: check provider logs for `rbus: recv topic="Device.`; if none, rtrouted is not forwarding (e.g. header mismatch or subscribe not registered).
