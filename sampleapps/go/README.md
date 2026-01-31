# RBus Go Sample Apps

Minimal **Go** sample provider and consumer that speak the rbus wire protocol (RTMessage + MessagePack get request/response). They interoperate with **rtrouted** and with the C sample provider/consumer.

## Prerequisites

- Go 1.21+
- **rtrouted** running (required for all rbus apps)

## Build

From `sampleapps/go`:

```bash
cd sampleapps/go
go build -o goProvider ./cmd/goProvider
go build -o goConsumer ./cmd/goConsumer
go build -o getManufacturer ./cmd/getManufacturer
go build -o setPeriodicInform ./cmd/setPeriodicInform
go build -o getUserPassword ./cmd/getUserPassword
ls -la goProvider goConsumer getManufacturer setPeriodicInform getUserPassword
```

## Run

**Order matters:** start rtrouted first, then the provider, then the consumer.

**Terminal 1 – start rtrouted** (must be running before provider/consumer):

```bash
rtrouted -f -l DEBUG
```

If you see `Done(12)` or rtrouted exits, another rtrouted may already be using `/tmp/rtrouted`. Either use that instance or stop it first (e.g. `killall rtrouted`) then start yours.

**If the consumer times out:** run with debug to see if messages are received:
```bash
RBUS_DEBUG=1 ./goProvider   # terminal 2 – watch for "rbus: recv topic=..."
RBUS_DEBUG=1 ./goConsumer  # terminal 3 – watch for "rbus: recv ..."
```
If the provider never logs `recv topic="Device.` then rtrouted is not forwarding get requests (subscriptions may not be registered). If the consumer never logs `recv` then responses are not reaching it.

**Terminal 2 – start a provider** (either C or Go):

```bash
# C sample provider
rbusSampleProvider

# Or Go sample provider
go run ./cmd/goProvider
# or: ./goProvider
```

**Terminal 3 – start a consumer** (either C or Go):

```bash
# C sample consumer
rbusSampleConsumer

# Or Go sample consumer
go run ./cmd/goConsumer
# or: ./goConsumer
```

The consumer performs `get` on the same 6 parameters as `rbusSampleConsumer` and prints the values.

**Get specific parameters (like rbuscli getvalues):** pass parameter name(s) as arguments:

```bash
./goConsumer Device.DeviceInfo.Manufacturer
./goConsumer Device.DeviceInfo.Manufacturer Device.DeviceInfo.ModelName
```

Output format matches `rbuscli getvalues` (Parameter N, Name, Type, Value).

**Get only Device.DeviceInfo.Manufacturer:** use the small dedicated consumer:

```bash
go build -o getManufacturer ./cmd/getManufacturer
./getManufacturer
```

It prints just the manufacturer value (e.g. `Traverse Technologies`). No provider needed beyond the one already on the device (e.g. PAM).

**Set a uint parameter** (same pattern as C `rbus_open` + `rbus_setUint`; reference: sampleapps/consumer/rbusTableConsumer.c):

```bash
go build -o setPeriodicInform ./cmd/setPeriodicInform
./setPeriodicInform 152
./setPeriodicInform 300   # value as argument (default 152)
```

Sets `Device.ManagementServer.PeriodicInformInterval`. Requires rtrouted and the component that owns that parameter.

**Get/set a string parameter (Users password, PAM/CR):**

```bash
go build -o getUserPassword ./cmd/getUserPassword
./getUserPassword           # get Device.Users.User.2.X_CISCO_COM_Password
./getUserPassword newpass   # set via wire protocol (connect to rtrouted, send METHOD_SETPARAMETERVALUES), then get to confirm
```

Uses only the wire protocol: connect to rtrouted (Unix socket), send get/set via RTMessage + MessagePack (METHOD_GETPARAMETERVALUES / METHOD_SETPARAMETERVALUES). No rbuscli. Override socket with `RBUS_SOCKET_PATH` if needed.

## Interop

- **Go consumer** can talk to **C provider** (`rbusSampleProvider`).
- **Go provider** can talk to **C consumer** (`rbusSampleConsumer`).

Same 6 parameters:

- `Device.DeviceInfo.SampleProvider.Manufacturer`
- `Device.DeviceInfo.SampleProvider.ModelName`
- `Device.DeviceInfo.SampleProvider.SoftwareVersion`
- `Device.SampleProvider.SampleData.IntData`
- `Device.SampleProvider.SampleData.BoolData`
- `Device.SampleProvider.SampleData.UIntData`

## Layout

- `internal/rbus/` – RTMessage header encode/decode, connection to rtrouted, MessagePack get request/response (no external deps).
- `cmd/goProvider/` – sample provider (subscribes to element topics, handles get, responds).
- `cmd/goConsumer/` – sample consumer (sends get for each param, prints values).

## Socket

Default rtrouted socket: **`/tmp/rtrouted`** (Unix domain stream). Override via env or code if your setup uses a different path.
