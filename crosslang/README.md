# Cross-language suite

protobus-java against the TypeScript, Python, Go and C++ ports' real
libraries, over a real broker, in both directions:

| Test | What runs |
|---|---|
| `java client against a {java,ts,py,go,cpp} server` | the Java client scenario against each language's server |
| `{ts,py,go,cpp} client against a java server` | each language's client scenario against the Java server |
| `mixedReplicasShareOneRetryLadder` | `interop.Flaky` served in all five languages at once, sharing one queue and one retry ladder |

Every client runs the same fifteen checks (unary calls, priority, streams in
order, empty streams, mid-stream errors, cancellation reaching the producer,
custom types with maps and defaults, an echo round trip, handled, unhandled and
protocol errors, call metadata, instance routing, and events both ways). The
mixed-replica test requires every message to be attempted exactly four times,
in more than one language, and to reach `interop.Flaky.DLQ` exactly once with
the shared metadata headers. Each test runs in a fresh vhost.

## Layout

- `proto/interop.proto`: the contract, identical to protobus-go's and protobus-cpp's.
- `src/main/java/.../JavaPeer.java`: the Java participant (`server` / `client`).
- `peers/ts/peer.js`, `peers/py/peer.py`, `peers/go/`: the TypeScript, Python
  and Go participants, the same files protobus-go's and protobus-cpp's suites run.
- The C++ participant is protobus-cpp's own `cpppeer`, from its build tree.
- `src/test/java/.../CrossLangTest.java`: the harness.

## Running it

```bash
docker compose up -d --wait
export PROTOBUS_TEST_AMQP_URL=amqp://guest:guest@127.0.0.1:25672/
export PROTOBUS_TEST_MGMT_URL=http://guest:guest@127.0.0.1:25673
(cd ../protobus && npm ci && npm run build-ts)        # the TypeScript peer runs the built library
(cmake -S ../protobus-cpp -B ../protobus-cpp/build && cmake --build ../protobus-cpp/build --target cpppeer)
./gradlew :crosslang:test
```

The peers are found in sibling checkouts: `PROTOBUS_TS` (default
`../protobus`), `PROTOBUS_PY` (default `../protobus-py`, with a `venv/`),
`PROTOBUS_GO` (default `../protobus-go`, with Go on the PATH) and
`PROTOBUS_CPP` (default `../protobus-cpp`, with `build/crosslang/cpppeer`). A
missing peer skips its tests, unless its variable is set explicitly, as CI
does: then its absence is a failure, so a misconfigured run cannot pass by
skipping.

Any peer can be run by hand against any server:

```bash
PROTOBUS_TEST_AMQP=$PROTOBUS_TEST_AMQP_URL PEER_TARGET=java node peers/ts/peer.js client
```
