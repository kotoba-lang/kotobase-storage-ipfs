# kotobase-storage-ipfs

Provider-neutral IPFS immutable-block transport for Kotobase.

This library has no dependency on a particular node daemon, HTTP RPC API, or
mutable-name service. It accepts two injected, Promise-returning operations:

```clojure
(ipfs/open
 {:client
  {:put-block! (fn [expected-cid bytes] ... stored-cid-promise)
   :get-block  (fn [cid] ... bytes-or-nil-promise)}})
```

The client may use an embedded implementation, a browser implementation, a
remote pinning service, or another IPFS-compatible transport. Returned bytes
remain untrusted; compose this adapter with
`kotobase.storage.verify/async-verifying-block-store` at the application
boundary.

## Blocks and refs are different planes

The adapter intentionally implements `IBlockStore` only. A mutable database
head requires agreement and rollback protection; those guarantees cannot be
inferred from content-addressed block transport or mutable naming.

Compose blocks with a separately selected ref provider:

```clojure
(storage/compose
 {:blocks (ipfs/open {:client block-client})
  :refs linearizable-ref-store})
```

The signed commit DAG remains canonical truth. Discovery mechanisms may point
at a candidate frontier, but they are not used by this adapter and are not a
correctness dependency.

## A concrete client: kotobase.net's own archive, over UnixFS + CARv2

`kotobase.storage.ipfs-kotobase` is a `{:put-block! :get-block}` client
backed by kotobase.net's first-party `PUT/GET /ipfs/:cid` archive
(`kotobase.archive-put` in `net-kotobase/control-plane/kotobase-api-gateway-cljs`).
That endpoint enforces a **4 MiB per-object ceiling** (it buffers the body
in Worker memory to hash it — a Worker property, not a B2 one). Rather than
widening that endpoint, `bin/large_put.cljs` shows the other fix: never ask
it to accept an object anywhere near that size.

```
                              unixfs.file/build
  <file, any size>  ───────────────────────────▶  {:cid root :blocks [...]}
                                                    (256 KiB raw leaves,
                                                     small dag-pb parents —
                                                     every block ≪ 4 MiB)
                                    │
                                    ▼
              kotobase.storage.ipfs-kotobase/open  ──▶  PUT /ipfs/:cid  (per block)
                                    │                    ipld.car.v2/pack, PUT too
                                    │                    WHEN the pack itself is
                                    │                    still ≤4 MiB (a read-side
                                    │                    optimisation only — never
                                    │                    silently skipped, always
                                    │                    logged when it doesn't fit)
                                    ▼
                          unixfs.file/read-file      ◀── GET /ipfs/:cid (per block)
                       (re-hashes every block; a          fetched fresh over the
                        byte-identical reassembly         network, not read back
                        is the only thing "verified"      from local memory
                        means)
```

The archive endpoint itself is unmodified and still fail-closed: it still
refuses anything over 4 MiB, still re-hashes every body it accepts. What
changed is what gets asked of it.

### The identity/location split (root ADR-2608148200)

`PUT /ipfs/:cid` accepts **only raw CIDv1(sha2-256)**. A UnixFS leaf's CID
already is one, but a dag-pb parent node's is not. `ipfs-kotobase` computes
the RAW CID of each block's own bytes as the archive **Location**, PUTs
there, and — only when identity and location differ — remembers the
mapping so `get-block` can still be asked for the identity CID. `put-block!`
always resolves to the identity CID it was given: the split is invisible
above this namespace, and the `kotobase.storage.ipfs` adapter's own
`stored-cid == cid` check is what proves that (`test/ipfs_kotobase_test.cljs`).

```clojure
(require '[kotobase.storage.ipfs :as ipfs]
         '[kotobase.storage.ipfs-kotobase :as ipfs-kotobase])

(def client (ipfs-kotobase/open {:base-url "https://kotobase.net" :token token}))
(def adapter (ipfs/open {:client client}))
;; adapter is a normal kotobase.storage.core/IBlockStore from here.
```

**Never pass the token on argv** (`ps` exposes it to every process on the
machine) — `bin/large_put.cljs` reads it from a file path named by
`KOTOBASE_ARCHIVE_TOKEN_FILE`.

### Try it: `bin/large_put.cljs`

```sh
# multiformats.core needs @noble/hashes -- clojure -Spath resolves SOURCE,
# not node_modules, so `npm install` once wherever io-multiformats lands:
IO_MF="$(clojure -Spath -M:cljs-test | tr : $'\n' | grep io-multiformats)"
(cd "${IO_MF%/src}" && npm install)

KOTOBASE_ARCHIVE_TOKEN_FILE=/path/to/token \
NODE_PATH="${IO_MF%/src}/node_modules" \
  nbb --classpath "$(clojure -Spath -M:cljs-test)" bin/large_put.cljs /path/to/large/file
```

It chunks the file, uploads every block (and the CARv2 pack, when it still
fits under the ceiling), fetches everything back over the network, and
refuses to call it a success unless the reassembled bytes are identical to
the original file.

### `bin/mock_archive_server.cljs` — the same round trip without live credentials

A local, loopback HTTP double of the archive's raw-CID contract (4 MiB
ceiling, digest check, no auth). It needs the same `NODE_PATH` as above
(it also uses `multiformats.core`):

```sh
IO_MF="$(clojure -Spath -M:cljs-test | tr : $'\n' | grep io-multiformats)"
NODE_PATH="${IO_MF%/src}/node_modules" \
  nbb --classpath "$(clojure -Spath -M:cljs-test)" bin/mock_archive_server.cljs &
```

Then point `bin/large_put.cljs` at it with `KOTOBASE_BASE_URL=http://localhost:8998`
(any token file still satisfies `-main`'s check; the double does not verify
it) to exercise the entire pipeline — chunking, upload, CARv2 pack, network
fetch, UnixFS reassembly, byte comparison — without a working
`KOTOBASE_ARCHIVE_TOKEN`. See
`.claude/skills/secrets-location-map/references/kotobase.md` in the
superproject for why that credential is not reliably available today.

## A second concrete client: any real Kubo node, over `kotobase.storage.ipfs-kubo`

`ipfs-kotobase` (above) talks back to kotobase.net's own archive — a cache
round trip, not a distinct storage substrate. `kotobase.storage.ipfs-kubo`
is the client that actually reaches an independently operated Kubo
(go-ipfs) node's HTTP RPC + gateway (self-hosted, or a Kubo-RPC-compatible
pinning provider), over `kotoba-lang/io-ipfs`'s pure Kubo protocol model:

```clojure
(require '[kotobase.storage.ipfs :as ipfs]
         '[kotobase.storage.ipfs-kubo :as ipfs-kubo])

(def client (ipfs-kubo/open {:api-url "https://ipfs-rpc.example"
                              :gateway-url "https://ipfs-gateway.example"
                              :token token}))
(def adapter (ipfs/open {:client client}))
;; adapter is a normal kotobase.storage.core/IBlockStore from here --
;; compose it with a ref store per the "Blocks and refs are different
;; planes" section above.
```

Kubo's own computed CID for a block usually matches the caller-assigned
identity CID (single raw leaves, `cid-version=1` defaults `raw-leaves` to
true), but is not guaranteed to byte-for-byte across Kubo versions/configs
-- this client keeps the same small identity->location map as
`ipfs-kotobase` for the rare block where it doesn't, and `put-block!`
always resolves to the identity CID regardless.

This is the client `kotoba-lang/kotobase-storage-d1`'s `KOTOBASE_AUTHORITY=ipfs`
option wires in as an additional, non-default block provider (superproject
ADR-2608281000 Decision 2: "ベースは分散型、中央集権は効率化のための cache").

## Test

```sh
nbb --classpath "$(clojure -Spath -M:cljs-test)" test/run.cljs
nbb --classpath "$(clojure -Spath -M:cljs-test)" test/ipfs_kotobase_test.cljs
nbb --classpath "$(clojure -Spath -M:cljs-test)" test/ipfs_kubo_test.cljs
```
