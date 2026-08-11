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

## Test

```sh
nbb --classpath "$(clojure -Spath -M:cljs-test)" test/run.cljs
```
