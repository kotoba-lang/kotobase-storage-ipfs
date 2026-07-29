# kotobase-storage-ipfs

IPFS immutable blocks plus a single-writer IPNS ref adapter. IPLD remains the
shared encoding and is not itself a storage backend. Multi-writer IPNS is
intentionally outside the linearizable profile.

Pass one `:ipns-key` for a single database, or `:ipns-key-fn` to map each ref
name to its own Kubo key. Publications are serialized inside the adapter. No
other process may publish those keys; multi-writer deployments require a
linearizable external ref service.

## The single-writer profile is the design, not a caveat

IPNS has no compare-and-swap. `name/publish` replaces the record
unconditionally. What `-compare-and-set-ref!` does here is read the name,
compare, publish — a CAS only because `serialize!` funnels every publication
through one in-process queue.

Two writers in two processes both read the same head, both find it matching,
and both publish. The later wins; the earlier writer's commit becomes
unreachable; **both are told they succeeded.**

`test/run.cljs` demonstrates that rather than describing it:

```
ok  - two writer processes are BOTH told they published
ok  - and only the last one survives -- cid-from-a is lost
```

The test asserts the *loss*, deliberately. If it ever starts failing, either
IPNS grew a conditional publish or the adapter grew a real lock, and this
file should be revisited rather than continue documenting a limit that moved.

## Sequence numbers are `nil`, and that is the honest answer

Kubo's `name/resolve` returns a path — no sequence, no validity, no signer.
So `:version` is `nil` for a Kubo-backed name.

An earlier version returned a counter held in a JS atom inside the client. It
started at 0, was never read from any IPNS record, and reset on every process
restart: it reported version 1 for a name the network held at sequence 400,
and two processes reported the same version for different records. A `nil`
meaning "this transport does not expose one" is worth more than a number
meaning nothing.

## What the test mock may not do

The suite previously ran against a mock whose `publish-name!` performed a
real compare-and-swap on `expected-sequence` and whose `resolve-name`
returned the stored sequence. **Kubo does neither.** A green suite against a
transport stronger than the real one is the single arrangement in which
passing tells you nothing.

The mock is now weak in exactly the ways Kubo is weak: unconditional publish,
no sequence.

## Test

```sh
nbb --classpath "$(clojure -Spath -M:cljs-test)" test/run.cljs
```
