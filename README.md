# kotobase-storage-ipfs

IPFS immutable blocks plus a single-writer IPNS ref adapter. IPLD remains the
shared encoding and is not itself a storage backend. Multi-writer IPNS is
intentionally outside the linearizable profile.

Pass one `:ipns-key` for a single database, or `:ipns-key-fn` to map each ref
name to its own Kubo key. Publications are serialized inside the adapter. No
other process may publish those keys; multi-writer deployments require a
linearizable external ref service.
