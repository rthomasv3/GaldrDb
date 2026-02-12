# GaldrDb TODO

## Auto-Increment ID Reuse After Delete+Crash

**Context:** NextId (auto-increment counter) is no longer persisted to disk on every insert — it's
derived from `MAX(btree_key)+1` during recovery in `RebuildVersionIndex`. This means if a document
is deleted and then a crash occurs before the next metadata write, the ID can be reused on recovery.
This is the same behavior as SQLite without `AUTOINCREMENT`.

**Task:** Consider adding a configuration option (similar to SQLite's `AUTOINCREMENT`) for users who
need monotonically increasing IDs that are never reused. Possible approaches:
- Non-transactional sequence write-ahead (like PostgreSQL sequences — gaps on rollback, but never reuse)
- Batch allocation (advance persisted counter by N, allocate from batch in memory — reduces write frequency)
- Note: a simple "separate page" approach won't work because concurrent inserts to the same collection
  would still cause page-level conflicts in our MVCC model

**Current behavior:** Default is MAX+1 recovery (zero overhead, allows reuse). This is acceptable for
most use cases. The monotonic option would be opt-in for users who need stable identity guarantees.

## BTree Crash Recovery: Orphaned/Stale Entries After FlushPendingOps

**Context:** The pending leaf ops system writes BTree leaf pages with null context (immediately durable,
bypassing the transaction). This is necessary so concurrent transactions can see flushed data through
their null-context reads. However, it creates a crash window between `FlushPendingOps` (BTree changes
durable) and `CommitWriteWithVersions` (doc data + version entries committed). A crash in this window
leaves the BTree in an inconsistent state:

- **Orphaned inserts:** BTree has keys pointing to uncommitted document locations (garbage data).
- **Stale updates:** BTree keys' locations were changed to point to uncommitted new data. The old
  (correct) location is lost from the BTree.
- **Lost deletes:** BTree keys were removed but the delete was never committed. The keys should still
  exist but are gone.

**Possible approaches:**
1. **WAL undo log:** Before FlushPendingOps, write an undo record to the WAL tied to the transaction ID.
   On recovery, if the undo record exists without a matching commit, replay the undo to reverse the BTree
   changes. Handles all three cases correctly.
2. **Validate during RebuildVersionIndex:** For each BTree entry, try to read the document at its
   location. If the read fails, remove the entry. Handles orphaned inserts but NOT stale updates or
   lost deletes.
3. **Reorder: CommitWrite before FlushPendingOps:** Commit doc data first, then flush BTree ops. Crash
   after CommitWrite but before flush means committed data exists but BTree doesn't reference it. Recovery
   would need a way to rediscover committed documents not in the BTree (e.g., from WAL commit records).

**Current status:** The crash window is very narrow (microseconds between two adjacent calls). Accepted
as a known limitation for now.

## BTree Root Page Crash Safety

**Context:** Structural writes (splits) change the BTree root page immediately on disk via null-context
writes. The collection metadata (which stores the root page ID) is written separately, also via null
context, but in a subsequent call. A crash between the two means the metadata has a stale root pointer.
On recovery, `RebuildVersionIndex` navigates the BTree from the stale root and misses any entries in
subtrees created by the split. This is permanent data loss.

**Possible approaches:**
1. **Atomic metadata update:** Bundle the structural write and metadata update into a single WAL
   transaction so they commit together. Requires changing how null-context writes interact with the WAL.
2. **Self-describing root:** Store the root page ID in the BTree pages themselves (e.g., a parent pointer
   or root marker). During recovery, scan BTree pages to find the true root instead of relying on metadata.
3. **Root page journal:** Write the new root page ID to a dedicated journal location before the split.
   On recovery, check the journal and update metadata if needed.

**Current status:** The crash window is narrow (split and metadata write happen in the same CommitInsert
call). Accepted as a known limitation for now.
