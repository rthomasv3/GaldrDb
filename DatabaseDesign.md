# Document Database File Structure

## File Layout Overview
```
┌─────────────────────────────────────────┐
│ HEADER (fixed size)                     │
│ - magic number / version                │
│ - page_size (e.g., 8192 bytes)          │
│ - total_page_count                      │
│ - bitmap_start_page                     │
│ - bitmap_page_count                     │
│ - free_space_map_start_page             │
│ - free_space_map_page_count             │
│ - collections_metadata_page             │
│ - mmap_hint                             │
│ - last_commit_frame                     │
│ - wal_checksum                          │
│ - padding                               │
└─────────────────────────────────────────┘
┌─────────────────────────────────────────┐
│ BITMAP PAGES                            │
│ - 1 bit per page (0=free, 1=allocated)  │
└─────────────────────────────────────────┘
┌─────────────────────────────────────────┐
│ FREE SPACE MAP PAGES                    │
│ - 2 bits per page (free space level)    │
│   00 = full (0-10% free)                │
│   01 = low free space (10-40% free)     │
│   10 = medium free space (40-70% free)  │
│   11 = high free space (70-100% free)   │
└─────────────────────────────────────────┘
┌─────────────────────────────────────────┐
│ COLLECTIONS METADATA PAGE(S)            │
│ - collection_count: int                 │
│ - entries[]                             │
│   - name: string (e.g., "books")        │
│   - root_page: int (B+ tree root)       │
│   - doc_count: int                      │
└─────────────────────────────────────────┘
┌─────────────────────────────────────────┐
│ DATA PAGES (mix of both types below)    │
│                                         │
│ 1. Document Pages (slotted)             │
│ 2. B+ Tree Node Pages                   │
└─────────────────────────────────────────┘
```

## Header Structure (Fixed Size)
```
Field                        | Type    | Description
-----------------------------|---------|----------------------------------
magic_number                 | 4 bytes | File format identifier
version                      | 4 bytes | Schema version
page_size                    | 4 bytes | Size of each page (e.g., 8192)
total_page_count             | 4 bytes | Total pages in file
bitmap_start_page            | 4 bytes | Page number where bitmap starts
bitmap_page_count            | 4 bytes | Number of pages used for bitmap
free_space_map_start_page    | 4 bytes | Page number where FSM starts
free_space_map_page_count    | 4 bytes | Number of pages used for FSM
collections_metadata_page    | 4 bytes | Page with collection metadata
mmap_hint                    | 1 byte  | 0=disabled, 1=enabled (for platforms supporting MemoryMappedFile)
last_commit_frame            | 4 bytes | Last committed WAL frame number (0 if none)
wal_checksum                 | 8 bytes | Checksum of WAL at last commit (e.g., CRC64)
(padding to page boundary)   | varies  | Reserved for future fields
```

## Bitmap Pages
```
- Each bit represents one page (0 = free, 1 = allocated)
- Stored in consecutive pages starting at bitmap_start_page
- Size: ceil(total_page_count / 8) bytes
```

## Free Space Map (FSM) Pages
```
- 2 bits per page indicating free space level
- Encoding:
  00 = full (0-10% free space)
  01 = low (10-40% free space)
  10 = medium (40-70% free space)
  11 = high (70-100% free space)
- Stored in consecutive pages starting at free_space_map_start_page
- Size: ceil(total_page_count * 2 / 8) bytes
- Updated whenever documents are added/removed from a page
```

## Collections Metadata Page Structure
```
Field             | Type               | Description
------------------|--------------------|---------------------------------
collection_count  | 4 bytes            | Number of collections
entries           | variable           | Array of collection entries

Per Collection Entry:
  name_length     | 4 bytes            | Length of collection name
  name            | name_length bytes  | Collection name (UTF-8)
  root_page       | 4 bytes            | B+ tree root page for collection
  doc_count       | 4 bytes            | Number of documents
  next_id         | 4 bytes            | Next available document ID (start at 1)
  (future fields) | varies             | Reserved
```

## Document Page Structure (Slotted Pages)
```
┌──────────────────────────────────────────┐
│ Page Header                              │
│ - page_type: 2 byte (0x01 = document)    │
│ - flags: 1 byte (e.g. 0 = compressed)    │
│ - slot_count: 2 bytes                    │
│ - free_space_offset: 2 bytes             │
│ - free_space_end: 2 bytes                │
│ - crc: 4 bytes                           │
├──────────────────────────────────────────┤
│ Slot Array (grows down)                  │
│ [slot_0][slot_1][slot_2]...              │
├──────────────────────────────────────────┤
│ Free Space                               │
├──────────────────────────────────────────┤
│ Document Data (grows up)                 │
│ [...doc_2][doc_1][doc_0]                 │
└──────────────────────────────────────────┘

Slot Entry Structure:
  page_count    | 4 bytes  | Number of pages document spans
  page_ids      | 4*N bytes| Array of page IDs (if multi-page)
  total_size    | 4 bytes  | Total document size in bytes
  offset        | 4 bytes  | Offset to data in this page
  length        | 4 bytes  | Length of data in this page
```

## B+ Tree Node Page Structure
```
┌──────────────────────────────────────────┐
│ Page Header                              │
│ - page_type: 1 byte (0x02 = b+tree)      │
│ - node_type: 1 byte (0=internal, 1=leaf) │
│ - key_count: 2 bytes                     │
│ - next_leaf: 4 bytes (leaf only, 0 if N/A)│
├──────────────────────────────────────────┤
│ Keys Array                               │
│ [key_0][key_1][key_2]...                 │
│ (document IDs as integers)               │
├──────────────────────────────────────────┤
│ Values/Pointers Array                    │
│                                          │
│ If Internal Node:                        │
│   [child_page_0][child_page_1]...        │
│                                          │
│ If Leaf Node:                            │
│   [(page_id, slot_index)]...             │
│   - page_id: 4 bytes                     │
│   - slot_index: 2 bytes                  │
└──────────────────────────────────────────┘
```

## Write Workflow Example: Insert Book Document (3KB)

### Step 1: Serialize Document
```
Book object → JSON (GaldrJson) → UTF-8 bytes → 3KB byte array
```

### Step 2: Find Suitable Page
```
1. Calculate required space: 3KB data + slot overhead (~20 bytes) = ~3020 bytes
2. Determine free space needed: ~37% of 8KB page
3. Check FSM for pages with medium (10) or high (11) free space in books collection
4. Options:
   a) If suitable page found → use that page (go to Step 3a)
   b) If no suitable page → allocate new page (go to Step 3b)
```

### Step 3a: Write to Existing Page (Page 120)
```
1. Read page 120
2. Check actual free space in page header
3. Add new slot entry:
   - page_count: 1
   - page_ids: [120]
   - total_size: 3072
   - offset: 5120 (where data starts in page)
   - length: 3072
4. Write document data at offset 5120
5. Update page header:
   - increment slot_count
   - update free_space_offset
6. Calculate new free space: ~38% remaining
7. Update FSM for page 120: set to 01 (low free space)
8. Write page 120 back to disk
```

### Step 3b: Write to New Page (No Suitable Page Found)
```
1. Check bitmap for free page (bit = 0) → find page 150
2. Set bitmap bit for page 150 to 1 (allocated)
3. Initialize page 150:
   - page_type: 0x01 (document)
   - slot_count: 0
   - free_space_offset: header_size
   - free_space_end: page_size
4. Add first slot entry:
   - page_count: 1
   - page_ids: [150]
   - total_size: 3072
   - offset: page_size - 3072
   - length: 3072
5. Write document data
6. Update page header: slot_count = 1
7. Calculate free space: ~62% remaining
8. Update FSM for page 150: set to 10 (medium free space)
9. Write page 150 to disk
```

### Step 4: Update B+ Tree Index
```
1. Read collections metadata → find "books" root_page: 50
2. Traverse B+ tree to find insertion point for document ID
3. Insert entry in leaf node: doc_id → (page: 150, slot: 0)
4. If leaf is full, split and update parent nodes
5. Write modified B+ tree nodes back to disk
6. Update FSM for any B+ tree pages modified
```

### Step 5: Update Collection Metadata
```
1. Read collections metadata page
2. Increment doc_count for "books" collection
3. Write collections metadata page back to disk
```

## Multi-Page Document Write Example: Large Book (20KB)

### Document spans multiple pages:
```
1. Serialize: 20KB byte array
2. Calculate pages needed: ceil(20KB / ~8KB usable) = 3 pages
3. Find/allocate 3 pages from FSM/bitmap: pages [200, 201, 202]
4. Write to page 200 (first page):
   - Slot entry:
     - page_count: 3
     - page_ids: [200, 201, 202]
     - total_size: 20480
     - offset: X
     - length: ~7KB (usable in page 200)
   - Write first 7KB of document
5. Write to page 201 (continuation):
   - No slot needed (continuation page)
   - Write next ~8KB chunk
6. Write to page 202 (final):
   - Write remaining ~5KB
7. Update FSM for all 3 pages
8. Update B+ tree: doc_id → (page: 200, slot: 0)
```

## Query Flow Example: Get Book with ID 5
```
1. Read header → collections_metadata_page
2. Read collections metadata → find "books" → root_page: 50
3. Read page 50 (B+ tree root for books collection)
4. Navigate tree following keys until leaf with ID 5
5. Leaf returns: (page_id: 120, slot: 2)
6. Read page 120, slot 2 → get slot metadata
   - page_count: 3
   - page_ids: [120, 135, 140]
   - total_size: 18KB
7. Read pages 120, 135, 140
8. Concatenate bytes from all pages → 18KB total
9. UTF-8 string → JSON deserialize → Book object
10. Return book
```

## Document Storage Format
```
Documents stored as:
  C# Object → JSON (GaldrJson) → UTF-8 bytes → Page(s)

Reading:
  Page(s) → UTF-8 bytes → UTF-8 string → JSON deserialize → C# Object
```

## API Design (AOT Compatible)
```csharp
// Get by ID
var book = db.GetById<Book>(5);

// Get by ID with includes
var book = db.GetById<Book>(5).Include(b => b.Authors);

// Insert
db.Insert(book);

// Collection mapping
[Collection("books")]
class Book 
{
    public int Id { get; set; }
    public string Title { get; set; }
    public List<int> AuthorIds { get; set; }
    public List<Author> Authors { get; set; } // Populated by Include()
}
```

## FSM Update Rules
```
After any write/delete operation on a page:
1. Calculate new free space percentage
2. Determine FSM value:
   - 0-10% free → 00 (full)
   - 10-40% free → 01 (low)
   - 40-70% free → 10 (medium)
   - 70-100% free → 11 (high)
3. Update FSM entry for that page
4. Write FSM page(s) to disk
```

## WAL File Format (Separate File: db.wal)

┌─────────────────────────────────────────┐
│ WAL Header (fixed size, e.g., 32 bytes) │
│ - magic_number: 4 bytes (e.g., 'WALD')  │
│ - version: 4 bytes                      │
│ - page_size: 4 bytes (matches main DB)  │
│ - frame_count: 4 bytes                  │
│ - salt1: 4 bytes (random for uniqueness)│
│ - salt2: 4 bytes (random)               │
│ - checksum: 8 bytes (CRC64 over header) │
└─────────────────────────────────────────┘
┌─────────────────────────────────────────┐
│ Frames (variable, appended sequentially)│
│ Frame 1                                 │
│ Frame 2                                 │
│ ...                                     │
└─────────────────────────────────────────┘

Per Frame Structure:
  - frame_number: 4 bytes (sequential, starting at 1)
  - change_type: 1 byte (0x01=page update, 0x02=metadata update, 0x03=FSM/bitmap chunk, reserve others)
  - payload_size: 4 bytes
  - payload: payload_size bytes (e.g., full page data for type 0x01, or diff for optimization)
  - commit_flag: 1 byte (1 if this frame ends a txn, 0 otherwise—for simple POC, every op is a txn)
  - frame_checksum: 4 bytes (CRC32 over frame)

Examples:
- For Page Update (change_type=0x01): Payload = page_id (4 bytes) + full page bytes (page_size).
- For Metadata Update (0x02): Payload = collection_name_length (4) + name + fields like next_id/doc_count.
- For FSM/Bitmap (0x03): Payload = start_page (4) + chunk_size (4) + chunk bytes.

## Notes
- All multi-byte integers stored in little-endian format
- Page numbering starts at 0
- Page 0 is always the header
- Collections metadata can overflow to multiple pages if needed
- B+ tree leaf nodes are linked for efficient range scans
- FSM allows O(1) lookup of pages with sufficient free space
- Free space in slotted pages compacted as needed
- FSM is updated on every document insert/update/delete

