# Design Document: Constant-Time In-Memory Snapshotting (Immutable Base + Indexed Delta-Segment Store)

| | |
|---|---|
| **Document Status** | PROPOSED / IMPLEMENTATION-SAFE DESIGN |
| **Component** | Hazelcast CP Subsystem / Raft State Machine / CPMap & CP Services |
| **Target Version** | 5.6+ |
| **Authors / Contributors** | Hazelcast CP Core Team |

---

### Table of Contents
1. [Executive Summary](#1-executive-summary)
2. [Root Cause Analysis & Design Evolution](#2-root-cause-analysis--design-evolution)
   - [Why the Previous Backward-Linked Chain Failed](#21-why-the-previous-backward-linked-chain-failed)
   - [Why True O(1) Snapshot Cut Requires Pre-Allocated Fixed Buffers](#22-why-true-o1-snapshot-cut-requires-pre-allocated-fixed-buffers)
   - [Why Follower Restore Must Materialize Off-Thread](#23-why-follower-restore-must-materialize-off-thread)
   - [Realistic Memory Footprint Breakdown](#24-realistic-memory-footprint-breakdown)
   - [Admission Control vs. Post-Consensus Invariants](#25-admission-control-vs-post-consensus-invariants)
3. [Terminology & System Identifiers](#3-terminology--system-identifiers)
4. [Architecture & Core Data Structures](#4-architecture--core-data-structures)
   - [Group-Wide Snapshot Coordination Model](#41-group-wide-snapshot-coordination-model)
   - [Indexed Delta-Segment Store (`DeltaSegmentStore`)](#42-indexed-delta-segment-store-deltasegmentstore)
   - [Fixed-Capacity Active Delta Buffer](#43-fixed-capacity-active-delta-buffer)
   - [Lease Model with Unique Representation IDs](#44-lease-model-with-unique-representation-ids)
5. [Partition-Thread Hot-Path Execution](#5-partition-thread-hot-path-execution)
   - [Pre-Consensus Admission & Quota Reservation](#51-pre-consensus-admission--quota-reservation)
   - [Mutation Recording Flow](#52-mutation-recording-flow)
   - [Group-Level O(1) Exact-Cut Algorithm](#53-group-level-o1-exact-cut-algorithm)
   - [O(1) Logical Log Prefix Detachment](#54-o1-logical-log-prefix-detachment)
6. [Decoupled Background Compaction](#6-decoupled-background-compaction)
   - [Compaction Triggers & Memory Pre-Reservation](#61-compaction-triggers--memory-pre-reservation)
   - [Off-Thread Compactor Pipeline & LWW Deduplication](#62-off-thread-compactor-pipeline--lww-deduplication)
   - [Partition-Thread Lineage-CAS Promotion](#63-partition-thread-lineage-cas-promotion)
   - [Segment Store Reclamation Floor Advancement](#64-segment-store-reclamation-floor-advancement)
7. [Follower State Transfer & Off-Thread Restore Engine](#7-follower-state-transfer--off-thread-restore-engine)
   - [Streaming Wire Protocol & Frame Reservation](#71-streaming-wire-protocol--frame-reservation)
   - [Off-Thread Materialization (`PreparedGroupState`)](#72-off-thread-materialization-preparedgroupstate)
   - [Zero-Downtime Root Reference Swap on Partition Thread](#73-zero-downtime-root-reference-swap-on-partition-thread)
   - [Container Abstraction Updates for CPMap & Services](#74-container-abstraction-updates-for-cpmap--services)
8. [Service-Specific Delta Capture Specifications](#8-service-specific-delta-capture-specifications)
9. [Comprehensive Memory Budgeting & Accounting](#9-comprehensive-memory-budgeting--accounting)
10. [Bootstrap & Initial Group Lifecycle](#10-bootstrap--initial-group-lifecycle)
11. [Design Comparison & Complexity Analysis](#11-design-comparison--complexity-analysis)
12. [Failure Modes & Fault Invariants](#12-failure-modes--fault-invariants)
13. [Configuration & Observability](#13-configuration--observability)
14. [Verification, Simulation & Testing Plan](#14-verification-simulation--testing-plan)

---

## 1. Executive Summary

State machine snapshotting in distributed consensus systems (such as the Hazelcast CP Subsystem based on Raft) is required to bound the Raft log size, reclaim storage, and bring lagging or newly joined followers up to date. However, snapshotting must never stall the partition thread or introduce tail-latency jitter into the mutation hot path.

Earlier naive implementations suffered from $O(N)$ full-map scanning pauses on the partition thread. Alternative proposals based on 128-bit persistent Radix/Merkle-page trees introduce extreme implementation complexity, heavy GC allocation churn on every write, and cache-miss indirection.

This document presents an **implementation-safe, constant-time, in-memory snapshotting architecture** based on an **Immutable Base + Indexed Delta-Segment Store**:

```
                                  GROUP COORDINATOR ARCHITECTURE
+----------------------------------------------------------------------------------------------------+
|                                         PARTITION THREAD                                           |
|                                                                                                    |
|  Writes ---> [ Admission Quota Check ] ---> Apply to Live Store ---> [ Fixed-Capacity Delta Buffer ] |
|                                                                                 |                  |
|                                                                 (At Cut Index C)| O(1) Detach &    |
|                                                                                 | Swap Pre-Reserved|
|                                                                                 v                  |
|                                                                   [ Frozen Delta Segment C ]       |
+---------------------------------------------------------------------------------|------------------+
                                                                                  |
                                                                                  v
+----------------------------------------------------------------------------------------------------+
|                              INDEXED DELTA-SEGMENT STORE (Per Group)                               |
|                                                                                                    |
|   Segment Seq:       [101]          [102]          [103]          [104]          [105]             |
|   Index Range:    (C0+1 .. C1)   (C1+1 .. C2)   (C2+1 .. C3)   (C3+1 .. C4)   (C4+1 .. C5)         |
|   Reclamation Floor = 101                                                       ^                  |
|                                                                                 |                  |
|   +-------------------+                                                         |                  |
|   |   BaseImage(B0)   | + DeltaSequenceRange [ 101 ... 105 ] -------------------+                  |
|   |  (Gen=1, Idx=C0)  |                                                                            |
|   +-------------------+                                                                            |
+-------------|--------------------------------------------------------------------------------------+
              |
              | (Compaction Trigger: chainDepth or chainBytes exceeded)
              v
+-------------------------------------------------------------------+
|               BACKGROUND COMPACTOR (Serial Off-Thread)            |
|                                                                   |
| 1. Lease BaseImage(B0) + Segments [101..103]                      |
| 2. Off-thread materialize: NewBase(C3) = Base(B0) + Deltas[101..103]|
| 3. Submit Lineage-CAS to Partition Thread:                        |
|    - CAS: if (currentBase.gen == 1) -> install BaseImage(C3, gen=2) |
|           and advance active sequence range to [104..105]         |
| 4. Advance Store Reclamation Floor in O(1) to 104                 |
+-------------------------------------------------------------------+
```

### Core Architectural Guarantees:
1. **Guaranteed Strict $O(1)$ Snapshot Cut**: Fixed-capacity pre-allocated arrays, pre-reserved clean replacement buffers, zero allocations or resizings on the partition thread, and incremental metadata maintenance.
2. **Safe Compaction via Indexed Sequence Ranges**: Discarding backward `predecessor` object graphs in favor of monotonic integer sequence ranges `[firstSeq ... lastSeq]` in an append-only segment store. Compaction replaces the base and range with a single lineage-validated pointer swap.
3. **Zero-Pause Off-Thread Follower Restore**: Follower snapshot ingestion builds all replacement service state off-thread (`PreparedGroupState`); the partition thread executes strictly an $O(S)$ pointer swap across registered services ($S \le 10$).
4. **Pre-Consensus Admission Control**: Backpressure is enforced at proposal admission **before** Raft consensus. Once an entry is committed by Raft, voters guarantee application without local buffer exhaustion.
5. **Exact Memory Accounting**: Explicit tracking of 9 distinct memory pools accounting for peak concurrent multi-base lifecycles ($2.0\times - 3.5\times$ baseline).

---

## 2. Root Cause Analysis & Design Evolution

### 2.1 Why the Previous Backward-Linked Chain Failed

In a backward-linked design (`DeltaNode` pointing to `DeltaNode predecessor`), each snapshot tail holds a direct transitive object reference to the entire history back to the initial base. 

```
Initial: Base(B0) <--- Node(1) <--- Node(2) <--- Node(3) <--- Node(4) [Tail]
Compactor produces Base(B2) covering Node(1) and Node(2).
Problem: Tail Node(4) still has hard reference: Node(4) -> Node(3) -> Node(2) -> Node(1) -> Base(B0).
```

To trim `Node(1)` and `Node(2)` from the live snapshot, the system would have to:
- Mutate previously published immutable nodes (violating thread-safety and corrupting concurrent snapshot readers); or
- Deep-copy/rebuild the active suffix `[Node(3), Node(4)]` on the partition thread ($O(\text{suffix length})$ pause).

**Solution**: Replace pointer graphs with an **Indexed Delta-Segment Store**. A snapshot view is defined as:
$$\text{SnapshotView} = (\text{BaseRef}, \text{firstSequence}, \text{lastSequence})$$
Compaction simply updates the view to $(\text{NewBase}, \text{compactedSequence} + 1, \text{currentTailSequence})$. No node reconstruction or mutation is required.

---

### 2.2 Why True O(1) Snapshot Cut Requires Pre-Allocated Fixed Buffers

The following operations in naive delta implementations inadvertently violate $O(1)$ execution:
- `entries.toArray(new DeltaEntry[0])`: Takes $O(\Delta)$ CPU time and triggers GC allocations.
- `calculateChainBytes(tail)`: Traverses the linked chain taking $O(\text{chain depth})$ time.
- `bufferPool.acquireCleanBuffer()`: May allocate, clear memory, or contend on locks during the snapshot cut.
- `ArrayList.add()`: May trigger dynamic array growth and array copy during write mutations.

**Solution**:
1. **Fixed-Capacity Array Buffers**: `ActiveDeltaBuffer` uses pre-sized arrays (`DeltaEntry[]`).
2. **Ownership Detach**: At the snapshot cut, the active buffer instance is detached directly into a `FrozenArrayDelta` wrapper without cloning or array allocation ($O(1)$).
3. **Pre-Reservation Invariant**: A clean replacement buffer is reserved and verified *prior* to reaching the snapshot commit index.
4. **Incremental Accounting**: Delta entry count, byte size, and sequence ranges are updated incrementally during each write operation ($O(1)$).

---

### 2.3 Why Follower Restore Must Materialize Off-Thread

Iterating over base chunks and applying deltas directly on the partition thread during snapshot restore performs $O(N + \Delta)$ work, stalling Raft consensus and causing heartbeat timeouts.

**Solution**:
- All snapshot streaming frames (`BaseImageFrame`, `DeltaSegmentFrame`) are routed to an off-thread `FollowerRestoreWorker`.
- The worker builds complete, fully materialized live container instances (`PreparedGroupState`).
- The partition thread only validates Raft term freshness, swaps the top-level container root pointers ($O(S)$ where $S$ is the number of services), installs the `SnapshotEntry`, and schedules old container deallocation asynchronously.

---

### 2.4 Realistic Memory Footprint Breakdown

Previous claims of $1.1\times - 1.2\times$ memory overhead did not account for concurrent operational lifecycles. Under realistic production conditions, the total retained in-memory footprint is composed of 9 discrete allocations:

$$\text{Total Memory} = M_{\text{live}} + M_{\text{active\_delta}} + M_{\text{frozen\_deltas}} + M_{\text{current\_base}} + M_{\text{leased\_old\_bases}} + M_{\text{compaction\_workspace}} + M_{\text{restore\_staging}} + M_{\text{transfer\_buffers}} + M_{\text{pending\_reclamation}}$$

```
+-----------------------------------------------------------------------------------+
|                           MEMORY FOOTPRINT EVOLUTION                              |
|                                                                                   |
| 1. Steady State:                                                                  |
|    [ Live Mutable Store (1.0x) ] + [ BaseImage (1.0x) ] + [ Deltas (0.1x-0.3x) ]  |
|    Total = ~2.1x - 2.3x                                                           |
|                                                                                   |
| 2. Peak During Compaction:                                                        |
|    Steady State (2.2x) + [ Compaction Workspace (1.0x) ]                          |
|    Total = ~3.2x                                                                  |
|                                                                                   |
| 3. Peak During Compaction + Concurrent Streaming Follower Transfer:               |
|    Steady State (2.2x) + Compaction (1.0x) + [ Pinned Old Base (1.0x) ]           |
|    Total = ~4.2x (Bounded by strict admission quotas)                             |
+-----------------------------------------------------------------------------------+
```

---

### 2.5 Admission Control vs. Post-Consensus Invariants

> **CRITICAL RAFT INVARIANT**: Once a log entry is committed by a Raft quorum, every voter **must** apply it deterministically. A node cannot reject a committed entry locally with a runtime `SnapshotOverloadException`.

Therefore, memory admission control and buffer capacity checks must occur **at proposal admission** before the command enters Raft consensus:
1. The leader verifies that active delta buffers and memory budgets have sufficient headroom for the worst-case mutation size.
2. If memory or buffer capacity limits are exceeded, the proposal is rejected or throttled **at the client API layer**.
3. Once committed, if an unexpected buffer overflow occurs during local application, the node must **fail-stop** (terminate) to prevent state divergence.

---

## 3. Terminology & System Identifiers

| Term | Definition |
|---|---|
| **`ServiceId`** | Compact primitive numeric identifier (`short` / `byte`) assigned to registered CP services (e.g., `0x01 = CPMap`, `0x02 = AtomicLong`, `0x03 = SessionRegistry`, `0x04 = MetadataGroup`). |
| **`SnapshotRepresentationId`** | Globally unique identifier for a physical snapshot representation: `(groupId, representationGen, snapshotIndex)`. Distinguishes different physical materializations of the same logical snapshot index. |
| **`BaseRef`** | Immutable reference to a baseline state image tagged with `(baseGeneration, baseIndex, totalBytes)`. |
| **`DeltaSegment`** | Fixed chunk of frozen mutations assigned a monotonic `deltaSequence` number and covering Raft index range `[fromIndex ... toIndex]`. |
| **`DeltaSegmentStore`** | Append-only indexed ring-buffer / list holding immutable `DeltaSegment` instances per CP group. |
| **`SnapshotImageLease`** | Non-resurrecting reference-counted lease pinned to a specific `SnapshotRepresentationId`. |
| **`PreparedGroupState`** | Fully constructed live service containers materialized off-thread by follower restore workers, ready for $O(S)$ pointer swapping. |
| **`PreparedCompaction`** | Off-thread materialized new base image with validation tokens, ready for partition-thread Lineage-CAS promotion. |

---

## 4. Architecture & Core Data Structures

### 4.1 Group-Wide Snapshot Coordination Model

Snapshots are managed cohesively at the **CP Group** level rather than independently by individual services.

```java
package com.hazelcast.cp.internal.datastructures.snapshot.v2;

/**
 * Unique identifier for a physical snapshot representation.
 */
public record SnapshotRepresentationId(
    CPGroupId groupId,
    long representationGen,
    long snapshotIndex
) {}

/**
 * Metadata for a coordinated group snapshot.
 */
public record GroupSnapshotMetadata(
    SnapshotRepresentationId representationId,
    int snapshotTerm,
    long timestamp,
    long totalEstimatedBytes,
    int memberListVersion,
    Collection<RaftEndpoint> members
) {}

/**
 * Coordinated snapshot view across all services in a CP group.
 */
public record GroupSnapshotView(
    GroupSnapshotMetadata metadata,
    BaseRef baseRef,
    long firstDeltaSequence,
    long lastDeltaSequence,
    Map<Short, ServiceSnapshotView> services,
    SnapshotImageLease lease
) implements AutoCloseable {
    @Override
    public void close() {
        if (lease != null) {
            lease.release();
        }
    }
}

/**
 * Per-service view within the group snapshot.
 */
public record ServiceSnapshotView(
    short serviceId,
    short codecVersion,
    Data serviceMetadata,
    long deltaEntryCount
) {}

/**
 * Immutable reference to a materialized base image.
 */
public record BaseRef(
    long baseGeneration,
    long baseIndex,
    int baseTerm,
    long totalEstimatedBytes,
    Map<Short, Object> serviceBases
) {}
```

---

### 4.2 Indexed Delta-Segment Store (`DeltaSegmentStore`)

The `DeltaSegmentStore` manages an indexed sequence of immutable `DeltaSegment` instances per CP group:

```java
package com.hazelcast.cp.internal.datastructures.snapshot.v2;

public interface DeltaSegmentStore {
    /**
     * Appends a frozen segment to the store and returns its assigned sequence number.
     * Thread-safety: Called only on partition thread.
     */
    long appendSegment(FrozenDeltaSegment segment);

    /**
     * Acquires an immutable leased range of delta segments [firstSeq, lastSeq].
     * Thread-safety: Safe for concurrent off-thread readers.
     */
    LeasedDeltaRange acquireRange(long firstSeq, long lastSeq);

    /**
     * Advances the reclamation floor. Segments with sequence < floorSeq become eligible
     * for physical deallocation once all active leases are released.
     */
    void advanceReclamationFloor(long floorSeq);

    /**
     * Returns the current lowest retained sequence number.
     */
    long getReclamationFloor();

    /**
     * Returns total bytes held by all active and pending-reclamation segments.
     */
    long getTotalRetainedBytes();
}
```

```java
public record FrozenDeltaSegment(
    long sequence,
    long fromRaftIndex,
    long toRaftIndex,
    int snapshotTerm,
    int entryCount,
    long byteSize,
    DeltaEntry[] entries
) {}
```

---

### 4.3 Fixed-Capacity Active Delta Buffer

`ActiveDeltaBuffer` operates strictly on pre-allocated fixed arrays. No resizing or allocations occur during state mutations or snapshot cuts.

```java
package com.hazelcast.cp.internal.datastructures.snapshot.v2;

public final class ActiveDeltaBuffer {
    private final int capacity;
    private final DeltaEntry[] entries;
    private int size;
    private long estimatedBytes;

    public ActiveDeltaBuffer(int capacity) {
        this.capacity = capacity;
        this.entries = new DeltaEntry[capacity];
        this.size = 0;
        this.estimatedBytes = 0;
    }

    public boolean hasCapacity(int additionalEntries, long additionalBytes, long maxBytesBudget) {
        return (size + additionalEntries <= capacity) && (estimatedBytes + additionalBytes <= maxBytesBudget);
    }

    public void append(short serviceId, byte opCode, Data key, Data value, long raftIndex, int raftOrdinal) {
        assert size < capacity : "Buffer overflow on partition thread!";
        DeltaEntry entry = new DeltaEntry(serviceId, opCode, key, value, raftIndex, raftOrdinal);
        entries[size++] = entry;
        estimatedBytes += entry.estimatedSize();
    }

    /**
     * O(1) non-allocating detachment. Transfers ownership of the internal array
     * to the FrozenDeltaSegment without copying.
     */
    public FrozenDeltaSegment detach(long sequence, long fromIndex, long toIndex, int term) {
        DeltaEntry[] frozenEntries = this.entries;
        int frozenSize = this.size;
        long frozenBytes = this.estimatedBytes;

        return new FrozenDeltaSegment(
            sequence, fromIndex, toIndex, term, frozenSize, frozenBytes, frozenEntries
        );
    }

    public void reset() {
        // Fast nulling of array slots for GC reclamation
        Arrays.fill(entries, 0, size, null);
        this.size = 0;
        this.estimatedBytes = 0;
    }
}
```

```java
public record DeltaEntry(
    short serviceId,
    byte opCode,        // 0x01 = PUT, 0x02 = REMOVE, 0x03 = SERVICE_STATE
    Data key,
    Data value,
    long raftIndex,     // Raft log index
    int raftOrdinal     // Sub-operation order within the Raft log entry
) {
    public long estimatedSize() {
        return 24 + (key != null ? key.totalSize() : 0) + (value != null ? value.totalSize() : 0);
    }
}
```

---

### 4.4 Lease Model with Unique Representation IDs

Leases prevent premature reclamation of base images and delta segments during off-thread compaction or follower streaming transfers.

```java
package com.hazelcast.cp.internal.datastructures.snapshot.v2;

public class SnapshotImageLeaseManager {
    private final ConcurrentMap<SnapshotRepresentationId, LeasedRepresentation> representations = new ConcurrentHashMap<>();

    public SnapshotImageLease acquire(SnapshotRepresentationId repId, BaseRef base, LeasedDeltaRange deltaRange) {
        LeasedRepresentation rep = representations.compute(repId, (id, existing) -> {
            if (existing == null) {
                return new LeasedRepresentation(repId, base, deltaRange);
            }
            if (!existing.tryRetain()) {
                throw new IllegalStateException("Cannot acquire lease on retired representation: " + repId);
            }
            return existing;
        });

        return new SnapshotImageLease(repId, rep::release);
    }

    private static final class LeasedRepresentation {
        private final SnapshotRepresentationId repId;
        private final BaseRef base;
        private final LeasedDeltaRange deltaRange;
        private final AtomicInteger refCount = new AtomicInteger(1);
        private volatile boolean retired = false;

        LeasedRepresentation(SnapshotRepresentationId repId, BaseRef base, LeasedDeltaRange deltaRange) {
            this.repId = repId;
            this.base = base;
            this.deltaRange = deltaRange;
        }

        boolean tryRetain() {
            while (true) {
                int count = refCount.get();
                if (count <= 0 || retired) {
                    return false;
                }
                if (refCount.compareAndSet(count, count + 1)) {
                    return true;
                }
            }
        }

        void release() {
            if (refCount.decrementAndGet() == 0) {
                retired = true;
                deltaRange.close();
                // Asynchronously trigger physical reclamation of base & segments
            }
        }
    }
}
```

---

## 5. Partition-Thread Hot-Path Execution

### 5.1 Pre-Consensus Admission & Quota Reservation

```
Client Mutation Request
        |
        v
[ Proposer / Leader Admission Stage ]
  - Check ActiveDeltaBuffer capacity: (currentSize + 1 <= maxEntries)
  - Check Memory Budget: (allocatedBytes + worstCaseDeltaBytes <= maxDeltaBudget)
  - Check Compaction Backpressure: (currentChainDepth < maxChainDepthThreshold)
        |
   +----+----+
   |         |
 (Pass)   (Fail) ---> Reject Request at Client Layer with OverloadRetryException
   |
   v
Append Proposal to Raft Log & Replicate to Quorum
```

---

### 5.2 Mutation Recording Flow

When a committed Raft operation executes on the partition thread:

```java
// Executed on Partition Thread during Raft apply
public void applyCommittedMutation(RaftOp op, long commitIndex, int ordinal) {
    assert partitionChecker.isPartitionThread();

    // 1. Mutate live state container
    liveMapStore.put(op.getKey(), op.getValue());

    // 2. Record mutation into active delta buffer (O(1) direct array write)
    activeDeltaBuffer.append(
        CPMapService.SERVICE_ID,
        DeltaOpCodes.PUT,
        op.getKey(),
        op.getValue(),
        commitIndex,
        ordinal
    );

    // 3. Incrementally update running metrics
    incrementalGroupMetrics.onMutationAppended(op.getKey().totalSize() + op.getValue().totalSize());
}
```

---

### 5.3 Group-Level O(1) Exact-Cut Algorithm

When `commitIndex - lastSnapshotIndex >= snapshotInterval`:

```
               PARTITION THREAD O(1) SNAPSHOT CUT SEQUENCE

 1. Verify: commitIndex == lastAppliedIndex == C
 2. Verify: Pre-reserved Clean Buffer (reservedBuffer != null)
 3. Detach Active Buffer -> FrozenDeltaSegment (Sequence = nextSeq++)
 4. Swap: activeDeltaBuffer = reservedBuffer
 5. Reset reservedBuffer slot and post asynchronous request for next buffer allocation
 6. Append FrozenDeltaSegment to DeltaSegmentStore
 7. Formulate Candidate GroupSnapshotView(BaseRef, firstSeq, currentSeq)
 8. Create Raft SnapshotEntry(termAt(C), C, GroupSnapshotView)
 9. Logically Detach Raft Log Prefix up to (C - logsToKeep) in O(1)
10. Update lastSnapshotIndex = C
11. Lineage Registration: Publish Candidate View to Group State Holder
```

```java
// Executed on Partition Thread
public GroupSnapshotView takeGroupSnapshotCut(long commitIndex) {
    assert partitionChecker.isPartitionThread();
    assert commitIndex == raftState.lastApplied();
    assert spareBufferReserve != null : "Clean buffer MUST be reserved before cut!";

    long prevSnapshotIndex = groupState.lastSnapshotIndex();
    int snapshotTerm = raftLog.getLogEntry(commitIndex).term();
    long seq = deltaStore.nextSequence();

    // 1. Detach active buffer into immutable frozen segment (O(1))
    FrozenDeltaSegment frozenSegment = activeDeltaBuffer.detach(seq, prevSnapshotIndex + 1, commitIndex, snapshotTerm);

    // 2. Swap active buffer with pre-reserved clean buffer (O(1))
    activeDeltaBuffer = spareBufferReserve;
    spareBufferReserve = null;
    triggerAsyncBufferPreAllocation(); // Allocate next reserve buffer off-thread

    // 3. Append segment to indexed store (O(1))
    deltaStore.appendSegment(frozenSegment);

    // 4. Construct snapshot metadata with incrementally maintained counters (O(1))
    long currentRepGen = groupState.nextRepresentationGeneration();
    SnapshotRepresentationId repId = new SnapshotRepresentationId(groupId, currentRepGen, commitIndex);
    
    GroupSnapshotMetadata metadata = new GroupSnapshotMetadata(
        repId, snapshotTerm, System.currentTimeMillis(),
        groupState.currentBaseRef().totalEstimatedBytes() + deltaStore.getActiveRangeBytes(),
        raftState.memberListVersion(), raftState.members()
    );

    LeasedDeltaRange deltaRange = deltaStore.acquireRange(groupState.firstActiveSequence(), seq);
    SnapshotImageLease lease = leaseManager.acquire(repId, groupState.currentBaseRef(), deltaRange);

    GroupSnapshotView snapshotView = new GroupSnapshotView(
        metadata, groupState.currentBaseRef(),
        groupState.firstActiveSequence(), seq,
        groupState.captureServiceViews(), lease
    );

    // 5. Install SnapshotEntry in Raft log and detach log prefix
    SnapshotEntry snapshotEntry = new SnapshotEntry(snapshotTerm, commitIndex, snapshotView, raftState.memberIndex(), raftState.members());
    long truncateIndex = calculateLogTruncationIndex(commitIndex);
    raftLog.setSnapshotLogical(snapshotEntry, truncateIndex);

    // 6. Finalize state
    groupState.setLatestSnapshot(snapshotView);
    groupState.setLastSnapshotIndex(commitIndex);

    // 7. Check compaction triggers
    checkCompactionTriggers(deltaStore.getActiveRangeDepth(), deltaStore.getActiveRangeBytes());

    return snapshotView;
}
```

---

### 5.4 O(1) Logical Log Prefix Detachment

To ensure the partition thread never blocks during log truncation:
- `raftLog.setSnapshotLogical(...)` updates the logical log start offset and snapshot pointer in $O(1)$.
- Physical file unlinking, segment truncation, or memory deallocation of old log entries is dispatched to a background I/O worker.

---

## 6. Decoupled Background Compaction

### 6.1 Compaction Triggers & Memory Pre-Reservation

Compaction triggers on the following conditions:
$$\text{rangeDepth} \ge \text{maxDeltaSegments} \quad \lor \quad \text{rangeBytes} \ge \text{maxDeltaBytes} \quad \lor \quad \text{timeSinceLastCompaction} \ge \text{maxCompactionInterval}$$

**Pre-Reservation Check**: Before launching the background worker, the system must reserve memory for the estimated new base ($M_{\text{current\_base}} \times 1.1$). If memory is unavailable, the admission controller throttles new mutations until memory frees up.

---

### 6.2 Off-Thread Compactor Pipeline & LWW Deduplication

```
                  BACKGROUND COMPACTION PIPELINE (Off-Thread)

 1. Acquire SnapshotImageLease on current GroupSnapshotView (Pin BaseRef & Deltas)
 2. Initialize BaseImageBuilder from BaseRef.serviceBases
 3. Stream DeltaSegments [firstSeq ... targetSeq]:
    - Group mutations by serviceId
    - For CPMap: Sort by (Key, RaftIndex, RaftOrdinal)
    - Apply Last-Write-Wins (LWW) collapse:
         PUT(K, V1) @ (Idx=10, Ord=1) + PUT(K, V2) @ (Idx=12, Ord=1) => PUT(K, V2)
         PUT(K, V2) @ (Idx=12, Ord=1) + REMOVE(K)   @ (Idx=15, Ord=1) => REMOVE(K)
 4. Apply deduplicated entries into BaseImageBuilder
 5. Seal into immutable BaseRef(newGen = oldGen + 1, targetIndex, serviceBases)
 6. Construct PreparedCompaction(targetRepId, baseGeneration, targetSeq, newBaseRef)
 7. Dispatch Lineage-CAS Task to Partition Thread
 8. Compactor releases initial SnapshotImageLease
```

---

### 6.3 Partition-Thread Lineage-CAS Promotion

When the `PreparedCompaction` is submitted to the partition thread:

```java
// Executed on Partition Thread
public boolean promoteCompactedBase(PreparedCompaction prepared) {
    assert partitionChecker.isPartitionThread();

    BaseRef currentBase = groupState.currentBaseRef();

    // Lineage-CAS: Verify base generation has not diverged
    if (currentBase.baseGeneration() != prepared.expectedBaseGeneration()) {
        logger.warning("Compaction promotion rejected: base generation mismatch. Expected: "
                + prepared.expectedBaseGeneration() + ", Current: " + currentBase.baseGeneration());
        prepared.discard();
        return false;
    }

    // 1. Promote new base
    groupState.setCurrentBaseRef(prepared.newBaseRef());

    // 2. Advance active delta sequence range (Drop compacted prefix in O(1))
    long newFirstSeq = prepared.compactedThroughSequence() + 1;
    groupState.setFirstActiveSequence(newFirstSeq);

    // 3. Advance reclamation floor in DeltaSegmentStore
    deltaStore.advanceReclamationFloor(newFirstSeq);

    logger.fine("Compacted base successfully promoted. New base index: %d, active sequence range: [%d ... %d]",
            prepared.newBaseRef().baseIndex(), newFirstSeq, deltaStore.getCurrentSequence());

    return true;
}
```

---

### 6.4 Segment Store Reclamation Floor Advancement

Advancing the reclamation floor marks delta segments with `sequence < newFirstSeq` as obsolete. Obsolete segments are physically deallocated as soon as their active lease reference counts reach zero.

---

## 7. Follower State Transfer & Off-Thread Restore Engine

### 7.1 Streaming Wire Protocol & Frame Reservation

When a follower lags beyond the leader's truncated log, the leader streams a snapshot using chunked network frames:

```
[ CheckpointHeaderFrame ]
  - SnapshotRepresentationId, RaftTerm, SnapshotIndex, BaseIndex, SequenceRange [firstSeq..lastSeq]
  - Declared Total Estimated Bytes (Base + Deltas)
        |
        v (Follower verifies memory reservation before accepting data frames)
[ BaseImageFrames (1 ... M) ]
        |
        v
[ DeltaSegmentFrames (firstSeq ... lastSeq) ]
        |
        v
[ EndCheckpointFrame ]
  - End-to-end SHA-256 Digest
```

---

### 7.2 Off-Thread Materialization (`PreparedGroupState`)

The follower routes incoming frames to a `FollowerRestoreWorker` thread:

```java
public class FollowerRestoreWorker {
    public PreparedGroupState materialize(CheckpointHeaderFrame header,
                                         List<BaseImageFrame> baseFrames,
                                         List<DeltaSegmentFrame> deltaFrames) {
        // 1. Materialize base containers in parallel off-thread
        Map<Short, Object> liveContainers = materializeServiceStores(baseFrames);

        // 2. Ingest and apply delta segments in strict sequence order
        for (DeltaSegmentFrame deltaFrame : deltaFrames) {
            FrozenDeltaSegment segment = deltaFrame.segment();
            applySegmentToContainers(liveContainers, segment);
        }

        // 3. Return prepared container bundle
        return new PreparedGroupState(
            header.snapshotRepresentationId(),
            header.snapshotIndex(),
            header.snapshotTerm(),
            liveContainers
        );
    }
}
```

---

### 7.3 Zero-Downtime Root Reference Swap on Partition Thread

Once `PreparedGroupState` is ready, a single short task is executed on the partition thread:

```java
// Executed on Partition Thread - STRICT O(S) EXECUTION
public void installPreparedSnapshot(PreparedGroupState prepared) {
    assert partitionChecker.isPartitionThread();

    // 1. Revalidate Raft Term and Step-Down Status
    if (prepared.snapshotTerm() < raftState.currentTerm() || raftState.isSteppedDown()) {
        logger.warning("Aborting snapshot installation: obsolete term or node stepped down");
        prepared.discardAsync();
        return;
    }

    // 2. O(S) Pointer Swap across registered service roots
    for (Map.Entry<Short, Object> entry : prepared.serviceContainers().entrySet()) {
        short serviceId = entry.getKey();
        Object newLiveContainer = entry.getValue();
        CPServiceContainerHolder service = serviceRegistry.getService(serviceId);
        
        Object oldContainer = service.swapLiveContainer(newLiveContainer);
        scheduleAsyncContainerDisposal(oldContainer);
    }

    // 3. Reset DeltaSegmentStore and Active Buffers to clean state at snapshotIndex
    activeDeltaBuffer.reset();
    deltaStore.resetToSequence(prepared.snapshotIndex());

    // 4. Install SnapshotEntry in Raft Log & Apply Log Suffix Plan
    SnapshotEntry snapshotEntry = new SnapshotEntry(
        prepared.snapshotTerm(), prepared.snapshotIndex(),
        prepared.representationId(), raftState.memberIndex(), raftState.members()
    );
    raftLog.installSnapshotOnFollower(snapshotEntry);

    // 5. Update commit and applied index pointers
    raftState.setCommitIndex(prepared.snapshotIndex());
    raftState.setLastApplied(prepared.snapshotIndex());

    logger.info("Follower snapshot installed with zero partition pause at commit index: " + prepared.snapshotIndex());
}
```

---

### 7.4 Container Abstraction Updates for CPMap & Services

To support $O(S)$ pointer swapping, all CP services must expose root container references:

```java
public interface CPServiceContainerHolder {
    /**
     * Atomically swaps the active live storage container with the prepared container.
     * Must execute in O(1) time without iterating elements.
     */
    Object swapLiveContainer(Object newContainer);
}
```

In `CPMapService`, `CPMapRegistry` holds a reference `AtomicReference<ConcurrentMap<String, CPMapStore>>`. Swapping the live container is a single reference exchange.

---

## 8. Service-Specific Delta Capture Specifications

```
+---------------------------------------------------------------------------------------------------+
|                                SERVICE DELTA RECORDING SPECIFICATION                              |
+--------------------------+-----------------------+-------------------+----------------------------+
| Service                  | State Type            | Delta OpCode      | Compaction & Restore Mode  |
+--------------------------+-----------------------+-------------------+----------------------------+
| CPMapService (0x01)      | Key-Value Map         | PUT (0x01)        | Last-Write-Wins (LWW) by   |
|                          |                       | REMOVE (0x02)     | (RaftIndex, RaftOrdinal)   |
+--------------------------+-----------------------+-------------------+----------------------------+
| AtomicLongService (0x02) | 64-bit Primitive      | SET_VALUE (0x03)  | Direct Latest-Value        |
|                          |                       |                   | Replacement                |
+--------------------------+-----------------------+-------------------+----------------------------+
| AtomicRefService (0x03)  | Data Object Reference | SET_REF (0x04)    | Direct Latest-Value        |
|                          |                       |                   | Replacement                |
+--------------------------+-----------------------+-------------------+----------------------------+
| SessionRegistry (0x04)   | Sessions & Heartbeats | SESSION_ADD (0x05)| State Reconstruction Merge |
|                          |                       | SESSION_CLOSE(0x06)|                           |
|                          |                       | HEARTBEAT (0x07)  |                            |
+--------------------------+-----------------------+-------------------+----------------------------+
| MetadataGroup (0x05)     | Group Topologies      | SET_CONFIG (0x08) | Full State Replacement     |
+--------------------------+-----------------------+-------------------+----------------------------+
```

---

## 9. Comprehensive Memory Budgeting & Accounting

### 9.1 Multi-Pool Memory Allocation Limits

To guarantee deterministic operational stability without OOM hazards:

```
Total Process Memory Allocation Budget for CP Subsystem: M_total
├── Live State Pool:           0.40 * M_total
├── Base Image Pool:           0.25 * M_total
├── Delta Segment Store Pool:  0.15 * M_total
├── Compaction Workspace:      0.10 * M_total
├── Follower Restore Staging:  0.05 * M_total
└── Network Transfer Buffers:  0.05 * M_total
```

### 9.2 Pre-Consensus Admission Control Algorithms

```java
public class CPGroupAdmissionController {
    private final long maxDeltaBytesBudget;
    private final int maxDeltaEntriesBudget;
    private final AtomicLong currentDeltaBytes = new AtomicLong(0);

    public boolean tryAcquireAdmission(int estimatedBytes) {
        while (true) {
            long current = currentDeltaBytes.get();
            if (current + estimatedBytes > maxDeltaBytesBudget) {
                return false; // Reject at proposal stage
            }
            if (currentDeltaBytes.compareAndSet(current, current + estimatedBytes)) {
                return true;
            }
        }
    }

    public void releaseAdmission(int bytes) {
        currentDeltaBytes.addAndGet(-bytes);
    }
}
```

---

## 10. Bootstrap & Initial Group Lifecycle

### 10.1 Newly Created CP Groups
- All newly created CP groups start at Raft index `0` with an empty `BaseRef(gen=0, index=0, serviceBases=emptyMap)`.
- The first snapshot cut at index `C1` creates `FrozenDeltaSegment(seq=1, from=1, to=C1)`.
- No initial $O(N)$ scan is ever required.

### 10.2 Migrating Pre-Existing Populated Groups (Journal-Bridged Bootstrap)
For existing CP groups upgraded from older full-scan versions:
1. An asynchronous background task copies live data into initial `BaseRef(gen=1)` while mutations are recorded to `ActiveDeltaBuffer`.
2. Once the baseline copy completes, the background compactor merges the initial deltas and publishes the first `GroupSnapshotView` on the partition thread via Lineage-CAS.

---

## 11. Design Comparison & Complexity Analysis

| Metric / Dimension | Full Map Scan ($O(N)$) | Persistent Radix / Merkle Tree | Backward Delta Chain | Proposed Indexed Segment Store |
|---|---|---|---|---|
| **Snapshot Cut Pause** | $O(N)$ (100ms - 10s) | $O(1)$ | Non-constant ($O(\Delta)$ copy) | **Strict $O(1)$ ($< 10\ \mu\text{s}$)** |
| **Follower Restore Pause** | $O(N)$ | $O(1)$ page swap | $O(N + \Delta)$ on partition thread | **Strict $O(S)$ ($< 50\ \mu\text{s}$)** |
| **Compaction Safety** | N/A | High complexity | Broken (transitive references) | **Safe Lineage-CAS & Store Trimming** |
| **Write Allocation Overhead** | 0 | $K\times$ path allocations | 1 object allocation | **0 allocations (pre-allocated array)** |
| **Realistic Peak Memory** | $2.0\times$ | $1.4\times - 1.8\times$ | Unbounded if compactor lags | **$2.5\times - 3.5\times$ (strictly budgeted)** |
| **Implementation LOC** | $\approx 300$ | $\approx 6000+$ | $\approx 800$ (flawed) | **$\approx 1500$ (clean, maintainable)** |

---

## 12. Failure Modes & Fault Invariants

### 12.1 Raft Leader Step-Down During Compaction
- **Invariant**: Background compaction output is immutable and tagged with `expectedBaseGeneration`.
- **Handling**: If the leader steps down, the task finishes off-thread; when the partition task executes, the Lineage-CAS checks the Raft leadership and epoch. If the node is no longer leader, the result is discarded cleanly.

### 12.2 Follower Disconnection During Snapshot Stream
- **Invariant**: Follower holds state in `FollowerRestoreWorker` staging area.
- **Handling**: On timeout, staging containers are cleared. Leader releases `SnapshotImageLease` upon transfer termination.

### 12.3 Partition Thread Crash / Fail-Stop
- **Invariant**: If local active buffer append fails after live state mutation, node halts immediately.
- **Handling**: Prevents non-deterministic state divergence across Raft cluster members.

---

## 13. Configuration & Observability

### 13.1 XML/YAML Configuration Reference

```xml
<hazelcast>
    <cp-subsystem>
        <data-structures>
            <cp-map>
                <snapshot>
                    <!-- Maximum entries per active delta buffer before mandatory cut -->
                    <max-delta-entries>65536</max-delta-entries>
                    <!-- Maximum memory retained by delta segments before triggering urgent compaction -->
                    <max-delta-bytes>134217728</max-delta-bytes> <!-- 128 MB -->
                    <!-- Maximum delta segments retained in store before compaction -->
                    <max-delta-segments>16</max-delta-segments>
                    <!-- Compaction rate throttle in MB/sec (0 for default auto-rate) -->
                    <compaction-rate-limit-mb>64</compaction-rate-limit-mb>
                </snapshot>
            </cp-map>
        </data-structures>
    </cp-subsystem>
</hazelcast>
```

### 13.2 Dynamic Metrics Provider

Exported under `cp.snapshot`:
- `cp.snapshot.cutPauseNanos`: Partition-thread pause duration during snapshot cut.
- `cp.snapshot.restorePauseNanos`: Partition-thread pause duration during follower snapshot install.
- `cp.snapshot.storeSegmentCount`: Total delta segments currently retained in `DeltaSegmentStore`.
- `cp.snapshot.storeBytes`: Total memory held by active and pending-reclamation delta segments.
- `cp.snapshot.compactionDurationMillis`: Execution time for background base materialization.
- `cp.snapshot.admissionRejections`: Count of client mutation proposals throttled/rejected by pre-consensus admission control.

---

## 14. Verification, Simulation & Testing Plan

### 14.1 Microbenchmarks (JMH)
- **Snapshot Cut Pause**: Measure P50, P99, P99.9, and P99.99 partition-thread pause on maps with $10^5$, $10^6$, and $10^7$ entries under 50k writes/sec.
- **Follower Root Swap**: Benchmark $O(S)$ pointer swap latency on follower nodes during live client load.

### 14.2 Concurrency & Stress Tests
- **Concurrent Compaction & Snapshotting**: Rapidly take snapshots while continuous background compaction runs under saturation write traffic.
- **Lineage-CAS Invalidation**: Simulate leader step-downs and membership changes during compaction promotion; verify zero state corruption.

### 14.3 Chaos & Jepsen Verification
- **Network Partitions During Snapshot Streaming**: Inject network latency and packet loss during follower snapshot restore; verify clean cleanup and retry without memory leaks.
- **Cluster Restart & Recovery**: Verify seamless group recovery from snapshot streams and Raft logs across rolling member restarts.
