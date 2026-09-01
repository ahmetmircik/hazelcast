# In-Memory Constant-Time Snapshotting for CP Subsystem
**Design Version: 12**
**Status: Approved**

---

## 1. Overview and Core Invariants

This document specifies a comprehensive, production-grade in-memory snapshotting subsystem for the Hazelcast CP Subsystem (Raft state machine). The design achieves bounded $O(S)$ partition-thread snapshot cuts and zero-pause follower restorations using an **Immutable Base + Indexed Delta-Segment Store** model.

### Core Invariants:
1. **Bounded $O(S)$ Two-Phase Snapshot Cut & Monotonic Sealed Publication**:
   - *Phase 1 (Partition-Thread Delta Cut)*: At commit index $C$, the partition thread executes bounded $O(S)$ operations ($S \le 10$ services): detaches `ActiveDeltaBuffer`, swaps in a pre-reserved clean buffer, appends `FrozenDeltaSegment(C)` to `DeltaSegmentStore`, captures an $O(1)$ non-resurrecting `LedgerRangeLease(snapshotFloor, lastAdmittedSeq, gen)`, and enqueues an `UNSEALED` candidate representation into a bounded sealing queue (1 in-flight + 1 latest coalescing pending candidate). The Raft logical head is **not** advanced, and the predecessor sealed representation is **not** retired.
   - *Phase 2 (Off-Thread Sealing & Monotonic Atomic Publication)*: `SnapshotSealerWorker` extracts the immutable `unreleasedReservationsAtCut` list from the leased ledger ring in ascending sequence order, computes frame hashes and `representationDigest` off-thread, and releases the ledger lease. An $O(1)$ partition-thread task publishes any sealed candidate whose `snapshotIndex > lastSnapshotIndex`, atomically installing `latestSealedSnapshotRepresentation = sealedCandidate`, advancing the Raft logical head (`raftLog.setSnapshotLogical(snapshotEntry, C)`), and retiring the predecessor. If an equal or newer candidate was already published, the older candidate is discarded. If sealing fails, the candidate is discarded with zero impact on the active sealed snapshot or un-truncated Raft log.
2. **Pre-Allocated Delta Buffer with Encapsulated Immutable Entries**: `ActiveDeltaBuffer` stores immutable `DeltaEntry` references in a pre-allocated fixed-capacity array. Entry allocations are charged upfront during proposal admission. `FrozenDeltaSegment` provides encapsulated read-only access, ensuring zero array modification after detach.
3. **Ring Slot Reservation & Lease-Safe Sequence Validation**: `DeltaSegmentStore` operates on a fixed-capacity pre-allocated circular ring buffer ($R = 1024$ slots). Admission verifies available unreserved slots before admitting mutations. Segment append verifies that the target slot is unoccupied or unleased below `reclamationFloor`. Range acquisition validates every individual slot sequence under an epoch lease.
4. **Decoupled Asynchronous Sealing Protocol with Queue Coalescing**: Sealing queue holds at most 1 in-flight and 1 pending candidate. If a newer cut arrives while a pending candidate awaits sealing, the pending slot is coalesced/replaced by the newer cut, releasing unsealed resources. Streaming transfers to followers are admitted exclusively on `SEALED` representations. Partition thread executes zero frame hashing or checksum-list traversals.
5. **Compaction Promotion with Exact Published Representation & Tail CAS**: Compaction materializes a new base image and active suffix deltas into an independent physical `SnapshotRepresentation`, fully sealed off-thread. Promotion on the partition thread is an $O(1)$ Lineage-CAS validating exact equality on `(groupIncarnation, expectedPublishedRepId, expectedBaseGen, expectedFirstSeq, targetSeq, targetIndex, targetTerm, currentTailSeqAtCapture)`. If a newer snapshot cut advanced the tail, the compaction is safely discarded and retried without index regression.
6. **Follower Restore with Dual Representation (Transfer Stream + Local Lineage Base)**: Follower restore streams are assembled and validated off-thread into `PreparedGroupState` containing:
   - `PreparedLiveRoots`: Folded live service storage containers.
   - `ReceivedSealedSnapshotRepresentation`: Verified received physical representation (retaining base and delta frames under the received `representationDigest`) installed as the operational sealed snapshot at index $C$ for immediate network transfer serving.
   - `FoldedLocalBaseRef(C)`: Deeply immutable folded base installed as `currentBaseRef` alongside a fresh `DeltaSegmentStore` starting at sequence 1 (`activeBufferFromIndex = C + 1`) for local snapshot cuts and compaction.
   - Exact duplicate fast path checks `commitIndex >= C && lastApplied >= C && constantTimeEquals(installedDigest, receivedDigest)` and verifies operational handle acquisition via try-with-resources.
7. **Immutable Leased Proposal Ledger & Exact Election Reconstruction**:
   - The proposer records immutable `ReservationMetadata(reservationSequence, worstCaseEntries, worstCaseBytes)` in a pre-allocated circular ledger ring (capacity 256).
   - Phase 1 snapshot cut captures an $O(1)$ range lease without array copies; the sealer extracts ascending, unique reservations into `CanonicalSnapshotHeader` with constructor-enforced bounds (`size <= P_max`, `entries <= E_max`, `bytes <= B_max`).
   - Upon leadership election, the leader blocks new proposals, initializes `creditFloor = Math.max(snapshotFloor, minQuorumReportedAppliedSeq)`, and reconstructs active charges for all sequences $> \text{creditFloor}$ across snapshot ledger state and all retained log entries from $\text{publishedSnapshotIndex} + 1$ to $\text{lastLogIndex}$ (deduplicating by sequence). Overflow-safe arithmetic (`Math.addExact`, `Math.multiplyExact`) is enforced throughout.
8. **Bounded Streaming Codec Contract & Workspace Accounting**: `CPServiceSnapshotCodec` streams chunk frames (max 64 KB) via `writeBase(B, FrameSink)` and `readBase(FrameSource, declaredBytes, Workspace)` without allocating monolithic whole-base byte arrays. Codecs declare restore-builder workspace limits charged to $M_{\text{restore\_staging}}$, with idempotent cancellation cleanup.
9. **Defensive Canonical Snapshot Encoding & Truly Immutable Hash256**: `Hash256` is a truly immutable final value class with defensive array cloning on all ingress/egress paths and constant-time equality. `CanonicalSnapshotHeader` encodes complete endpoint UUIDs, bounded string lengths, unreleased reservation lists, and sorted codec mappings in version 1 binary format (`HZ_SNAP1`).
10. **Epoch-Based Segmented Raft Log Detachment**: `RaftLog` uses an immutable segment table view with an atomic generation epoch. Readers acquire non-resurrecting leases on the view. Log prefix detachment publishes a new segment view and retires the old epoch in $O(1)$ without partition-thread list manipulation. Detached physical log segments are deallocated off-thread after epoch reader quiescence.

---

## 2. Architecture & Data Structures

```
                                  GROUP COORDINATOR ARCHITECTURE
+----------------------------------------------------------------------------------------------------+
|                                         PARTITION THREAD                                           |
|                                                                                                    |
|  Writes ---> [ Admission Quota Check ] ---> Apply to Live Store ---> [ Fixed-Capacity Delta Buffer ] |
|                                                                                 |                  |
|                                                (Phase 1 Delta Cut at Index C)   | O(1) Detach &    |
|                                                                                 | Swap Pre-Reserved|
|                                                                                 v                  |
|                                                                   [ Frozen Delta Segment C ]       |
+---------------------------------------------------------------------------------|------------------+
                                                                                  |
                                                                                  v (O(1) Ring Insert)
+----------------------------------------------------------------------------------------------------+
|                    DELTA-SEGMENT RING DIRECTORY (Per Group, Pre-Allocated)                         |
|                                                                                                    |
|   Ring Slot:         [101 & mask]   [102 & mask]   [103 & mask]   [104 & mask]   [105 & mask]      |
|   Index Range:       (C0+1 .. C1)   (C1+1 .. C2)   (C2+1 .. C3)   (C3+1 .. C4)   (C4+1 .. C5)      |
|   Reclamation Floor = 101                                                            ^             |
|                                                                                      |             |
|   +-------------------+                                                              |             |
|   |   BaseImage(B0)   | + DeltaSequenceRange [ 101 ... 105 ] ------------------------+             |
|   |  (Gen=1, Idx=C0)  |                                                                            |
|   +-------------------+                                                                            |
+-------------|--------------------------------------------------------------------------------------+
              |
              | (Bounded Queue: 1 in-flight + 1 latest coalescing pending candidate)
              v
+-------------------------------------------------------------------+
|               SNAPSHOT SEALER WORKER (Serial Off-Thread)          |
|                                                                   |
| 1. Read Leased Ledger Range -> Emit Sorted unreleasedReservations |
| 2. Compute 64 KB Chunk Frame Hashes + HZ_SNAP1 RepresentationDigest|
| 3. Release Ledger Range Lease; Mark Candidate State: SEALED       |
| 4. Dispatch Phase 2 Monotonic Publication Task to Partition Thread|
|    - Partition thread checks: candidate.index > lastSnapshotIndex |
|    - Installs latestSealedSnapshotRepresentation = sealedCandidate|
|    - Executes raftLog.setSnapshotLogical(snapshotEntry, C) (O(1)) |
|    - Retires predecessor sealed representation (O(1))             |
+-------------------------------------------------------------------+
```

### 2.1 Core Types & Canonical Structures

```java
package com.hazelcast.cp.internal.datastructures.snapshot.v2;

/**
 * Truly immutable 32-byte SHA-256 hash value class.
 */
public final class Hash256 {
    private final byte[] bytes;

    public Hash256(byte[] bytes) {
        if (bytes == null || bytes.length != 32) {
            throw new IllegalArgumentException("Hash256 requires exactly 32 bytes");
        }
        this.bytes = bytes.clone();
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public boolean constantTimeEquals(Hash256 other) {
        if (other == null) return false;
        return MessageDigest.isEqual(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Hash256 other) && constantTimeEquals(other);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return Hex.encodeHexString(bytes);
    }
}

/**
 * Representation lifecycle state.
 */
public enum RepresentationState {
    UNSEALED,
    SEALED,
    SEAL_FAILED,
    RETIRED
}

/**
 * Globally unique physical representation identifier.
 */
public record SnapshotRepresentationId(
    CPGroupId groupId,
    long groupIncarnation,
    long representationGen,
    long snapshotIndex
) {}

/**
 * Replicated reservation metadata carried by proposals and ledger state.
 */
public record ReservationMetadata(
    long reservationSequence,
    int worstCaseEntries,
    int worstCaseBytes
) {}

/**
 * Range lease captured in O(1) at Phase 1 snapshot cut.
 */
public record LedgerRangeLease(
    long snapshotFloor,
    long lastAdmittedSequence,
    long leaseGeneration
) {}

/**
 * Canonical deterministic snapshot header with defensive copy constructor.
 */
public record CanonicalSnapshotHeader(
    CPGroupId groupId,
    long groupIncarnation,
    long snapshotIndex,
    int snapshotTerm,
    long membershipLogIndex,
    List<RaftEndpoint> orderedMembers,             // Canonically sorted member list
    Map<Short, Short> codecDirectory,              // Canonically sorted serviceId -> codecVersion map
    long firstDeltaSequence,
    long lastDeltaSequence,
    long declaredDataBytes,
    long lastQuorumAppliedReservationSeq,          // Quorum credit floor at snapshot cut
    List<ReservationMetadata> unreleasedReservationsAtCut, // Unreleased reservations summary (<= P_max)
    List<Hash256> framePayloadChecksums            // Immutable SHA-256 chunk hashes
) {
    public CanonicalSnapshotHeader {
        if (orderedMembers.size() > 65535) throw new IllegalArgumentException("Member count exceeds 65535");
        if (codecDirectory.size() > 65535) throw new IllegalArgumentException("Codec count exceeds 65535");
        if (unreleasedReservationsAtCut.size() > 64) {
            throw new IllegalArgumentException("Unreleased reservations exceed P_max (64)");
        }

        // Validate strictly ascending sequences, uniqueness, and charge bounds
        long prevSeq = -1;
        for (ReservationMetadata r : unreleasedReservationsAtCut) {
            if (r.reservationSequence() <= prevSeq) {
                throw new IllegalArgumentException("Reservation sequences must be strictly ascending and unique");
            }
            if (r.worstCaseEntries() <= 0 || r.worstCaseEntries() > 256) {
                throw new IllegalArgumentException("worstCaseEntries out of bounds: " + r.worstCaseEntries());
            }
            if (r.worstCaseBytes() <= 0 || r.worstCaseBytes() > 4096) {
                throw new IllegalArgumentException("worstCaseBytes out of bounds: " + r.worstCaseBytes());
            }
            prevSeq = r.reservationSequence();
        }

        // Enforce authoritative member ordering: (uuid, host, port)
        var sortedMembers = new ArrayList<>(orderedMembers);
        sortedMembers.sort(Comparator.comparing(RaftEndpoint::getUuid)
            .thenComparing(RaftEndpoint::getHost)
            .thenComparingInt(RaftEndpoint::getPort));
        orderedMembers = Collections.unmodifiableList(sortedMembers);

        // Enforce sorted codec directory
        codecDirectory = Collections.unmodifiableSortedMap(new TreeMap<>(codecDirectory));
        unreleasedReservationsAtCut = List.copyOf(unreleasedReservationsAtCut);
        framePayloadChecksums = List.copyOf(framePayloadChecksums);
    }

    /**
     * Version 1 Canonical Binary Serialization (HZ_SNAP1 domain separator + big-endian fields).
     */
    public Hash256 computeRepresentationDigest() {
        MessageDigest digest = DigestUtils.getSha256();
        ByteBuffer buf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN);

        digest.update(buf.putLong(0, 0x485A5F534E415031L).array(), 0, 8); // Magic "HZ_SNAP1"
        
        // Canonical CPGroupId encoding: (seed: long, id: long, nameLength: short, nameUtf8: byte[])
        buf.clear();
        buf.putLong(groupId.getSeed());
        digest.update(buf.array(), 0, 8);
        buf.clear();
        buf.putLong(groupId.getId());
        digest.update(buf.array(), 0, 8);
        byte[] groupBytes = groupId.getGroupName().getBytes(StandardCharsets.UTF_8);
        if (groupBytes.length > 256) throw new IllegalArgumentException("Group name exceeds 256 bytes");
        buf.clear();
        buf.putShort((short) groupBytes.length);
        digest.update(buf.array(), 0, 2);
        digest.update(groupBytes);

        // Header numeric fields
        buf.clear();
        buf.putLong(groupIncarnation);
        digest.update(buf.array(), 0, 8);
        buf.clear();
        buf.putLong(snapshotIndex);
        digest.update(buf.array(), 0, 8);
        buf.clear();
        buf.putInt(snapshotTerm);
        digest.update(buf.array(), 0, 4);
        buf.clear();
        buf.putLong(membershipLogIndex);
        digest.update(buf.array(), 0, 8);

        // Ordered members with full UUID encoding
        buf.clear();
        buf.putInt(orderedMembers.size());
        digest.update(buf.array(), 0, 4);
        for (RaftEndpoint m : orderedMembers) {
            UUID uuid = m.getUuid();
            buf.clear();
            buf.putLong(uuid.getMostSignificantBits());
            buf.putLong(uuid.getLeastSignificantBits());
            digest.update(buf.array(), 0, 16);

            byte[] hostBytes = m.getHost().getBytes(StandardCharsets.UTF_8);
            if (hostBytes.length > 256) throw new IllegalArgumentException("Host exceeds 256 bytes");
            buf.clear();
            buf.putShort((short) hostBytes.length);
            digest.update(buf.array(), 0, 2);
            digest.update(hostBytes);
            buf.clear();
            buf.putInt(m.getPort());
            digest.update(buf.array(), 0, 4);
        }

        // Ordered codecs
        buf.clear();
        buf.putShort((short) codecDirectory.size());
        digest.update(buf.array(), 0, 2);
        for (var entry : codecDirectory.entrySet()) {
            buf.clear();
            buf.putShort(entry.getKey());
            buf.putShort(entry.getValue());
            digest.update(buf.array(), 0, 4);
        }

        // Sequences, bytes & quorum applied reservation floor
        buf.clear();
        buf.putLong(firstDeltaSequence);
        digest.update(buf.array(), 0, 8);
        buf.clear();
        buf.putLong(lastDeltaSequence);
        digest.update(buf.array(), 0, 8);
        buf.clear();
        buf.putLong(declaredDataBytes);
        digest.update(buf.array(), 0, 8);
        buf.clear();
        buf.putLong(lastQuorumAppliedReservationSeq);
        digest.update(buf.array(), 0, 8);

        // Unreleased reservations summary at cut
        buf.clear();
        buf.putShort((short) unreleasedReservationsAtCut.size());
        digest.update(buf.array(), 0, 2);
        for (ReservationMetadata res : unreleasedReservationsAtCut) {
            buf.clear();
            buf.putLong(res.reservationSequence());
            digest.update(buf.array(), 0, 8);
            buf.clear();
            buf.putInt(res.worstCaseEntries());
            buf.putInt(res.worstCaseBytes());
            digest.update(buf.array(), 0, 8);
        }

        // Ordered frame payload checksums
        buf.clear();
        buf.putInt(framePayloadChecksums.size());
        digest.update(buf.array(), 0, 4);
        for (Hash256 hash : framePayloadChecksums) {
            digest.update(hash.bytes());
        }

        return new Hash256(digest.digest());
    }
}

/**
 * Coordinated group snapshot metadata.
 */
public record GroupSnapshotMetadata(
    SnapshotRepresentationId representationId,
    CanonicalSnapshotHeader canonicalHeader,
    long localWallTimeNanos,
    Hash256 representationDigest
) {}

/**
 * Canonical immutable delta entry.
 */
public record DeltaEntry(
    short serviceId,
    Data objectId,           // Stable serialized object identifier (e.g. CPMap name)
    long objectGeneration,   // Object creation incarnation counter to prevent ID reuse
    byte opCode,             // 0x01 = PUT, 0x02 = REMOVE, 0x03 = SERVICE_STATE
    Data key,
    Data value,
    long raftIndex,          // Raft commit log index
    int raftOrdinal          // Sub-operation order within the Raft log entry
) {
    public long estimatedSize() {
        return 36 + (objectId != null ? objectId.totalSize() : 0)
                  + (key != null ? key.totalSize() : 0)
                  + (value != null ? value.totalSize() : 0);
    }
}

/**
 * Immutable frozen delta segment with encapsulated entry access.
 */
public final class FrozenDeltaSegment {
    private final long sequence;
    private final long fromRaftIndex;
    private final long toRaftIndex;
    private final int snapshotTerm;
    private final int entryCount;
    private final long byteSize;
    private final DeltaEntry[] entries; // Private encapsulated array

    public FrozenDeltaSegment(long sequence, long fromRaftIndex, long toRaftIndex,
                              int snapshotTerm, int entryCount, long byteSize, DeltaEntry[] entries) {
        this.sequence = sequence;
        this.fromRaftIndex = fromRaftIndex;
        this.toRaftIndex = toRaftIndex;
        this.snapshotTerm = snapshotTerm;
        this.entryCount = entryCount;
        this.byteSize = byteSize;
        this.entries = entries; // Owned exclusively after detach
    }

    public long sequence() { return sequence; }
    public long fromRaftIndex() { return fromRaftIndex; }
    public long toRaftIndex() { return toRaftIndex; }
    public int snapshotTerm() { return snapshotTerm; }
    public int entryCount() { return entryCount; }
    public long byteSize() { return byteSize; }
    public DeltaEntry getEntry(int index) { return entries[index]; }
    public Iterator<DeltaEntry> iterator() { return Iterators.forArray(entries, 0, entryCount); }
}

/**
 * Typed service base container reference.
 */
public record ServiceBaseRef(
    short serviceId,
    short codecVersion,
    Object immutableServicePayload,
    long estimatedBytes
) {}

/**
 * Immutable base image reference.
 */
public record BaseRef(
    long baseGeneration,
    long baseIndex,
    int baseTerm,
    long totalEstimatedBytes,
    Map<Short, ServiceBaseRef> serviceBases
) {}

/**
 * Bounded streaming frame sink for snapshot serialization.
 */
public interface SnapshotFrameSink {
    void writeChunk(byte[] buffer, int offset, int length) throws IOException;
    long totalBytesWritten();
}

/**
 * Bounded streaming frame source for snapshot deserialization.
 */
public interface SnapshotFrameSource {
    int readChunk(byte[] targetBuffer, int offset, int maxLength) throws IOException;
    boolean hasMoreChunks();
}

/**
 * Service builder workspace handle for bounded staging allocations.
 */
public interface ServiceWorkspace extends AutoCloseable {
    long allocatedBytes();
    void trackAllocation(long bytes);
    @Override
    void close(); // Idempotent cleanup of partial builder structures
}

/**
 * Bounded streaming snapshot codec contract implemented by CP services.
 */
public interface CPServiceSnapshotCodec<B, D> {
    short serviceId();
    short codecVersion();
    double conservativeWorkspaceMultiplier(); // e.g. 1.5x
    long maxRestoreWorkspaceBytes(long declaredBytes);
    B createEmptyBase();
    B materializeBase(B existingBase, Iterator<D> deltas); // Deeply immutable: MUST NOT mutate existingBase
    void applyDeltaToLive(Object liveContainer, D delta);
    Object swapLiveContainer(Object currentContainer, Object newContainer);
    void writeBase(B base, SnapshotFrameSink sink) throws IOException;
    B readBase(SnapshotFrameSource source, long declaredBytes, ServiceWorkspace workspace) throws IOException;
}
```

---

## 3. Partition-Thread Execution Engine

### 3.1 Pre-Consensus Admission Control & Quorum Progress Ledger

To guarantee cluster-wide consensus liveness without violating Raft determinism:
1. **Anchored Quorum Proposal Ledger & Immutable Ring Storage**:
   - Each Raft proposal replicates its `ReservationMetadata(reservationSequence, worstCaseEntries, worstCaseBytes)`.
   - Enforced maximum in-flight proposals: $P_{\text{max}} = 64$.
   - Validated entries-per-proposal bound: $E_{\text{max}} = 256$.
   - Validated bytes-per-proposal bound: $B_{\text{max}} = 4096$ bytes.
   - Active reservations are stored in a pre-allocated circular ring buffer `AtomicReferenceArray<ReservationMetadata> reservationRing = new AtomicReferenceArray<>(256)`.
2. **Protected Voter Headroom Invariant ($H_{\text{voter}}$)**:
   $$H_{\text{voter\_entries}} = \text{Math.multiplyExact}(P_{\text{max}}, E_{\text{max}}) = 64 \times 256 = 16384\text{ entries}$$
   $$H_{\text{voter\_bytes}} = \text{Math.multiplyExact}(P_{\text{max}}, B_{\text{max}}) = 64 \times 4096 = 256\text{ KB}$$
   Every voter reserves $H_{\text{voter}}$ capacity exclusively for executing committed Raft log entries. Background compaction, restore staging, and reclamation cannot consume any portion of $H_{\text{voter}}$.
3. **Validated Configuration Invariants**:
   $$\text{maxDeltaEntries} \ge \text{Math.addExact}(H_{\text{voter\_entries}}, \text{minimumOperationalCapacity}) \quad (65536 \ge 16384 + 16384)$$
   $$\text{maxDeltaBytesBudget} \ge \text{Math.addExact}(H_{\text{voter\_bytes}}, \text{minimumOperationalBytes}) \quad (128\text{ MB} \ge 256\text{ KB} + 32\text{ MB})$$
4. **Leader Admission Gate**: Proposing leaders verify capacity against the outstanding ledger:
   $$\text{Math.addExact}(\text{Math.addExact}(\text{currentBufferEntries}, \text{ledgerInFlightEntries}), E_{\text{proposed}}) \le \text{maxDeltaEntries} - H_{\text{voter\_entries}}$$
   $$\text{Math.addExact}(\text{Math.addExact}(\text{allocatedDeltaBytes}, \text{ledgerInFlightBytes}), B_{\text{proposed}}) \le \text{maxDeltaBytesBudget} - H_{\text{voter\_bytes}}$$
   $$\text{spareBufferReserve} \neq \text{null}$$
   $$\text{ringDirectoryAvailableSlots} > 32$$
   If limits are exceeded or the sealing queue is full, client proposals are throttled or rejected with `OverloadRetryException`.
5. **Ledger Quorum Progress Release**: A proposal's reservation charge is retired from the ledger only when a quorum of voters acknowledges `lastAppliedReservationSeq >= seq`. Followers lagging beyond $L_{\text{max}} = 512$ are evicted from the active credit calculation and catch up via sealed snapshot.
6. **Leadership Change Reconstruction Protocol**: Upon election as leader:
   - Proposal admission is blocked.
   - Collect reported `lastAppliedReservationSeq` from the election heartbeat quorum; initialize `creditFloor = Math.max(latestSealedSnapshotRepresentation.canonicalHeader().lastQuorumAppliedReservationSeq(), minQuorumReportedAppliedSeq)`.
   - Restore outstanding reservations:
     - For each `ReservationMetadata` in `latestSealedSnapshotRepresentation.canonicalHeader().unreleasedReservationsAtCut()`: if `reservationSequence > creditFloor`, insert into the active ledger and charge $(E, B)$.
     - For every retained log entry $i \in (\text{publishedSnapshotIndex} \dots \text{lastLogIndex}]$ (regardless of commit status): extract `entry.reservationMetadata()`. If `reservationSequence > creditFloor` and not already restored from snapshot summary, insert into active ledger, deduplicating by `reservationSequence`.
   - Once reconstruction completes, proposal admission is enabled.
7. **Unconditional Committed Application**: Once committed by Raft, entries are applied to the live store and appended to `ActiveDeltaBuffer` unconditionally. If an unexpected invariant violation occurs post-commit, the member executes an immediate **fail-stop**.

### 3.2 Fixed-Capacity Active Delta Buffer & Ring Store

1. **Active Delta Buffer**:
   - Operates on a pre-allocated `DeltaEntry[]` array.
   - `append(DeltaEntry entry)`: Stores immutable reference at `entries[size++]` ($O(1)$).
   - `detach(...)`: Hands off pre-sized array ownership to `FrozenDeltaSegment` without copying ($O(1)$).
   - Replenishment: Clean replacement buffers are pre-reserved asynchronously prior to reaching the snapshot cut index. If `spareBufferReserve == null`, admission pauses incoming proposals until allocation completes.
2. **Delta-Segment Ring Directory Store (`DeltaSegmentStore`)**:
   - Implemented as a fixed-capacity pre-allocated circular ring buffer ($R = 1024$ slots):
     `AtomicReferenceArray<FrozenDeltaSegment> ring = new AtomicReferenceArray<>(1024)`.
   - `appendSegment(FrozenDeltaSegment seg)`:
     Verifies `ring.get((int)(seg.sequence() & 1023)) == null || ring.get((int)(seg.sequence() & 1023)).sequence() < reclamationFloor.get()`. If occupied by an active sequence, append throws deterministic invariant exception triggering fail-stop. Otherwise, sets `ring.lazySet((int)(seg.sequence() & 1023), seg)` ($O(1)$).
   - `acquireRange(firstSeq, lastSeq)`: Acquires range under an epoch lease and validates for every $s \in [\text{firstSeq} \dots \text{lastSeq}]$ that `ring.get((int)(s & 1023)) != null && ring.get((int)(s & 1023)).sequence() == s`.
   - `advanceReclamationFloor(newFloor)`: Sets `reclamationFloor = newFloor` in $O(1)$. Background worker cleans `ring.lazySet(oldSeq & 1023, null)` once active reader leases drop to zero.

### 3.3 Two-Phase Coordinated Snapshot Cut & Monotonic Publication Protocol

#### Phase 1: Partition-Thread Snapshot Cut (Commit Index $C$)
1. Verify `commitIndex == lastApplied == C`.
2. Verify `spareBufferReserve != null` (pre-reserved clean replacement buffer).
3. Read `activeBufferFromIndex` (start index of current active buffer).
4. Detach active delta buffer into `FrozenDeltaSegment(seq = nextSeq++, from = activeBufferFromIndex, to = C, term = raftLog.termAt(C))` ($O(1)$).
5. Swap active buffer: `activeDeltaBuffer = spareBufferReserve; spareBufferReserve = null` ($O(1)$).
6. Update `activeBufferFromIndex = C + 1`.
7. Insert `FrozenDeltaSegment` into `DeltaSegmentStore` ring buffer ($O(1)$).
8. Capture non-resurrecting range lease on the reservation ledger ring:
   `LedgerRangeLease(lastQuorumAppliedReservationSeq, currentAdmittedReservationSeq, nextLedgerLeaseGen++)` in $O(1)$ zero-iteration, zero-allocation.
9. Formulate `SnapshotCandidate` (`UNSEALED` status):
   - Compute `SnapshotRepresentationId(groupId, incarnation, nextRepGen++, C)`.
   - Bind `BaseRef`, active sequence range `[firstSeq ... nextSeq - 1]`, incremental byte counters, and the captured `LedgerRangeLease`.
   - Construct candidate `SnapshotEntry`.
10. Enqueue candidate representation into bounded Sealer Queue (holds 1 in-flight + 1 latest pending slot with coalescing). If a pending slot exists, it is replaced by this newest candidate, releasing the unsealed lease of the replaced candidate immediately.
11. Trigger async allocation of next `spareBufferReserve`.
12. Check compaction triggers.
*(Note: The Raft logical head is NOT advanced, and the predecessor sealed representation is NOT retired in Phase 1.)*

#### Phase 2: Asynchronous Sealing & Monotonic Atomic Publication
1. `SnapshotSealerWorker` reads the leased range from `reservationRing`, extracting unique `ReservationMetadata` records strictly in ascending sequence order to form `unreleasedReservationsAtCut`.
2. Releases the `LedgerRangeLease`.
3. Streams base and delta frames through 64 KB chunk hashers off-thread, constructing `List<Hash256> framePayloadChecksums`.
4. Constructs immutable `CanonicalSnapshotHeader` and calculates `representationDigest` via version 1 canonical binary format (`HZ_SNAP1`).
5. Atomically transitions candidate representation state to `SEALED`.
6. Dispatches an $O(1)$ publication task to the Partition Thread.
7. Partition Thread executes monotonic publication:
   - Verifies monotonic progression: `candidate.snapshotIndex() > lastSnapshotIndex`. If an equal or newer candidate was already published (`candidate.snapshotIndex() <= lastSnapshotIndex`), the older candidate is discarded cleanly.
   - Installs `latestSealedSnapshotRepresentation = sealedCandidate`.
   - Executes `raftLog.setSnapshotLogical(snapshotEntry, candidate.snapshotIndex())` advancing the logical log head in $O(1)$.
   - Retires the predecessor sealed representation in $O(1)$.
   - Updates `lastSnapshotIndex = candidate.snapshotIndex()`.
8. If sealing encounters an error: candidate is marked `SEAL_FAILED` and discarded. The previous sealed snapshot and un-truncated Raft log remain completely active and unaffected!

---

## 4. Indexed Delta-Segment Store & Compaction

### 4.1 Compaction Lifecycle & Lineage-CAS Promotion

Compaction is triggered when $\text{rangeDepth} \ge \text{maxDeltaSegments}$ or $\text{rangeBytes} \ge \text{maxDeltaBytes}$.

```
                  BACKGROUND COMPACTION LIFECYCLE

 1. Compactor verifies upfront workspace reservation quota:
    M_compaction_reserve = sum(W_service * BaseSize_service).
 2. Compactor acquires SnapshotHandle on current latestSealedSnapshotRepresentation:
    - Captures: (groupIncarnation, expectedPublishedRepId, expectedBaseGen = G,
                 expectedFirstSeq, targetSeq, targetIndex, targetTerm, currentTailSeqAtCapture)
 3. Off-thread worker materializes new BaseRef(gen = G + 1, index = targetIndex, term = targetTerm):
    - For CPMap (0x01): Groups by (serviceId, objectId, key) and collapses via
      Last-Write-Wins (LWW) ordered strictly by (raftIndex, raftOrdinal).
    - For Scalar CP Services: Applies latest state value.
    - Predecessor BaseRef is NEVER mutated in-place.
 4. Constructs NEW physical SnapshotRepresentation:
    - Sets BaseRef = newBaseRef, delta range = [targetSeq + 1 ... currentTailSeqAtCapture].
    - Seals new representation off-thread (calculates frame hashes and representationDigest).
 5. Constructs PreparedCompaction tuple containing the ready, sealed representation and captured state.
 6. Submits Lineage-CAS task to Partition Thread.
 7. Partition Thread executes Lineage-CAS:
    - Verifies exact equality:
      currentIncarnation == prep.groupIncarnation &&
      latestSealedSnapshotRepresentation.id().equals(prep.expectedPublishedRepId) &&
      currentBaseRef.baseGeneration == prep.expectedBaseGen &&
      currentFirstSeq == prep.expectedFirstSeq &&
      deltaStore.getSegment(prep.targetSeq).toRaftIndex() == prep.targetIndex &&
      deltaStore.getSegment(prep.targetSeq).snapshotTerm() == prep.targetTerm &&
      deltaStore.currentTailSequence() == prep.currentTailSeqAtCapture
    - If Match:
      a. Installs new sealed SnapshotRepresentation.
      b. Advances active sequence range: firstActiveSequence = prep.targetSeq + 1.
      c. Advances store reclamation floor to prep.targetSeq + 1.
      d. Retires old SnapshotRepresentation.
    - If Mismatch (newer cut published or tail advanced): Discards PreparedCompaction and marks candidate RETIRED.
 8. Compactor releases input SnapshotHandle in finally block.
```

---

## 5. Follower Snapshot Restore Engine

### 5.1 Streaming Protocol & Frame Header

```
[ CheckpointHeaderFrame ]
  - groupId, groupIncarnation, leaderEndpoint, leaderTerm, transferId
  - snapshotIndex, snapshotTerm, membershipLogIndex, orderedMembers
  - codecDirectory, declaredDataBytes, lastQuorumAppliedReservationSeq
  - unreleasedReservationsAtCut, frameChecksums, representationDigest
        |
        v (Follower validates codec compatibility & reserves memory budget)
[ BaseImageFrames (1 ... M, 64 KB chunks) ]
        |
        v
[ DeltaSegmentFrames (firstSeq ... lastSeq, 64 KB chunks) ]
        |
        v
[ EndCheckpointFrame (End-to-End Representation Digest Verification) ]
```

### 5.2 Off-Thread Follower Materialization (`PreparedGroupState`)

`FollowerRestoreWorker` executes state construction completely off-thread:
1. **Codec Validation**: Validates complete `codecDirectory` against locally registered codec versions. If any service is missing or version is incompatible, follower replies with `InstallSnapshotResponse(REJECTED_UNSUPPORTED_CODEC)` without allocating staging memory.
2. **Workspace & Digest Verification**: Streams 64 KB chunks through `readBase(source, declaredBytes, workspace)` and delta replay under declared workspace bounds, verifying chunk hashes and canonical `representationDigest`. Partial failures invoke `workspace.close()` for idempotent cleanup.
3. **Dual State Construction**:
   - Materializes `PreparedLiveRoots`: Live service storage containers reconstructed from base and deltas.
   - Materializes `FoldedLocalBaseRef(C)`: Deeply immutable folded base image for local state and subsequent compactions.
   - Retains the verified received physical `SnapshotRepresentation` (storing received base and delta frames under the received `representationDigest`) as the operational sealed snapshot at index $C$ for immediate network transfer serving.
   - Prepares clean replacement `ActiveDeltaBuffer` and a clean `DeltaSegmentStore` starting at sequence 1 with `activeBufferFromIndex = snapshotIndex + 1`.
4. Emits `PreparedGroupState(transferId, incarnation, leaderEndpoint, leaderTerm, snapshotIndex, snapshotTerm, membership, membershipLogIndex, liveRoots, receivedSealedSnapshotRep, foldedLocalBaseRef, cleanActiveBuffer, cleanDeltaStore, representationDigest)`.

### 5.3 Partition-Thread Switchover ($O(S)$ Execution)

When `PreparedGroupState` is ready:
1. **Freshness & Fencing Validation**:
   - Verify `currentTerm == leaderTerm`, `nodeStatus == ACTIVE`, `prepared.groupIncarnation == currentGroupIncarnation`, `leaderEndpoint == currentLeader`, and `transferId == activeSessionTransferId`.
   - Stale snapshot check: If `prepared.snapshotIndex < commitIndex`, check exact duplicate predicate. If not duplicate, reject with `InstallSnapshotResponse(STALE_INDEX)`.
   - Exact duplicate check:
     ```java
     if (commitIndex >= prepared.snapshotIndex() && lastApplied >= prepared.snapshotIndex()
             && currentSnapshotMetadata.representationDigest().constantTimeEquals(prepared.representationDigest())) {
         try (SnapshotHandle handle = latestSealedSnapshotRepresentation.tryAcquireHandle()) {
             if (handle != null) {
                 return acknowledgeSuccessImmediately();
             }
         }
     }
     ```
   - Advance `taskCancellationGeneration++` to invalidate concurrent in-flight compaction or snapshot cut tasks.
2. **Partition-Thread Suffix Retention Plan**:
   - If local log entry at `prepared.snapshotIndex` exists and its term matches `prepared.snapshotTerm`: Retain local log entries $> \text{snapshotIndex}$.
   - Otherwise: Discard all local log entries $> \text{snapshotIndex}$.
3. **$O(S)$ Container Root Pointer Swap**:
   ```java
   for (var entry : prepared.liveRoots().entrySet()) {
       CPServiceSnapshotCodec codec = codecRegistry.get(entry.getKey());
       Object oldContainer = codec.swapLiveContainer(liveContainers.get(entry.getKey()), entry.getValue());
       scheduleAsyncContainerDisposal(oldContainer);
   }
   ```
4. **Atomic Pointer Swaps (Zero-Array Clearing)**:
   - `activeDeltaBuffer = prepared.cleanActiveBuffer()` ($O(1)$).
   - `deltaSegmentStore = prepared.cleanDeltaStore()` ($O(1)$).
   - `currentBaseRef = prepared.foldedLocalBaseRef()` ($O(1)$).
   - `latestSealedSnapshotRepresentation = prepared.receivedSealedSnapshotRep()` ($O(1)$).
   - `activeBufferFromIndex = prepared.snapshotIndex() + 1`.
5. **Raft State & Membership Installation**:
   - Install `SnapshotEntry` into Raft log, restore membership and `membershipLogIndex`, and apply log suffix retention plan.
   - Update `commitIndex = prepared.snapshotIndex()` and `lastApplied = prepared.snapshotIndex()`.
6. **Fail-Stop Invariant**: If any error occurs after step 3 begins, the member immediately executes fail-stop to prevent partial state corruption.

---

## 6. Constant-Time Raft Log Prefix Detachment

To ensure the partition thread never blocks during log truncation:
1. **Epoch-Based Segment Table**: `RaftLog` maintains an `AtomicReference<SegmentTableView>` where `SegmentTableView(epoch, baseLogIndex, List<LogSegment> segments)`.
2. **Non-Resurrecting Reader Leases**: Network replication readers acquire a non-resurrecting lease `tryAcquire(epoch)` on the captured segment view and hold it through serialization.
3. **$O(1)$ Logical Detachment**: `raftLog.setSnapshotLogical(snapshotEntry, truncateIndex)` installs a newly published segment table view and marks the previous epoch as retired in $O(1)$ without list iteration or array copies.
4. **Deferred Physical Reclamation**: Detached segments from retired epochs are deallocated off-thread by an asynchronous cleanup worker only after active reader leases reach zero.

---

## 7. Lease Model & Representation Lifecycle

Physical representations and consumer handles are strictly separated:

```
+--------------------------------------------------------------------+
|               SnapshotRepresentation (Physical State)              |
|  - RepresentationId: (groupId, incarnation, repGen, snapshotIndex) |
|  - State: UNSEALED -> SEALED -> RETIRED                            |
|  - Non-resurrecting Atomic Reference Counter                       |
|  - Retains BaseRef + LeasedDeltaRange                              |
+---------------------------------+----------------------------------+
                                  |
            +---------------------+---------------------+
            |                                           |
            v                                           v
  [ SnapshotHandle (Compactor) ]              [ SnapshotHandle (Network Stream) ]
  - Idempotent Closeable                      - Idempotent Closeable
  - Released on Compaction End                - Admitted only when SEALED
```

1. **Handle Acquisition**: `representation.acquireHandle()` returns an independent `SnapshotHandle` only while state is `SEALED` (or `UNSEALED` for local compactor). If `refCount == 0` or state is `RETIRED`, acquisition fails fast.
2. **Retirement**: When the partition thread promotes a new representation, the previous representation is marked `RETIRED`. No new handles can be acquired.
3. **Pending Reclamation Accounting**: Memory held by retired representations remains charged to `pendingReclamationBytes` until their last active handle is closed and physical memory is released.

---

## 8. Memory Budgeting & Liveness Safety

Total CP memory is governed by a strict inequality across all 9 discrete pools:

$$M_{\text{live}} + M_{\text{active\_delta}} + M_{\text{frozen\_deltas}} + M_{\text{current\_base}} + M_{\text{leased\_old\_bases}} + M_{\text{compaction\_reserve}} + M_{\text{restore\_staging}} + M_{\text{transfer\_buffers}} + M_{\text{pending\_reclamation}} + H_{\text{voter}} \le M_{\text{total}}$$

- **Steady State (Estimated)**: $\approx 2.0\times - 2.3\times$ baseline live state.
- **Peak Load (Budgeted)**: Up to $3.5\times$ baseline live state.
- **Proactive Compaction Workspace Reservation**:
  $$M_{\text{compaction\_reserve}} = \sum_{s \in \text{Services}} W_s \times \text{BaseSize}_s$$
  This reserve is allocated upfront before admitting deltas that can trigger compaction.
- **Emergency Liveness Protection**: If total CP memory usage exceeds 90% of budget:
  1. Active streaming snapshot transfers are aborted and evicted with backoff to release leased bases.
  2. Compaction tasks are prioritized to accelerate delta reclamation.
  3. Client proposals receive `OverloadRetryException`.

---

## 9. Scope & Initial Lifecycle

### 9.1 Newly Created CP Groups
- Initialized at Raft index `0` with an empty `BaseRef(gen=0, index=0)`.
- First snapshot cut at $C_1$ produces `FrozenDeltaSegment(seq=1, from=1, to=C_1)`.
- Zero-cost bootstrap without full map scans.

### 9.2 Scope Constraint
- **Initial Version Scope**: Constant-time in-memory snapshotting is enabled for all newly created CP groups.
- Existing populated groups upgraded from legacy versions will retain standard snapshot mechanisms until a dedicated offline migration utility is executed.

---

## 10. Configuration & Observability

### Configuration Properties:
- `max-delta-entries` (default: 65536): Fixed active buffer capacity.
- `max-delta-bytes` (default: 128 MB): Memory threshold for delta segments before triggering urgent compaction.
- `max-delta-segments` (default: 16): Maximum delta segments retained before compaction.
- `compaction-rate-limit-mb` (default: 64 MB/s): Rate limit for compaction base materialization.
- `max-pipeline-depth` (default: 64): Maximum allowable in-flight pipeline proposals ($P_{\text{max}}$).
- `max-entries-per-proposal` (default: 256): Validated entries per proposal bound ($E_{\text{max}}$).
- `max-worst-case-delta-bytes` (default: 4096 bytes): Conservative per-proposal byte expansion limit ($B_{\text{max}}$).
- `max-streaming-chunk-size` (default: 65536 bytes): Fixed chunk frame size for snapshot streaming.

### Dynamic Metrics (`cp.snapshot`):
- `cutPauseNanos`: Partition thread pause during snapshot cut (P50, P99, P99.9).
- `restorePauseNanos`: Partition thread pause during follower root pointer swap.
- `storeSegmentCount`: Total delta segments currently retained.
- `storeBytes`: Total bytes held by active and pending-reclamation segments.
- `compactionDurationMillis`: Background compaction duration.
- `pendingReclamationBytes`: Memory held by retired representations awaiting lease release.
- `admissionRejections`: Proposals throttled/rejected by pre-consensus admission control.
- `activePipelineProposals`: Number of currently unacknowledged in-flight proposals.
- `unsealedRepresentationCount`: Number of representations awaiting off-thread sealing.
