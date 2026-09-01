# Implementation Summary: Hazelcast CP In-Memory Constant-Time Snapshotting (v2)

## 1. Design Decision & Reference
- **Design Document**: [`design.md`](file:///Users/ahmet.mircik/ai_cp/origin_cp_os_moved/design.md) (Design Version 12)
- **Review Decision**: **Decision D14** in [`review.md`](file:///Users/ahmet.mircik/ai_cp/origin_cp_os_moved/review.md) (All findings R1-0 through R6-2 ACCEPTED; design formally approved).
- **Implementation Review Decision**: **Decision D20** in [`implementation-review.md`](file:///Users/ahmet.mircik/ai_cp/origin_cp_os_moved/implementation-review.md) (**APPROVED** — all findings IMPL-1 through IMPL-16, CPMAP-1, and DC-1 fully resolved).


---

## 2. Intended Behavior & Current Implementation State
1. **$O(S)$ Phase 1 Cut**:
   - Zero-copy $O(1)$ array transfer in `ActiveDeltaBuffer.detach()` producing `FrozenDeltaSegment`.
   - $O(1)$ circular ring append to `DeltaSegmentStore`.
   - $O(1)$ capture of `LedgerRangeLease`.
   - Instant swap to pre-reserved spare `ActiveDeltaBuffer`.
   - Dynamic binding of `snapshotTerm` from Raft log and consensus `groupIncarnation`.
   - Enqueuing unsealed candidate to `SnapshotSealerWorker` drained by active background daemon thread.
2. **CPMap Data Path Integration & Delta Buffer Feeding**:
   - In `NodeEngineRaftIntegration.runOperation`: when constant-time snapshotting is enabled, committed CPMap mutations (`CPMapPutOp`, `CPMapSetOp`, `CPMapPutIfAbsentOp`, `CPMapCompareAndSetOp`, `CPMapRemoveOp`, `CPMapDeleteOp`) are automatically recorded as `DeltaEntry` records (`OP_PUT` or `OP_REMOVE`) directly into the coordinator's `ActiveDeltaBuffer`.
   - Ordered sub-operation `raftOrdinal` tracking (`nextOrdinalFor(commitIndex)`) distinguishing multiple mutations under the same `commitIndex`.
   - In accordance with CP Subsystem design, `CPMapRegistry.destroyMapStore` permanently disables reuse of destroyed map names within the same CP group (`DistributedObjectDestroyedException`). `objectGeneration` is set to `1L` in `DeltaEntry`, representing the map's single unique incarnation and reserved for future cross-group object migration.
   - `CPMapSnapshotCodec` handles dynamic UTF-8 and `HeapData` map name resolution, Last-Write-Wins folding, streaming 64 KB chunk frame codecs, and restoring live map stores on follower restore.
3. **Decoupled Asynchronous Sealing Pipeline (Phase 2)**:
   - `SnapshotSealerWorker` with bounded queue (1 in-flight + 1 coalesced pending slot), running on a dedicated daemon thread `cp-snapshot-sealer-<group>`.
   - Real streaming frame serialization of service bases and leased delta segments via `SnapshotFrameSink`.
   - 64 KB chunk hashing computing SHA-256 digests over all serialized payloads into `framePayloadChecksums`.
   - Extraction of unreleased proposal reservations from ledger lease.
   - Deterministic `HZ_SNAP1` canonical digest calculation.
   - Off-thread sealing triggering partition-thread monotonic publication via `GroupSnapshotCoordinator.publishSealedSnapshot`.
4. **Monotonic Atomic Publication & Raft Log Truncation**:
   - Updates `latestSealedSnapshotRepresentation` and advances `lastSnapshotIndex` strictly monotonically on the partition thread.
   - In `RaftNodeImpl`, Phase 1 cut creates `SnapshotEntry` and installs it into `RaftLog.setSnapshot(...)` advancing the logical head in $O(1)$.
   - Retires predecessor representation without in-place mutation.
5. **Lineage-CAS Compaction Pipeline**:
   - `SnapshotCompactor` materializes deeply immutable folded bases from uncompacted delta segments on an asynchronous compaction executor `cp-snapshot-compactor-<group>`.
   - Threshold-driven compaction automatically triggered when uncompacted segments accumulate.
   - Enforces full 7-field Lineage-CAS in `promoteCompaction`:
     1. `groupIncarnation == currentGroupIncarnation`
     2. `expectedPublishedRepId.equals(latestSealedSnapshotRepresentation.id())`
     3. `expectedBaseGen == currentBaseRef.baseGeneration()`
     4. `currentTailSeqAtCapture == deltaSegmentStore.currentTailSequence()`
     5. `expectedFirstSeq == deltaSegmentStore.reclamationFloor()`
     6. `deltaSegmentStore.getSegment(targetSeq).toRaftIndex() == targetIndex`
     7. `deltaSegmentStore.getSegment(targetSeq).snapshotTerm() == targetTerm`
6. **Truthful Follower Restore Pipeline**:
   - `FollowerRestoreWorker` streams frames via `SnapshotFrameSource`, tracks memory in `ServiceWorkspace`, and verifies representation digests.
   - Assembles `PreparedGroupState` off-thread via `prepareFollowerRestore`.
   - Partition-thread `applyFollowerRestore`:
     - Monotonicity check rejecting stale non-duplicate snapshots.
     - Idempotent fast path for exact duplicate snapshots.
     - Atomic live root and service container pointer swaps with fail-stop error boundary.
     - Reconstructs CPMap stores from restored snapshot representations.
     - Ring directory reinitialization and `ProposalLedger.reconstruct()`.
   - Wired into `NodeEngineRaftIntegration.restoreSnapshot(...)`.
7. **Lifecycle & Thread Management**:
   - `GroupSnapshotCoordinator` starts `cp-snapshot-sealer-<group>` thread and `cp-snapshot-compactor-<group>` executor upon initialization.
   - Explicit `shutdown()` hooks wired into `NodeEngineRaftIntegration.onNodeStatusChange` (when `status == TERMINATED`) and `NodeEngineRaftIntegration.onGroupDestroyed(groupId)` to cleanly terminate sealer and compactor background threads when the CP group / node terminates.
8. **Feature Flag Integration & Legacy Path Protection**:
   - Configurable via `ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED` (`"hazelcast.cp.snapshot.v2.enabled"`), defaulting to `false`.
   - `NodeEngineRaftIntegration.takeServiceSnapshots` strictly guards against invalid calls when v2 is enabled.

---

## 3. Scope and Non-Goals
- **Scope**: Complete in-memory snapshotting core engine (`com.hazelcast.cp.internal.datastructures.snapshot.v2.*`) and integration hooks in `RaftIntegration`, `NodeEngineRaftIntegration`, and `RaftNodeImpl`.
- **Non-Goals / Explicit Limitations**:
  - Legacy chunked snapshot path is retained for backward compatibility when flag is disabled.
  - §6 (epoch-based segmented Raft log detachment) and §8 (9-pool memory budgeting / emergency throttling) are deferred to dedicated follow-on milestones.

---

## 4. Changed Implementation Areas
- `com.hazelcast.cp.internal.datastructures.snapshot.v2.core`: `Hash256`, `RepresentationState`, `SnapshotRepresentationId`, `ReservationMetadata`, `LedgerRangeLease`, `CanonicalSnapshotHeader`, `GroupSnapshotMetadata`, `DeltaEntry`, `FrozenDeltaSegment`, `ServiceBaseRef`, `BaseRef`, `SnapshotFrameSink`, `SnapshotFrameSource`, `ServiceWorkspace`, `CPServiceSnapshotCodec`.
- `com.hazelcast.cp.internal.datastructures.snapshot.v2.engine`: `ActiveDeltaBuffer`, `DeltaSegmentStore`, `ProposalLedger`, `SnapshotRepresentation`, `SnapshotHandle`, `SnapshotSealerWorker`, `FollowerRestoreWorker`, `SnapshotCompactor`, `PreparedCompaction`, `PreparedGroupState`, `GroupSnapshotCoordinator`.
- `com.hazelcast.cp.internal.datastructures.snapshot.v2.codecs`: `CPMapSnapshotCodec`.
- `com.hazelcast.cp.internal.datastructures.cpmap.operation`: `CPMapRaftOp`, `CPMapPutOp`, `CPMapRemoveOp`, `CPMapCompareAndSetOp`, `CPMapPutIfAbsentOp`.
- Core integration: `ClusterProperty`, `RaftIntegration`, `NodeEngineRaftIntegration`, `RaftNodeImpl`.

---

## 5. Current Revision
- Working tree commit base: `153b15d6`

---

## 6. Test Coverage
```bash
mvn test -PparallelTest -Dquick -DskipTests=false -Dtest="com.hazelcast.cp.internal.datastructures.snapshot.v2.*Test" -Dgit.useNativeGit=true -Dkotlin.compiler.daemon=false -Dkotlin.compiler.execution.strategy=in-process -pl hazelcast
```
- **Total Tests Run**: 23 tests across 10 test classes (0 failures, 0 errors, 0 skipped).
- **Checkstyle Audit**: `mvn checkstyle:check -pl hazelcast` passed with 0 violations.
