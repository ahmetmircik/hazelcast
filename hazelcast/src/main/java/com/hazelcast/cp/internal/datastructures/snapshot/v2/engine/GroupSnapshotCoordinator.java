/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cp.internal.datastructures.snapshot.v2.engine;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.BaseRef;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CPServiceSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.LedgerRangeLease;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ServiceBaseRef;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSource;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotRepresentationId;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Main coordinator executing bounded O(S) Phase 1 snapshot cuts on the partition thread.
 */
public final class GroupSnapshotCoordinator {

    private static final int COMPACTION_UNCOMPACTED_THRESHOLD = 8;

    private final CPGroupId groupId;
    private final long groupIncarnation;
    private final int deltaBufferCapacity;
    private final Map<Short, CPServiceSnapshotCodec<?, ?>> codecRegistry;

    private final DeltaSegmentStore deltaSegmentStore;
    private final ProposalLedger proposalLedger;
    private final Map<Short, Object> liveContainers;
    private final AtomicLong representationGen = new AtomicLong(1L);
    private final SnapshotSealerWorker sealerWorker;
    private final Thread sealerThread;
    private final SnapshotCompactor compactor;
    private final ExecutorService compactorExecutor;

    private ActiveDeltaBuffer activeDeltaBuffer;
    private ActiveDeltaBuffer spareDeltaBuffer;
    private BaseRef currentBaseRef;
    private SnapshotRepresentation latestSealedSnapshotRepresentation;
    private long lastSnapshotIndex;
    private long currentSegmentSequence = 1L;

    public GroupSnapshotCoordinator(
            CPGroupId groupId,
            long groupIncarnation,
            int deltaBufferCapacity,
            Map<Short, CPServiceSnapshotCodec<?, ?>> codecs
    ) {
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.groupIncarnation = groupIncarnation;
        this.deltaBufferCapacity = deltaBufferCapacity;
        this.codecRegistry = Map.copyOf(codecs);
        this.deltaSegmentStore = new DeltaSegmentStore();
        this.proposalLedger = new ProposalLedger(1L);
        this.liveContainers = new ConcurrentHashMap<>();
        this.activeDeltaBuffer = new ActiveDeltaBuffer(deltaBufferCapacity);
        this.spareDeltaBuffer = new ActiveDeltaBuffer(deltaBufferCapacity);
        this.sealerWorker = new SnapshotSealerWorker();
        this.compactor = new SnapshotCompactor();

        this.currentBaseRef = initInitialBaseRef(codecs);
        SnapshotRepresentationId initialRepId = new SnapshotRepresentationId(
                groupId, groupIncarnation, representationGen.getAndIncrement(), 0L);
        this.latestSealedSnapshotRepresentation = new SnapshotRepresentation(
                initialRepId, currentBaseRef, Collections.emptyList());
        this.lastSnapshotIndex = 0L;

        this.sealerThread = startSealerThread(groupId);
        this.compactorExecutor = createCompactorExecutor(groupId);
    }

    private BaseRef initInitialBaseRef(Map<Short, CPServiceSnapshotCodec<?, ?>> codecs) {
        Map<Short, ServiceBaseRef> initialBases = new HashMap<>();
        for (Map.Entry<Short, CPServiceSnapshotCodec<?, ?>> entry : codecs.entrySet()) {
            Object emptyBase = entry.getValue().createEmptyBase();
            initialBases.put(entry.getKey(), new ServiceBaseRef(entry.getKey(), 0L, emptyBase));
            liveContainers.put(entry.getKey(), emptyBase);
        }
        return new BaseRef(0L, 1L, initialBases);
    }

    private Thread startSealerThread(CPGroupId groupId) {
        Thread t = new Thread(sealerWorker, "cp-snapshot-sealer-" + groupId.getName());
        t.setDaemon(true);
        t.start();
        return t;
    }

    private ExecutorService createCompactorExecutor(CPGroupId groupId) {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "cp-snapshot-compactor-" + groupId.getName());
            t.setDaemon(true);
            return t;
        });
    }

    public CPGroupId groupId() {
        return groupId;
    }

    public long groupIncarnation() {
        return groupIncarnation;
    }

    public ActiveDeltaBuffer activeDeltaBuffer() {
        return activeDeltaBuffer;
    }

    public ProposalLedger proposalLedger() {
        return proposalLedger;
    }

    public DeltaSegmentStore deltaSegmentStore() {
        return deltaSegmentStore;
    }

    public SnapshotSealerWorker sealerWorker() {
        return sealerWorker;
    }

    public SnapshotCompactor compactor() {
        return compactor;
    }

    public SnapshotRepresentation latestSealedSnapshotRepresentation() {
        return latestSealedSnapshotRepresentation;
    }

    public long lastSnapshotIndex() {
        return lastSnapshotIndex;
    }

    public void registerLiveContainer(short serviceId, Object container) {
        liveContainers.put(serviceId, container);
    }

    public SnapshotRepresentation performPhase1SnapshotCut(
            long commitIndex,
            int snapshotTerm,
            long membershipLogIndex,
            List<RaftEndpoint> orderedMembers
    ) {
        long seq = currentSegmentSequence++;
        FrozenDeltaSegment segment = activeDeltaBuffer.detach(seq, commitIndex, commitIndex, snapshotTerm);
        deltaSegmentStore.append(segment);

        LedgerRangeLease lease = proposalLedger.captureRangeLease();
        List<FrozenDeltaSegment> leasedSegments = deltaSegmentStore.acquireRange(
                deltaSegmentStore.reclamationFloor(), seq);

        SnapshotRepresentationId repId = new SnapshotRepresentationId(
                groupId, groupIncarnation, representationGen.getAndIncrement(), commitIndex);
        SnapshotRepresentation candidate = new SnapshotRepresentation(repId, currentBaseRef, leasedSegments);

        activeDeltaBuffer = spareDeltaBuffer;
        spareDeltaBuffer = new ActiveDeltaBuffer(deltaBufferCapacity);

        sealerWorker.offerTask(new SnapshotSealerWorker.SealerTask(
                candidate,
                lease,
                proposalLedger,
                snapshotTerm,
                membershipLogIndex,
                orderedMembers,
                codecRegistry,
                this::publishSealedSnapshot
        ));

        if (deltaSegmentStore.currentTailSequence() - deltaSegmentStore.reclamationFloor()
                >= COMPACTION_UNCOMPACTED_THRESHOLD) {
            triggerCompactionAsync();
        }

        return candidate;
    }

    public SnapshotRepresentation performPhase1SnapshotCut(long commitIndex, int snapshotTerm) {
        return performPhase1SnapshotCut(commitIndex, snapshotTerm, 0L, List.of());
    }

    public void triggerCompactionAsync() {
        compactorExecutor.submit(() -> {
            try {
                long firstSeq = deltaSegmentStore.reclamationFloor();
                long tailSeq = deltaSegmentStore.currentTailSequence();
                if (tailSeq <= firstSeq) {
                    return;
                }
                long targetSeq = tailSeq;
                FrozenDeltaSegment targetSeg = deltaSegmentStore.getSegment(targetSeq);
                if (targetSeg == null) {
                    return;
                }
                List<FrozenDeltaSegment> segments = deltaSegmentStore.acquireRange(firstSeq, targetSeq);
                SnapshotCompactor.CompactionRequest request = new SnapshotCompactor.CompactionRequest(
                        latestSealedSnapshotRepresentation,
                        groupIncarnation,
                        targetSeq,
                        targetSeg.toRaftIndex(),
                        targetSeg.snapshotTerm(),
                        tailSeq,
                        firstSeq,
                        segments,
                        codecRegistry,
                        representationGen.getAndIncrement()
                );

                PreparedCompaction prep = compactor.prepareCompaction(request);
                promoteCompaction(prep);
            } catch (Throwable ignored) {
                // Background compaction retry on next threshold trigger
            }
        });
    }

    public PreparedGroupState prepareFollowerRestore(
            CanonicalSnapshotHeader header,
            SnapshotFrameSource source,
            long expectedFirstSeq,
            long targetSeq
    ) throws IOException {
        FollowerRestoreWorker restoreWorker = new FollowerRestoreWorker();
        return restoreWorker.restoreFromStream(header, source, codecRegistry, expectedFirstSeq, targetSeq);
    }

    public synchronized void publishSealedSnapshot(SnapshotRepresentation sealedSnapshot) {
        if (sealedSnapshot.id().snapshotIndex() > lastSnapshotIndex) {
            if (latestSealedSnapshotRepresentation != null) {
                latestSealedSnapshotRepresentation.retire();
            }
            this.latestSealedSnapshotRepresentation = sealedSnapshot;
            this.lastSnapshotIndex = sealedSnapshot.id().snapshotIndex();
        } else {
            sealedSnapshot.retire();
        }
    }

    public synchronized boolean promoteCompaction(PreparedCompaction prep) {
        if (prep.groupIncarnation() != groupIncarnation) {
            return false;
        }
        if (!prep.expectedPublishedRepId().equals(latestSealedSnapshotRepresentation.id())) {
            return false;
        }
        if (prep.expectedBaseGen() != currentBaseRef.baseGeneration()) {
            return false;
        }
        if (prep.currentTailSeqAtCapture() != deltaSegmentStore.currentTailSequence()) {
            return false;
        }
        if (prep.expectedFirstSeq() != deltaSegmentStore.reclamationFloor()) {
            return false;
        }
        FrozenDeltaSegment targetSeg = deltaSegmentStore.getSegment(prep.targetSeq());
        if (targetSeg == null || targetSeg.toRaftIndex() != prep.targetIndex()
                || targetSeg.snapshotTerm() != prep.targetTerm()) {
            return false;
        }

        latestSealedSnapshotRepresentation.retire();
        this.latestSealedSnapshotRepresentation = prep.compactedRepresentation();
        this.currentBaseRef = prep.compactedRepresentation().baseRef();
        deltaSegmentStore.advanceReclamationFloor(prep.targetSeq() + 1);
        return true;
    }

    public synchronized boolean applyFollowerRestore(PreparedGroupState prep) {
        if (prep.snapshotIndex() < lastSnapshotIndex) {
            return false;
        }

        if (prep.snapshotIndex() == lastSnapshotIndex) {
            CanonicalSnapshotHeader currentHeader = latestSealedSnapshotRepresentation.header();
            if (currentHeader != null
                    && prep.representationDigest().constantTimeEquals(currentHeader.representationDigest())) {
                return true;
            }
            return false;
        }

        try {
            for (Map.Entry<Short, Object> entry : prep.liveContainers().entrySet()) {
                short serviceId = entry.getKey();
                @SuppressWarnings("unchecked")
                CPServiceSnapshotCodec codec = codecRegistry.get(serviceId);
                if (codec != null) {
                    Object currentContainer = liveContainers.get(serviceId);
                    Object swapped = codec.swapLiveContainer(currentContainer, entry.getValue());
                    liveContainers.put(serviceId, swapped);
                }
            }

            if (latestSealedSnapshotRepresentation != null) {
                latestSealedSnapshotRepresentation.retire();
            }

            this.latestSealedSnapshotRepresentation = prep.snapshotRepresentation();
            this.currentBaseRef = prep.snapshotRepresentation().baseRef();
            this.lastSnapshotIndex = prep.snapshotIndex();

            this.deltaSegmentStore.clear();
            this.activeDeltaBuffer = new ActiveDeltaBuffer(deltaBufferCapacity);
            this.spareDeltaBuffer = new ActiveDeltaBuffer(deltaBufferCapacity);
            this.currentSegmentSequence = 1L;

            proposalLedger.reconstruct(prep.snapshotFloor(), prep.unreleasedReservations());
            return true;
        } catch (Throwable t) {
            throw new IllegalStateException("Fail-stop: unexpected failure during snapshot switchover", t);
        }
    }

    public void shutdown() {
        sealerWorker.shutdown();
        if (sealerThread != null) {
            sealerThread.interrupt();
        }
        if (compactorExecutor != null) {
            compactorExecutor.shutdownNow();
        }
    }
}
