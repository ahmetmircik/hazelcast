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

package com.hazelcast.cp.internal.datastructures.snapshot.v2;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftEndpointImpl;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.codecs.CPMapSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.Hash256;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.RepresentationState;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSource;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotRepresentationId;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.GroupSnapshotCoordinator;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.PreparedCompaction;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.PreparedGroupState;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.SnapshotCompactor;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.SnapshotRepresentation;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GroupSnapshotCoordinatorTest extends HazelcastTestSupport {

    private static final byte[] VALID_BYTES_1 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    private static final byte[] VALID_BYTES_2 = new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    private static final byte[] VALID_BYTES_3 = new byte[]{21, 22, 23, 24, 25, 26, 27, 28, 29, 30};

    @Test
    public void testPhase1CutSealingAndMonotonicPublication() {
        CPGroupId groupId = new RaftGroupId("test-group", 1L, 1L);
        CPMapSnapshotCodec codec = new CPMapSnapshotCodec();
        GroupSnapshotCoordinator coordinator = new GroupSnapshotCoordinator(groupId, 1L, 100, Map.of(codec.serviceId(), codec));

        try {
            coordinator.activeDeltaBuffer().append(new DeltaEntry(
                    codec.serviceId(), new HeapData(VALID_BYTES_1), 1L, DeltaEntry.OP_PUT,
                    new HeapData(VALID_BYTES_2), new HeapData(VALID_BYTES_3), 10L, 0));

            SnapshotRepresentation candidate = coordinator.performPhase1SnapshotCut(10L, 1);
            assertNotNull(candidate);
            assertEquals(10L, candidate.id().snapshotIndex());

            assertTrueEventually(() -> {
                assertEquals(RepresentationState.SEALED, candidate.state());
                assertNotNull(candidate.header());
                assertFalse(candidate.header().framePayloadChecksums().isEmpty());
                assertEquals(10L, coordinator.lastSnapshotIndex());
                assertEquals(candidate, coordinator.latestSealedSnapshotRepresentation());
            });
        } finally {
            coordinator.shutdown();
        }
    }

    @Test
    public void testCompactionWithSevenFieldLineageCas() {
        CPGroupId groupId = new RaftGroupId("test-group", 1L, 1L);
        CPMapSnapshotCodec codec = new CPMapSnapshotCodec();
        GroupSnapshotCoordinator coordinator = new GroupSnapshotCoordinator(groupId, 1L, 100, Map.of(codec.serviceId(), codec));

        try {
            coordinator.activeDeltaBuffer().append(new DeltaEntry(
                    codec.serviceId(), new HeapData(VALID_BYTES_1), 1L, DeltaEntry.OP_PUT,
                    new HeapData(VALID_BYTES_2), new HeapData(VALID_BYTES_3), 20L, 0));

            FrozenDeltaSegment seg = coordinator.activeDeltaBuffer().detach(1L, 20L, 20L, 1);
            coordinator.deltaSegmentStore().append(seg);

            SnapshotCompactor compactor = new SnapshotCompactor();
            SnapshotCompactor.CompactionRequest request = new SnapshotCompactor.CompactionRequest(
                    coordinator.latestSealedSnapshotRepresentation(),
                    1L,
                    1L,
                    20L,
                    1,
                    coordinator.deltaSegmentStore().currentTailSequence(),
                    coordinator.deltaSegmentStore().reclamationFloor(),
                    List.of(seg),
                    Map.of(codec.serviceId(), codec),
                    10L
            );
            PreparedCompaction prep = compactor.prepareCompaction(request);

            assertTrue("Compaction promotion with valid 7-field Lineage-CAS must succeed",
                    coordinator.promoteCompaction(prep));
            assertEquals(20L, coordinator.latestSealedSnapshotRepresentation().id().snapshotIndex());

            PreparedCompaction stalePrep = new PreparedCompaction(
                    1L,
                    new SnapshotRepresentationId(groupId, 1L, 999L, 0L),
                    0L,
                    1L,
                    20L,
                    1,
                    1L,
                    1L,
                    prep.compactedRepresentation()
            );
            assertFalse("Stale compaction must be rejected by CAS", coordinator.promoteCompaction(stalePrep));
        } finally {
            coordinator.shutdown();
        }
    }

    @Test
    public void testFollowerRestoreAndSwitchoverSafety() throws IOException {
        CPGroupId groupId = new RaftGroupId("test-group", 1L, 1L);
        CPMapSnapshotCodec codec = new CPMapSnapshotCodec();
        GroupSnapshotCoordinator coordinator = new GroupSnapshotCoordinator(groupId, 1L, 100, Map.of(codec.serviceId(), codec));

        try {
            CanonicalSnapshotHeader header = new CanonicalSnapshotHeader(
                    groupId, 1L, 50L, 1, 25L,
                    List.of(new RaftEndpointImpl(UUID.randomUUID())),
                    Map.of(codec.serviceId(), codec.codecVersion()),
                    1L, 5L, 1L, 1L, List.of(),
                    List.of(new Hash256(new byte[32]))
            );

            SnapshotFrameSource source = new SnapshotFrameSource() {
                private boolean read;
                @Override
                public int readChunk(byte[] buffer, int offset, int length) {
                    if (!read) {
                        read = true;
                        buffer[offset] = 0;
                        return 1;
                    }
                    return 0;
                }
                @Override
                public boolean hasMoreChunks() {
                    return !read;
                }
            };

            PreparedGroupState prep = coordinator.prepareFollowerRestore(header, source, 1L, 20L);

            assertTrue("Follower restore must succeed on valid prepared state",
                    coordinator.applyFollowerRestore(prep));
            assertEquals(50L, coordinator.lastSnapshotIndex());

            assertTrue("Duplicate restore must be accepted idempotently",
                    coordinator.applyFollowerRestore(prep));

            PreparedGroupState stalePrep = new PreparedGroupState(
                    40L, 1, 1L, 20L, List.of(),
                    new Hash256(new byte[32]), prep.snapshotRepresentation(),
                    Map.of(), List.of(), 1L);
            assertFalse("Stale non-duplicate restore must be rejected",
                    coordinator.applyFollowerRestore(stalePrep));
        } finally {
            coordinator.shutdown();
        }
    }

    @Test
    public void testLiveBackgroundCompactionTrigger() {
        CPGroupId groupId = new RaftGroupId("test-group", 1L, 1L);
        CPMapSnapshotCodec codec = new CPMapSnapshotCodec();
        GroupSnapshotCoordinator coordinator = new GroupSnapshotCoordinator(groupId, 1L, 100, Map.of(codec.serviceId(), codec));

        try {
            for (int i = 1; i <= 10; i++) {
                coordinator.activeDeltaBuffer().append(new DeltaEntry(
                        codec.serviceId(), new HeapData(VALID_BYTES_1), 1L, DeltaEntry.OP_PUT,
                        new HeapData(VALID_BYTES_2), new HeapData(VALID_BYTES_3), i * 10L, 0));
                coordinator.performPhase1SnapshotCut(i * 10L, 1);
            }

            assertTrueEventually(() -> {
                assertTrue("Reclamation floor must advance via background compaction",
                        coordinator.deltaSegmentStore().reclamationFloor() > 1);
            });
        } finally {
            coordinator.shutdown();
        }
    }
}
