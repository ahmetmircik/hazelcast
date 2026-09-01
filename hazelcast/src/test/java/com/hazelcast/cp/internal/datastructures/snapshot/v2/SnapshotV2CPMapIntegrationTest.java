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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapCompareAndSetOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutIfAbsentOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapRemoveOp;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.codecs.CPMapSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.RepresentationState;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSink;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSource;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.GroupSnapshotCoordinator;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.PreparedGroupState;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.SnapshotRepresentation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotV2CPMapIntegrationTest extends HazelcastTestSupport {

    private static final byte[] KEY_1 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    private static final byte[] VAL_1 = new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    private static final byte[] VAL_2 = new byte[]{21, 22, 23, 24, 25, 26, 27, 28, 29, 30};

    @Test
    public void testCPMapMutationsFeedDeltaBufferAndSnapshotRoundTrip() throws IOException {
        CPGroupId groupId = new RaftGroupId("test-group", 1L, 1L);
        CPMapSnapshotCodec codec = new CPMapSnapshotCodec();
        GroupSnapshotCoordinator coordinator = new GroupSnapshotCoordinator(groupId, 1L, 100, Map.of(codec.serviceId(), codec));

        try {
            String mapName = "testMapNameLong";
            Data mapData = new HeapData(mapName.getBytes(StandardCharsets.UTF_8));
            Data keyData = new HeapData(KEY_1);
            Data valData1 = new HeapData(VAL_1);
            Data valData2 = new HeapData(VAL_2);

            CPMapPutOp putOp = new CPMapPutOp(mapName, keyData, valData1);
            assertEquals(mapName, putOp.getObjectName());
            assertEquals(keyData, putOp.getKey());
            assertEquals(valData1, putOp.getValue());

            CPMapPutIfAbsentOp putIfAbsentOp = new CPMapPutIfAbsentOp(mapName, keyData, valData2);
            assertEquals(keyData, putIfAbsentOp.getKey());
            assertEquals(valData2, putIfAbsentOp.getValue());

            CPMapCompareAndSetOp casOp = new CPMapCompareAndSetOp(mapName, keyData, valData1, valData2);
            assertEquals(keyData, casOp.getKey());
            assertEquals(valData1, casOp.getExpectedValue());
            assertEquals(valData2, casOp.getNewValue());

            CPMapRemoveOp removeOp = new CPMapRemoveOp(mapName, keyData);
            assertEquals(keyData, removeOp.getKey());

            coordinator.activeDeltaBuffer().append(new DeltaEntry(
                    codec.serviceId(), mapData, 1L, DeltaEntry.OP_PUT, keyData, valData1, 10L, 0));
            coordinator.activeDeltaBuffer().append(new DeltaEntry(
                    codec.serviceId(), mapData, 1L, DeltaEntry.OP_PUT, keyData, valData2, 20L, 1));

            SnapshotRepresentation candidate = coordinator.performPhase1SnapshotCut(20L, 1);
            assertNotNull(candidate);

            assertTrueEventually(() -> {
                assertEquals(RepresentationState.SEALED, candidate.state());
                assertEquals(20L, coordinator.lastSnapshotIndex());
                assertNotNull(coordinator.latestSealedSnapshotRepresentation());
            });

            List<DeltaEntry> entries = new ArrayList<>();
            for (FrozenDeltaSegment seg : candidate.leasedDeltas()) {
                seg.iterator().forEachRemaining(entries::add);
            }
            Map<String, Map<Data, Data>> foldedBase = codec.materializeBase(
                    codec.createEmptyBase(), entries.iterator());
            assertTrue(foldedBase.containsKey(mapName));
            assertEquals(valData2, foldedBase.get(mapName).get(keyData));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            SnapshotFrameSink sink = new SnapshotFrameSink() {
                private long total;
                @Override
                public void writeChunk(byte[] buffer, int offset, int length) throws IOException {
                    out.write(buffer, offset, length);
                    total += length;
                }
                @Override
                public long totalBytesWritten() {
                    return total;
                }
            };
            codec.writeBase(foldedBase, sink);

            byte[] serialized = out.toByteArray();
            ByteArrayInputStream in = new ByteArrayInputStream(serialized);
            SnapshotFrameSource source = new SnapshotFrameSource() {
                @Override
                public int readChunk(byte[] buffer, int offset, int length) {
                    return in.read(buffer, offset, length);
                }

                @Override
                public boolean hasMoreChunks() {
                    return in.available() > 0;
                }
            };

            CanonicalSnapshotHeader header = candidate.header();
            PreparedGroupState preparedState = coordinator.prepareFollowerRestore(header, source, 1L, 1L);
            assertNotNull(preparedState);
            assertTrue(coordinator.applyFollowerRestore(preparedState));
            assertEquals(20L, coordinator.lastSnapshotIndex());
        } finally {
            coordinator.shutdown();
        }
    }

    @Test
    public void testRealCPMapClusterMutationsWithConstantTimeSnapshotFlag() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        try {
            Config config = new Config();
            config.setProperty(ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED.getName(), "true");
            config.getCPSubsystemConfig().setCPMemberCount(3).setGroupSize(3);

            HazelcastInstance[] instances = factory.newInstances(config);
            assertClusterSizeEventually(3, instances[0]);

            CPMap<String, String> map = instances[0].getCPSubsystem().getMap("testCPMapLive");
            map.put("key1", "val1");
            map.put("key2", "val2");
            map.putIfAbsent("key3", "val3");
            map.compareAndSet("key2", "val2", "val2_updated");
            map.remove("key1");

            assertNull(map.get("key1"));
            assertEquals("val2_updated", map.get("key2"));
            assertEquals("val3", map.get("key3"));
        } finally {
            factory.terminateAll();
        }
    }

    @Test
    public void testRealCPMapFollowerRestoreAfterSnapshotCut() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        try {
            Config config = new Config();
            config.setProperty(ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED.getName(), "true");
            config.getCPSubsystemConfig().setCPMemberCount(3).setGroupSize(3);
            config.getCPSubsystemConfig().getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(20);

            HazelcastInstance[] instances = factory.newInstances(config, 3);
            assertClusterSizeEventually(3, instances[0]);

            CPMap<String, String> map = instances[0].getCPSubsystem().getMap("testCPMapRestore");
            for (int i = 0; i < 25; i++) {
                map.put("k-" + i, "v-" + i);
            }

            // Shutdown the 3rd instance
            instances[2].shutdown();

            // Create 4th instance and promote to CP member to trigger snapshot install/restore
            HazelcastInstance newInstance = factory.newHazelcastInstance(config);
            newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                    .toCompletableFuture().get();

            CPMap<String, String> newMap = newInstance.getCPSubsystem().getMap("testCPMapRestore");
            assertTrueEventually(() -> {
                for (int i = 0; i < 25; i++) {
                    assertEquals("v-" + i, newMap.get("k-" + i));
                }
            });
        } finally {
            factory.terminateAll();
        }
    }
}
