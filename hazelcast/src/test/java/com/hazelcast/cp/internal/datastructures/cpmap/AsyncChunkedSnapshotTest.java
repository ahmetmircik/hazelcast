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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import com.hazelcast.cp.internal.datastructures.snapshot.SnapshotPipelineConfig;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * End-to-end smoke test of the snapshot redesign with the feature enabled:
 * the O(1) async chunked snapshot generation (capture on the Raft thread,
 * scan/chunk/persist on the snapshot worker, bounded install back on the
 * Raft thread) together with the file-backed RaftStateStore and the delta
 * pipeline, exercised in a real 3-member cluster over several snapshot
 * cycles. The state must stay consistent on every member.
 */
@RunWith(HazelcastSerialClassRunner.class)
public class AsyncChunkedSnapshotTest extends HazelcastRaftTestSupport {

    private static final int SNAPSHOT_THRESHOLD = 100;

    private HazelcastInstance[] instances;

    @BeforeClass
    public static void enableSnapshotRedesign() throws IOException {
        deleteRecursively(Path.of("target/async-chunked-snapshot-test"));
        System.setProperty(SnapshotPipelineConfig.SNAPSHOT_ENABLED_PROPERTY, "true");
        System.setProperty(SnapshotPipelineConfig.SNAPSHOT_DIR_PROPERTY,
                "target/async-chunked-snapshot-test");
    }

    @AfterClass
    public static void disableSnapshotRedesign() {
        System.clearProperty(SnapshotPipelineConfig.SNAPSHOT_ENABLED_PROPERTY);
        System.clearProperty(SnapshotPipelineConfig.SNAPSHOT_DIR_PROPERTY);
    }

    @Before
    public void setup() {
        instances = newInstances(3);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig().getRaftAlgorithmConfig()
                .setCommitIndexAdvanceCountToSnapshot(SNAPSHOT_THRESHOLD);
        return config;
    }

    @Test
    public void asyncChunkedSnapshotAndDeltaPipeline_keepStateConsistent() {
        String mapName = "testCPMap";
        CPMap<String, String> testCPMap = instances[0].getCPSubsystem().getMap(mapName);
        CPGroupId groupId = ((CPMapProxy) testCPMap).getGroupId();

        Map<String, String> localMap = new HashMap<>();
        // several snapshot cycles: async capture + worker scan + install
        int total = SNAPSHOT_THRESHOLD * 3;
        for (int i = 0; i < total; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            testCPMap.put(key, value);
            localMap.put(key, value);
        }

        // the async chunked snapshot was generated and installed on the leader
        checkSnapshot(getLeaderNode(instances, groupId));

        // the state stays consistent on every member (leader and followers):
        // the off-thread scan + overlay finish + transfer did not corrupt or
        // lose anything
        for (HazelcastInstance instance : instances) {
            CPMap<String, String> map = instance.getCPSubsystem().getMap(mapName);
            for (Map.Entry<String, String> e : localMap.entrySet()) {
                assertEquals("member " + instance.getName(), e.getValue(), map.get(e.getKey()));
            }
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            try (var files = Files.list(path)) {
                for (Path child : files.toList()) {
                    deleteRecursively(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }

    private void checkSnapshot(RaftNodeImpl instance) {
        assertTrueEventually(() -> {
            SnapshotEntry snapshotEntry = instance.state().log().snapshot();
            assertGreaterOrEquals("snapshot size", snapshotEntry.index(), SNAPSHOT_THRESHOLD);
        });
    }
}
