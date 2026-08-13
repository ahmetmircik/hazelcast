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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.cp.RaftAlgorithmConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.OperationPacketFilter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cp.internal.RaftServiceDataSerializerHook.F_ID;
import static com.hazelcast.cp.internal.RaftServiceSerializerConstants.APPEND_REQUEST_OP;
import static com.hazelcast.cp.internal.RaftServiceSerializerConstants.INSTALL_SNAPSHOT_REQUEST_OP;
import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.MAX_CHUNK_SIZE_IN_MB;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLastLogOrSnapshotEntry;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getLeaderMember;
import static com.hazelcast.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.PacketFiltersUtil.setCustomFilter;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ChunkedSnapshotTransferLeaderChangeTest extends HazelcastRaftTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int SNAPSHOT_THRESHOLD = 5;
    private static final int LARGE_VALUE_SIZE = 2 * 1024 * 1024;
    private static final String GROUP_NAME = "chunked-snapshot-leader-change";
    private static final String MAP_NAME = "map@" + GROUP_NAME;

    @Test
    public void follower_catches_up_from_older_snapshot_after_leader_change() throws Exception {
        HazelcastInstance[] instances = newInstances(NODE_COUNT);
        CPMap<String, byte[]> map = instances[0].getCPSubsystem().getMap(MAP_NAME);

        // Use two large values so each snapshot has more than one data chunk.
        map.set("large-1", new byte[LARGE_VALUE_SIZE]);
        map.set("large-2", new byte[LARGE_VALUE_SIZE]);

        CPGroup group = instances[0].getCPSubsystem().getCPSubsystemManagementService()
                                  .getCPGroup(GROUP_NAME).toCompletableFuture().get();
        CPGroupId groupId = group.id();
        HazelcastInstance leader = getLeaderInstance(instances, groupId);

        ArrayList<HazelcastInstance> followers = new ArrayList<>();
        for (HazelcastInstance instance : instances) {
            if (instance != leader) {
                followers.add(instance);
            }
        }
        HazelcastInstance futureLeader = followers.get(0);
        HazelcastInstance stalledFollower = followers.get(1);

        // Bring all members to the same snapshot before creating different follower states.
        long initialSnapshotIndex = takeNextSnapshot(map, leader, futureLeader, groupId, 0);
        assertTrueEventually(() -> assertEquals(initialSnapshotIndex, snapshotIndex(stalledFollower, groupId)));
        long stalledCommitIndex = commitIndex(stalledFollower, groupId);

        LeaderChangePacketFilter filter = new LeaderChangePacketFilter(
                getNode(leader).getSerializationService(),
                getAddress(futureLeader), getAddress(stalledFollower));
        filter.blockStalledFollower = true;
        setCustomFilter(leader, filter);

        // Let the future leader take an older snapshot while the stalled follower stays behind.
        long olderSnapshotIndex = takeNextSnapshot(
                map, leader, futureLeader, groupId, initialSnapshotIndex);
        assertTrue(stalledCommitIndex < olderSnapshotIndex);

        // Stop one entry before the original leader's next snapshot.
        for (int i = 1; i < SNAPSHOT_THRESHOLD; i++) {
            map.set("before-newer-snapshot-" + i, new byte[]{(byte) i});
        }
        long newerSnapshotIndex = olderSnapshotIndex + SNAPSHOT_THRESHOLD;
        assertTrueEventually(() -> assertEquals(newerSnapshotIndex - 1, commitIndex(futureLeader, groupId)));

        // Send the final entry to the future leader, but block later commit updates.
        filter.limitAppendRequestsToFutureLeader = true;
        filter.appendRequestsToFutureLeaderToAllow.set(1);
        Future<?> finalWrite = spawn(() -> map.set("newer-snapshot", new byte[]{1}));
        assertTrueEventually(() -> assertEquals(newerSnapshotIndex, lastLogIndex(futureLeader, groupId)));
        finalWrite.get(30, SECONDS);

        assertTrueEventually(() -> assertEquals(newerSnapshotIndex, snapshotIndex(leader, groupId)));
        assertEquals(olderSnapshotIndex, snapshotIndex(futureLeader, groupId));
        assertEquals(newerSnapshotIndex - 1, commitIndex(futureLeader, groupId));

        // Send only metadata and one data chunk, leaving the newer snapshot incomplete.
        int snapshotRequestCount = filter.snapshotRequestCount.get();
        filter.snapshotRequestsToStalledFollowerToAllow.set(2);
        assertTrueEventually(() -> assertTrue(filter.snapshotRequestCount.get() >= snapshotRequestCount + 3));
        assertEquals(initialSnapshotIndex, snapshotIndex(stalledFollower, groupId));

        // The two remaining members need each other for quorum. The new leader must
        // replace the follower's incomplete newer transfer with its older snapshot.
        leader.getLifecycleService().terminate();
        assertTrueEventually(() -> {
            RaftEndpoint electedLeader = getLeaderMember(raftNode(futureLeader, groupId));
            assertNotNull(electedLeader);
            assertEquals(futureLeader.getCPSubsystem().getLocalCPMember().getUuid(), electedLeader.getUuid());
        });

        // A successful write proves the two remaining members can form a working quorum.
        CPMap<String, byte[]> mapOnNewLeader = futureLeader.getCPSubsystem().getMap(MAP_NAME);
        Future<?> progress = spawn(() -> mapOnNewLeader.set("progress", new byte[]{1}));
        progress.get(30, SECONDS);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        // A 1 MB limit puts each 2 MB value in a separate chunk.
        config.setProperty(MAX_CHUNK_SIZE_IN_MB.getName(), "1");

        RaftAlgorithmConfig raftConfig = config.getCPSubsystemConfig().getRaftAlgorithmConfig();

        // Snapshot every five commits to keep the test short and indexes predictable.
        raftConfig.setCommitIndexAdvanceCountToSnapshot(SNAPSHOT_THRESHOLD);

        // Start failover after one missed heartbeat.
        raftConfig.setMaxMissedLeaderHeartbeatCount(1);

        // A 200 ms base gives snapshot retries a four-second maximum backoff.
        raftConfig.setAppendRequestBackoffTimeoutInMillis(200);

        // Keep election rounds to one second.
        raftConfig.setLeaderElectionTimeoutInMillis(1_000);
        return config;
    }

    private long takeNextSnapshot(CPMap<String, byte[]> map, HazelcastInstance leader,
                                  HazelcastInstance futureLeader, CPGroupId groupId,
                                  long currentSnapshotIndex) {
        long currentCommitIndex = commitIndex(leader, groupId);
        long nextSnapshotIndex = currentSnapshotIndex + SNAPSHOT_THRESHOLD;
        int entryCount = (int) (nextSnapshotIndex - currentCommitIndex);
        assertTrue("Expected at least one entry before the next snapshot", entryCount > 0);

        for (int i = 0; i < entryCount; i++) {
            map.set("snapshot-" + nextSnapshotIndex + '-' + i, new byte[]{(byte) i});
        }

        assertTrueEventually(() -> assertEquals(nextSnapshotIndex, snapshotIndex(futureLeader, groupId)));
        return nextSnapshotIndex;
    }

    private static long snapshotIndex(HazelcastInstance instance, CPGroupId groupId) {
        return getSnapshotEntry(raftNode(instance, groupId)).index();
    }

    private static long commitIndex(HazelcastInstance instance, CPGroupId groupId) {
        return getCommitIndex(raftNode(instance, groupId));
    }

    private static long lastLogIndex(HazelcastInstance instance, CPGroupId groupId) {
        return getLastLogOrSnapshotEntry(raftNode(instance, groupId)).index();
    }

    private static RaftNodeImpl raftNode(HazelcastInstance instance, CPGroupId groupId) {
        return getRaftNode(instance, groupId);
    }

    /**
     * Filters packets from the original leader to create the required follower states.
     * The future leader gets the final log entry but not its commit update. The stalled
     * follower gets snapshot metadata and one data chunk. Since the filter is on the
     * original leader, {@code endpoint} is the packet destination.
     */
    private static final class LeaderChangePacketFilter extends OperationPacketFilter {
        private final Address futureLeader;
        private final Address stalledFollower;

        // Number of matching requests allowed before later requests are dropped.
        private final AtomicInteger appendRequestsToFutureLeaderToAllow = new AtomicInteger();
        private final AtomicInteger snapshotRequestsToStalledFollowerToAllow = new AtomicInteger();

        // Count allowed and dropped requests so the test can wait for a retry.
        private final AtomicInteger snapshotRequestCount = new AtomicInteger();

        // Block appends to the stalled follower; its permit counter controls snapshots.
        private volatile boolean blockStalledFollower;

        // Allow one final append, then block commit updates to the future leader.
        private volatile boolean limitAppendRequestsToFutureLeader;

        private LeaderChangePacketFilter(InternalSerializationService serializationService,
                                         Address futureLeader, Address stalledFollower) {
            super(serializationService);
            this.futureLeader = futureLeader;
            this.stalledFollower = stalledFollower;
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            // Leave unrelated operations unchanged.
            if (factory != F_ID) {
                return Action.ALLOW;
            }

            if (blockStalledFollower && stalledFollower.equals(endpoint)) {
                // Force the stalled follower to catch up through a snapshot.
                if (type == APPEND_REQUEST_OP) {
                    return Action.DROP;
                }
                if (type == INSTALL_SNAPSHOT_REQUEST_OP) {
                    snapshotRequestCount.incrementAndGet();

                    // Allow metadata and one data chunk, then leave the transfer incomplete.
                    return snapshotRequestsToStalledFollowerToAllow.getAndUpdate(count -> Math.max(0, count - 1)) > 0
                            ? Action.ALLOW : Action.DROP;
                }
            }

            if (limitAppendRequestsToFutureLeader && type == APPEND_REQUEST_OP && futureLeader.equals(endpoint)) {
                // Give the future leader the final entry but not its commit update.
                return appendRequestsToFutureLeaderToAllow.getAndUpdate(count -> Math.max(0, count - 1)) > 0
                        ? Action.ALLOW : Action.DROP;
            }

            return Action.ALLOW;
        }
    }
}
