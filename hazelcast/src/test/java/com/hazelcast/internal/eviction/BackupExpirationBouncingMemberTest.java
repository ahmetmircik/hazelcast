/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.eviction;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.util.StringUtil;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.map.BackupExpirationTest.getTotalEntryCount;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
@Repeat(10)
public class BackupExpirationBouncingMemberTest extends HazelcastTestSupport {

    String mapName = "test";
    int maxIdleSeconds = 2;
    int backupCount = 3;
    int keySpace = 1000;

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .useTerminate()
            .build();

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getMapConfig(mapName)
                .setMaxIdleSeconds(maxIdleSeconds)
                .setBackupCount(backupCount);
        return config;
    }

    @After
    public void tearDown() throws Exception {
        bounceMemberRule.getFactory().shutdownAll();
    }

    @Test(timeout = 10 * 60 * 1000)
    public void backups_should_be_empty_after_expiration() {
        Runnable[] methods = new Runnable[2];
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        methods[0] = new Get(testDriver);
        methods[1] = new Set(testDriver);

        bounceMemberRule.testRepeatedly(methods, 20);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                AtomicReferenceArray<HazelcastInstance> members = bounceMemberRule.getMembers();
                AtomicReferenceArray<HazelcastInstance> testDrivers = bounceMemberRule.getTestDrivers();

                assertSize(members);
                assertSize(testDrivers);
            }

            private String memberCountMsg(AtomicReferenceArray<HazelcastInstance> members) {
                int length = members.length();
                String msg = "Number of members " + length;
                for (int i = 0; i < length; i++) {
                    HazelcastInstance node = members.get(i);
                    assert node != null;
                    if (node.getLifecycleService().isRunning()
                            && node.getCluster().getClusterState() != ClusterState.PASSIVE) {
                        msg += node.toString() + ", ";
                    }
                }
                return msg;
            }

            private void assertSize(AtomicReferenceArray<HazelcastInstance> members) {
                int length = members.length();
                for (int i = 0; i < length; i++) {
                    HazelcastInstance node = members.get(i);
                    assert node != null;
                    if (node.getLifecycleService().isRunning()
                            && node.getCluster().getClusterState() != ClusterState.PASSIVE) {

                        ClusterState clusterState = node.getCluster().getClusterState();
                        IMap map = node.getMap(mapName);

                        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
                        MapService mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME);
                        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
                        long lastStartMillis = getLastStartMillis(mapServiceContext.getExpirationManager());
                        long lastEndMillis = getLastEndMillis(mapServiceContext.getExpirationManager());

                        String backupPartitionsOnThisMember = "";
                        InternalPartition[] internalPartitions = ((InternalPartitionServiceImpl) nodeEngineImpl.getPartitionService()).getInternalPartitions();
                        for (InternalPartition partition : internalPartitions) {
                            if (!partition.isLocal()) {
                                backupPartitionsOnThisMember += partition.getPartitionId() + ", ";
                            }
                        }

                        LocalMapStats localMapStats = map.getLocalMapStats();
                        String msg = "Failed on node: %s, current cluster state is: %s, "
                                + "members: %s, "
                                + "backupPartitionsOnThisMember: %s, "
                                + "ownedEntryCount: %d, backupEntryCount: %d, "
                                + "expiredRecordsCleanerTask=[now: %s, lastStart: %s, lastEnd: %s]";

                        String formattedMsg = String.format(msg,
                                node,
                                clusterState.toString(),
                                memberCountMsg(members),
                                backupPartitionsOnThisMember,
                                localMapStats.getOwnedEntryCount(),
                                localMapStats.getBackupEntryCount(),
                                StringUtil.timeToStringFriendly(System.currentTimeMillis()),
                                StringUtil.timeToStringFriendly(lastStartMillis),
                                StringUtil.timeToStringFriendly(lastEndMillis));

                        assertEquals(formattedMsg, 0, getTotalEntryCount(localMapStats));
                    }
                }
            }
        }, 300);
    }

    private class Get implements Runnable {

        private final HazelcastInstance hz;

        public Get(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                hz.getMap(mapName).get(i);
            }
        }
    }

    private class Set implements Runnable {

        private final HazelcastInstance hz;

        public Set(HazelcastInstance hz) {
            this.hz = hz;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                hz.getMap(mapName).set(i, i);
            }
        }
    }

    private long getLastStartMillis(ExpirationManager expirationManager) {
        return expirationManager.task.lastStartMillis;
    }

    private long getLastEndMillis(ExpirationManager expirationManager) {
        return expirationManager.task.lastEndMillis;
    }
}
