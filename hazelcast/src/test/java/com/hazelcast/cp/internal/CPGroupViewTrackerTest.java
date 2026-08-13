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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the cache lifecycle of {@link CPGroupViewTracker}: how a known leader
 * is stored, updated and invalidated.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPGroupViewTrackerTest {

    private CPGroupViewTracker tracker;
    private CPGroupId groupId;
    private CPMemberInfo leader;
    private CPMemberInfo follower;

    @Before
    public void setup() throws Exception {
        NodeEngine nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(mock(ILogger.class));
        tracker = new CPGroupViewTracker(nodeEngine);

        groupId = new RaftGroupId("test", 1, 1);
        leader = new CPMemberInfo(UUID.randomUUID(), new Address("127.0.0.1", 5701), false);
        follower = new CPMemberInfo(UUID.randomUUID(), new Address("127.0.0.1", 5702), false);
    }

    @Test
    public void when_leaderIsSet_then_itIsCached() {
        tracker.setLastKnownLeader(groupId, leader);

        assertEquals(leader, tracker.getLastKnownLeader(groupId));
    }

    @Test
    public void when_leaderIsSetToNull_then_cachedEntryIsRemoved() {
        tracker.setLastKnownLeader(groupId, leader);
        assertEquals(leader, tracker.getLastKnownLeader(groupId));

        tracker.setLastKnownLeader(groupId, null);

        assertNull(tracker.getLastKnownLeader(groupId));
    }

    @Test
    public void when_newLeaderIsSet_then_oldLeaderIsReplaced() {
        tracker.setLastKnownLeader(groupId, leader);
        assertEquals(leader, tracker.getLastKnownLeader(groupId));

        tracker.setLastKnownLeader(groupId, follower);

        assertEquals(follower, tracker.getLastKnownLeader(groupId));
    }

    @Test
    public void when_knownLeaderIsRemovedAsCPMember_then_cachedEntryIsRemoved() {
        tracker.setLastKnownLeader(groupId, leader);
        assertEquals(leader, tracker.getLastKnownLeader(groupId));

        tracker.onCPMemberRemoved(leader);

        assertNull(tracker.getLastKnownLeader(groupId));
    }

    @Test
    public void when_nonLeaderCPMemberIsRemoved_then_cachedEntryIsKept() {
        tracker.setLastKnownLeader(groupId, leader);
        assertEquals(leader, tracker.getLastKnownLeader(groupId));

        tracker.onCPMemberRemoved(follower);

        assertEquals(leader, tracker.getLastKnownLeader(groupId));
    }

    @Test
    public void when_groupIsDestroyed_then_cachedEntryIsRemoved() {
        tracker.setLastKnownLeader(groupId, leader);
        assertEquals(leader, tracker.getLastKnownLeader(groupId));

        tracker.onCPGroupDestroyed(groupId);

        assertNull(tracker.getLastKnownLeader(groupId));
    }

    @Test
    public void when_trackerIsReset_then_allCachedEntriesAreRemoved() {
        tracker.setLastKnownLeader(groupId, leader);
        assertEquals(leader, tracker.getLastKnownLeader(groupId));

        tracker.reset();

        assertNull(tracker.getLastKnownLeader(groupId));
    }
}
