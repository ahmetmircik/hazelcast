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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.test.Accessors.getAddress;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration tests for the known-leader cache used by {@link RaftInvocation} for
 * direct-to-leader routing. Verifies that a cached leader is invalidated when it
 * shuts down, is removed from the CP group, or is no longer the leader, and that
 * invocations recover by falling back to round-robin routing.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftInvocationKnownLeaderTest extends HazelcastRaftTestSupport {

    protected HazelcastInstance[] instances;

    /**
     * A successful response can only come from the leader of the group, so the
     * responder must be learned as the known leader for direct-to-leader routing.
     */
    @Test
    public void when_invocationSucceeds_then_knownLeaderIsLearned() throws Exception {
        int nodeCount = 5;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationManager.createRaftGroup("test", nodeCount).get();

        RaftNodeImpl leaderNode = getLeaderNode(instances, groupId);
        CPMemberInfo leader = toCPMemberInfo(instances[0], leaderNode.getLocalMember());
        HazelcastInstance invoker = getFollowers(instances, leaderNode).get(0);

        for (int i = 0; i < 10; i++) {
            getRaftInvocationManager(invoker).invoke(groupId, new RaftTestApplyOp("val" + i)).get();
        }

        assertEqualsEventually(() -> getRaftService(invoker).getGroupViewTracker().getLastKnownLeader(groupId), leader);
    }

    /**
     * A stale cached leader that is still reachable but is no longer the leader must be
     * replaced with the real leader, discovered from the {@code NotLeaderException}
     * returned by the stale endpoint.
     */
    @Test
    public void when_cachedLeaderIsStale_then_invocationDiscoversNewLeader() throws Exception {
        int nodeCount = 5;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationManager.createRaftGroup("test", nodeCount).get();
        invocationManager.invoke(groupId, new RaftTestApplyOp("val")).get();

        RaftNodeImpl leaderNode = getLeaderNode(instances, groupId);
        List<HazelcastInstance> followers = getFollowers(instances, leaderNode);
        HazelcastInstance invoker = followers.get(0);
        CPMemberInfo realLeader = toCPMemberInfo(invoker, leaderNode.getLocalMember());

        // Cache a follower as the known leader of the group
        CPMemberInfo staleLeader = toCPMemberInfo(invoker, getRaftNode(followers.get(1), groupId).getLocalMember());
        getRaftService(invoker).getGroupViewTracker().setLastKnownLeader(groupId, staleLeader);

        // Invocation must recover from the stale cached leader and discover the real one
        getRaftInvocationManager(invoker).invoke(groupId, new RaftTestApplyOp("val2")).get();

        assertEqualsEventually(() -> getRaftService(invoker).getGroupViewTracker().getLastKnownLeader(groupId), realLeader);
    }

    /**
     * When the cached leader shuts down, the cache must be invalidated and invocations
     * must recover by routing to the newly elected leader.
     */
    @Test
    public void when_cachedLeaderShutsDown_then_invocationRecoversViaNewLeader() throws Exception {
        int nodeCount = 5;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationManager.createRaftGroup("test", nodeCount).get();
        invocationManager.invoke(groupId, new RaftTestApplyOp("val")).get();

        RaftNodeImpl leaderNode = getLeaderNode(instances, groupId);
        HazelcastInstance leaderInstance = getLeaderInstance(instances, groupId);
        List<HazelcastInstance> followers = getFollowers(instances, leaderNode);
        HazelcastInstance invoker = followers.get(0);
        CPMemberInfo leader = toCPMemberInfo(invoker, leaderNode.getLocalMember());

        // Cache the leader in the tracker of a follower
        getRaftService(invoker).getGroupViewTracker().setLastKnownLeader(groupId, leader);

        leaderInstance.getLifecycleService().terminate();
        assertClusterSizeEventually(nodeCount - 1, invoker);

        // Invocations must keep succeeding
        RaftInvocationManager invokerManager = getRaftInvocationManager(invoker);
        assertTrueEventually(() -> {
            try {
                invokerManager.invoke(groupId, new RaftTestApplyOp("val2")).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // The known leader must no longer be the terminated member
        assertTrueEventually(() -> {
            CPMember knownLeader = getRaftService(invoker).getGroupViewTracker().getLastKnownLeader(groupId);
            assertNotNull(knownLeader);
            assertNotEquals(leader, knownLeader);
        });
    }

    /**
     * When the cached leader is removed from the CP subsystem, the cached entry must be
     * invalidated and invocations must recover via the remaining group members.
     * The removed member stays alive and answers with a {@code NotLeaderException}
     * that does not carry a new leader UUID, so recovery depends on resetting the cache.
     */
    @Test
    public void when_cachedLeaderIsRemovedFromCPGroup_then_invocationRecovers() throws Exception {
        int cpNodeCount = 5;
        int groupSize = 3;
        instances = newInstances(cpNodeCount, groupSize, 0);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationManager.createRaftGroup("test", groupSize).get();
        invocationManager.invoke(groupId, new RaftTestApplyOp("val")).get();

        // Pick a CP member that is NOT part of the test group. It is still a reachable
        // cluster member, but it does not have a raft node for the group, so it answers
        // NotLeaderException without a leader UUID.
        Collection<CPMember> groupMembers =
                instances[0].getCPSubsystem().getCPSubsystemManagementService().getCPGroup("test").toCompletableFuture().get()
                            .members();
        HazelcastInstance removedMemberInstance = findInstanceNotInGroup(instances, groupMembers);
        assertNotNull(removedMemberInstance);
        CPMemberInfo removedMember = getRaftService(removedMemberInstance).getLocalCPMember();

        HazelcastInstance invoker = findInstanceOtherThan(instances, removedMemberInstance);
        HazelcastInstance remover = findInstanceOtherThan(instances, removedMemberInstance, invoker);
        assertNotNull(invoker);
        assertNotNull(remover);

        // Cache the removed member as the known leader of the group
        getRaftService(invoker).getGroupViewTracker().setLastKnownLeader(groupId, removedMember);

        // Remove the (alive) member from the CP subsystem
        remover.getCPSubsystem().getCPSubsystemManagementService().removeCPMember(removedMember.getUuid()).toCompletableFuture()
               .get();

        // Invocations must keep succeeding via the remaining group members
        RaftInvocationManager invokerManager = getRaftInvocationManager(invoker);
        assertTrueEventually(() -> {
            try {
                invokerManager.invoke(groupId, new RaftTestApplyOp("val2")).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // The cached leader must never point back to the removed member
        assertTrueEventually(() -> {
            CPMember knownLeader = getRaftService(invoker).getGroupViewTracker().getLastKnownLeader(groupId);
            assertNotNull(knownLeader);
            assertNotEquals(removedMember, knownLeader);
        });
    }

    private List<HazelcastInstance> getFollowers(HazelcastInstance[] instances, RaftNodeImpl leaderNode) {
        Address leaderAddress = toCPMemberInfo(instances[0], leaderNode.getLocalMember()).getAddress();
        List<HazelcastInstance> followers = new ArrayList<>();
        for (HazelcastInstance instance : instances) {
            if (!getAddress(instance).equals(leaderAddress)) {
                followers.add(instance);
            }
        }
        return followers;
    }

    private CPMemberInfo toCPMemberInfo(HazelcastInstance instance, RaftEndpoint endpoint) {
        CPMember member = getRaftService(instance).getInvocationManager().getRaftInvocationContext()
                .getCPMember(endpoint.getUuid());
        assertNotNull(member);
        return (CPMemberInfo) member;
    }

    private HazelcastInstance findInstanceNotInGroup(HazelcastInstance[] instances, Collection<CPMember> groupMembers) {
        for (HazelcastInstance instance : instances) {
            if (!groupMembers.contains(getRaftService(instance).getLocalCPMember())) {
                return instance;
            }
        }
        return null;
    }

    private HazelcastInstance findInstanceOtherThan(HazelcastInstance[] instances, HazelcastInstance... excluded) {
        for (HazelcastInstance instance : instances) {
            boolean skip = false;
            for (HazelcastInstance e : excluded) {
                if (instance == e) {
                    skip = true;
                    break;
                }
            }
            if (!skip) {
                return instance;
            }
        }
        return null;
    }
}
