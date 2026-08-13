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

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPGroupsSnapshot;
import com.hazelcast.cp.CPMember;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
* Tracks a view of CP group information from 1 cluster member's perspective.
 */
public class CPGroupViewTracker {

    protected final ILogger logger;

    // maintains a "last known" view of group membership states
    protected final Map<CPGroupId, GroupState> groupIdToState = new ConcurrentHashMap<>();

    public CPGroupViewTracker(NodeEngine nodeEngine) {
        this.logger = nodeEngine.getLogger(getClass());
    }

    /**
     * Callback to update the last known leader of a CP Group, usually called
     * when a {@link com.hazelcast.cp.exception.NotLeaderException} is received
     * and a new leader is provided.
     *
     * @param groupId  the {@link CPGroupId} to update the known leader for
     * @param cpLeader the {@link CPMember} of the new known leader of this group
     */
    public void setLastKnownLeader(CPGroupId groupId, @Nullable CPMember cpLeader) {
        if (cpLeader == null) {
            // treat as though we have lost track of the group, wait for publication by new leader to update
            groupIdToState.remove(groupId);
            return;
        }

        // Provide a temporary entry if there's no existing state, or if the provided leader does not match our records
        groupIdToState.compute(groupId, (id, existingState) -> {
            if (existingState == null || existingState.leader == null || !existingState.leader.equals(cpLeader)) {
                // provide a temporary view of the leader only for now, wait for publication by leader to update
                return new GroupState(cpLeader, Collections.emptySet(), -1);
            }
            // the leader provided matches our records, no further action
            return existingState;
        });
    }

    /**
     * Retrieves the {@link CPMember} of the last known leader for the
     * provided {@link CPGroupId}.
     *
     * @param groupId the ID of the group to look up the leader of
     * @return        the {@link CPMember} of leader of this group if known, else {@code null}
     */
    public CPMember getLastKnownLeader(CPGroupId groupId) {
        GroupState state = groupIdToState.get(groupId);
        return state == null ? null : state.leader;
    }

    /**
     * Internal callback used to update the membership view when a CP member is removed.
     *
     * @param member the {@link CPMember} of the member being removed
     */
    public void onCPMemberRemoved(CPMember member) {
        groupIdToState.entrySet().removeIf(entry -> {
            if (member.equals(entry.getValue().getLeader())) {
                // remove entire group state and wait for new leader to update
                return true;
            }
            entry.getValue().getFollowers().remove(member);
            return false;
        });
    }

    public void onCPGroupDestroyed(CPGroupId groupId) {
        groupIdToState.remove(groupId);
    }

    /**
     * Clears all currently tracked CP group information.
     */
    public void reset() {
        logger.fine("Resetting known CP group view information");
        groupIdToState.clear();
    }

    /**
     * Internal, mutable variant of {@link CPGroupsSnapshot.GroupInfo}
     */
    static class GroupState {
        private final Set<CPMember> followers;
        private final int term;
        private final CPMember leader;

        GroupState(CPMember leader, Set<CPMember> followers, int term) {
            this.leader = leader;
            this.followers = followers;
            this.term = term;
        }

        GroupState(CPGroupsSnapshot.GroupInfo groupInfo) {
            this.leader = groupInfo.leader();
            this.followers = groupInfo.followers();
            this.term = groupInfo.term();
        }

        public Set<CPMember> getFollowers() {
            return followers;
        }

        public int getTerm() {
            return term;
        }

        public CPMember getLeader() {
            return leader;
        }

        public CPGroupsSnapshot.GroupInfo toGroupInfo() {
            return new CPGroupsSnapshot.GroupInfo(leader, followers, term);
        }

        @Override
        public String toString() {
            return "GroupState {term=" + term + ", "
                    + "leader=" + leader + ", "
                    + "followers=" + followers + "}";
        }
    }
}
