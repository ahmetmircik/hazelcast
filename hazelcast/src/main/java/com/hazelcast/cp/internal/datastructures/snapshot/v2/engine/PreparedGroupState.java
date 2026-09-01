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

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.BaseRef;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.Hash256;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ReservationMetadata;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.internal.util.UuidUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Off-thread follower restore preparation tuple.
 */
public record PreparedGroupState(
    UUID transferId,
    long groupIncarnation,
    RaftEndpoint leaderEndpoint,
    int leaderTerm,
    long snapshotIndex,
    int snapshotTerm,
    List<RaftEndpoint> membership,
    long membershipLogIndex,
    Map<Short, Object> liveRoots,
    SnapshotRepresentation receivedSealedSnapshotRep,
    BaseRef foldedLocalBaseRef,
    ActiveDeltaBuffer cleanActiveBuffer,
    DeltaSegmentStore cleanDeltaStore,
    Hash256 representationDigest
) {

    private static final int DEFAULT_BUFFER_SIZE = 100;

    public PreparedGroupState(
            long snapshotIndex,
            int snapshotTerm,
            long groupIncarnation,
            long membershipLogIndex,
            List<RaftEndpoint> membership,
            Hash256 representationDigest,
            SnapshotRepresentation snapshotRepresentation,
            Map<Short, Object> liveRoots,
            List<ReservationMetadata> unreleasedReservations,
            long snapshotFloor
    ) {
        this(
            UuidUtil.newUnsecureUUID(),
            groupIncarnation,
            null,
            snapshotTerm,
            snapshotIndex,
            snapshotTerm,
            membership != null ? membership : Collections.emptyList(),
            membershipLogIndex,
            liveRoots != null ? liveRoots : Collections.emptyMap(),
            snapshotRepresentation,
            snapshotRepresentation != null ? snapshotRepresentation.baseRef() : BaseRef.empty(),
            new ActiveDeltaBuffer(DEFAULT_BUFFER_SIZE),
            new DeltaSegmentStore(1L),
            representationDigest
        );
    }

    public Map<Short, Object> liveContainers() {
        return liveRoots;
    }

    public SnapshotRepresentation snapshotRepresentation() {
        return receivedSealedSnapshotRep;
    }

    public long snapshotFloor() {
        return receivedSealedSnapshotRep != null && receivedSealedSnapshotRep.header() != null
                ? receivedSealedSnapshotRep.header().snapshotFloor() : 0L;
    }

    public List<ReservationMetadata> unreleasedReservations() {
        return receivedSealedSnapshotRep != null && receivedSealedSnapshotRep.header() != null
                ? receivedSealedSnapshotRep.header().unreleasedReservations() : Collections.emptyList();
    }
}
