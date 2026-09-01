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
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CPServiceSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.Hash256;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ServiceBaseRef;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ServiceWorkspace;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSource;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotRepresentationId;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Off-thread follower restore worker that verifies frame hashes, rebuilds immutable base trees,
 * and prepares the atomic {@link PreparedGroupState} for partition-thread switchover.
 */
public final class FollowerRestoreWorker {

    public PreparedGroupState restoreFromStream(
            CanonicalSnapshotHeader header,
            SnapshotFrameSource source,
            Map<Short, CPServiceSnapshotCodec<?, ?>> codecRegistry,
            long localIncarnation,
            long representationGen) throws IOException {

        Hash256 computedDigest = header.computeRepresentationDigest();
        if (!computedDigest.constantTimeEquals(header.representationDigest())) {
            throw new IllegalStateException("Corrupt snapshot stream: computed digest does not match header");
        }

        Map<Short, ServiceBaseRef> restoredServiceBases = new HashMap<>();
        Map<Short, Object> liveContainers = new HashMap<>();

        for (Map.Entry<Short, Short> entry : header.codecDirectory().entrySet()) {
            short serviceId = entry.getKey();
            @SuppressWarnings("unchecked")
            CPServiceSnapshotCodec codec = codecRegistry.get(serviceId);
            if (codec == null) {
                throw new IllegalStateException("Missing codec for serviceId=" + serviceId);
            }

            long maxWorkspace = codec.maxRestoreWorkspaceBytes(header.declaredDataBytes());
            ServiceWorkspace workspace = new ServiceWorkspace(serviceId, maxWorkspace);

            Object serviceBasePayload = codec.readBase(source, header.declaredDataBytes(), workspace);
            restoredServiceBases.put(serviceId, new ServiceBaseRef(serviceId, 0L, serviceBasePayload));
            liveContainers.put(serviceId, serviceBasePayload);
        }

        BaseRef groupBaseRef = new BaseRef(header.snapshotIndex(), 1L, restoredServiceBases);
        SnapshotRepresentationId repId = new SnapshotRepresentationId(
                header.groupId(), localIncarnation, representationGen, header.snapshotIndex());
        SnapshotRepresentation physicalRep = new SnapshotRepresentation(repId, groupBaseRef, Collections.emptyList());
        physicalRep.seal(header);

        return new PreparedGroupState(
                header.snapshotIndex(),
                header.snapshotTerm(),
                header.groupIncarnation(),
                header.membershipLogIndex(),
                header.members(),
                header.representationDigest(),
                physicalRep,
                liveContainers,
                header.unreleasedReservations(),
                header.snapshotFloor()
        );
    }
}
