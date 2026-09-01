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
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ServiceBaseRef;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotRepresentationId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Off-thread snapshot compactor that materializes folded immutable bases and prepares
 * {@link PreparedCompaction} with full Lineage-CAS validation attributes.
 */
public final class SnapshotCompactor {

    public record CompactionRequest(
        SnapshotRepresentation baseRepresentation,
        long groupIncarnation,
        long targetSeq,
        long targetRaftIndex,
        int targetSnapshotTerm,
        long currentTailSeqAtCapture,
        long expectedFirstSeq,
        List<FrozenDeltaSegment> deltasToFold,
        Map<Short, CPServiceSnapshotCodec<?, ?>> codecRegistry,
        long newRepresentationGen
    ) {}

    @SuppressWarnings("unchecked")
    public PreparedCompaction prepareCompaction(CompactionRequest request) {
        List<DeltaEntry> allDeltas = new ArrayList<>();
        for (FrozenDeltaSegment seg : request.deltasToFold()) {
            Iterator<DeltaEntry> it = seg.iterator();
            while (it.hasNext()) {
                allDeltas.add(it.next());
            }
        }

        Map<Short, ServiceBaseRef> newServiceBases = new HashMap<>();
        SnapshotRepresentation baseRep = request.baseRepresentation();
        for (Map.Entry<Short, ServiceBaseRef> entry : baseRep.baseRef().serviceBases().entrySet()) {
            short serviceId = entry.getKey();
            CPServiceSnapshotCodec codec = request.codecRegistry().get(serviceId);
            if (codec != null) {
                Object materialized = codec.materializeBase(
                        entry.getValue().immutableServicePayload(), allDeltas.iterator());
                newServiceBases.put(serviceId, new ServiceBaseRef(serviceId, 0L, materialized));
            }
        }

        long nextBaseGen = baseRep.baseRef().baseGeneration() + 1;
        BaseRef compactedBaseRef = new BaseRef(request.targetRaftIndex(), nextBaseGen, newServiceBases);
        SnapshotRepresentationId newRepId = new SnapshotRepresentationId(
                baseRep.id().groupId(),
                request.groupIncarnation(),
                request.newRepresentationGen(),
                request.targetRaftIndex()
        );

        SnapshotRepresentation compactedRepresentation = new SnapshotRepresentation(
                newRepId, compactedBaseRef, Collections.emptyList());

        return new PreparedCompaction(
                request.groupIncarnation(),
                baseRep.id(),
                baseRep.baseRef().baseGeneration(),
                request.targetSeq(),
                request.targetRaftIndex(),
                request.targetSnapshotTerm(),
                request.currentTailSeqAtCapture(),
                request.expectedFirstSeq(),
                compactedRepresentation
        );
    }
}
