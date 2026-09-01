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
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CanonicalSnapshotHeader;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.GroupSnapshotMetadata;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.RepresentationState;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotRepresentationId;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Physical snapshot representation with decoupled state machine and non-resurrecting reference counting.
 */
public final class SnapshotRepresentation {

    private final SnapshotRepresentationId id;
    private final BaseRef baseRef;
    private final List<FrozenDeltaSegment> leasedDeltas;
    private final AtomicReference<RepresentationState> state;
    private final AtomicInteger refCount;
    private volatile GroupSnapshotMetadata metadata;

    public SnapshotRepresentation(SnapshotRepresentationId id, BaseRef baseRef,
                                  List<FrozenDeltaSegment> leasedDeltas, RepresentationState initialState) {
        this.id = id;
        this.baseRef = baseRef;
        this.leasedDeltas = List.copyOf(leasedDeltas);
        this.state = new AtomicReference<>(initialState);
        this.refCount = new AtomicInteger(1);
    }

    public SnapshotRepresentation(SnapshotRepresentationId id, BaseRef baseRef,
                                  List<FrozenDeltaSegment> leasedDeltas) {
        this(id, baseRef, leasedDeltas, RepresentationState.UNSEALED);
    }

    public SnapshotRepresentationId id() {
        return id;
    }

    public BaseRef baseRef() {
        return baseRef;
    }

    public List<FrozenDeltaSegment> leasedDeltas() {
        return leasedDeltas;
    }

    public RepresentationState state() {
        return state.get();
    }

    public GroupSnapshotMetadata metadata() {
        return metadata;
    }

    public CanonicalSnapshotHeader header() {
        return metadata != null ? metadata.header() : null;
    }

    public void setMetadata(GroupSnapshotMetadata metadata) {
        this.metadata = metadata;
    }

    public boolean seal(CanonicalSnapshotHeader header) {
        if (state.compareAndSet(RepresentationState.UNSEALED, RepresentationState.SEALED)) {
            this.metadata = new GroupSnapshotMetadata(id, header, System.nanoTime(), header.computeRepresentationDigest());
            return true;
        }
        return false;
    }

    public void markSealFailed() {
        state.set(RepresentationState.SEAL_FAILED);
    }

    public SnapshotHandle acquireHandle() {
        if (state.get() == RepresentationState.RETIRED || state.get() == RepresentationState.SEAL_FAILED) {
            return null;
        }
        while (true) {
            int current = refCount.get();
            if (current <= 0) {
                return null;
            }
            if (refCount.compareAndSet(current, current + 1)) {
                if (state.get() == RepresentationState.RETIRED || state.get() == RepresentationState.SEAL_FAILED) {
                    refCount.decrementAndGet();
                    return null;
                }
                return new SnapshotHandle(this);
            }
        }
    }

    public SnapshotHandle tryAcquireHandle() {
        return acquireHandle();
    }



    public void releaseHandle() {
        int remaining = refCount.decrementAndGet();
        if (remaining == 0) {
            retire();
        }
    }

    public boolean tryReleaseHandle() {
        int remaining = refCount.decrementAndGet();
        if (remaining == 0) {
            retire();
            return true;
        }
        return false;
    }


    public void retire() {
        state.set(RepresentationState.RETIRED);
    }

    public int activeHandleCount() {
        return Math.max(0, refCount.get());
    }

    public int activeReferences() {
        return activeHandleCount();
    }
}
