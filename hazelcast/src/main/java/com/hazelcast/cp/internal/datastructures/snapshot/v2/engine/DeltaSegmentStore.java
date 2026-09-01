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

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Fixed-capacity circular ring directory store for {@link FrozenDeltaSegment} instances.
 */
public final class DeltaSegmentStore {

    public static final int RING_CAPACITY = 1024;
    private static final int RING_MASK = RING_CAPACITY - 1;

    private final AtomicReferenceArray<FrozenDeltaSegment> ring;
    private final AtomicLong currentTailSeq;
    private final AtomicLong reclamationFloor;

    public DeltaSegmentStore(long initialSequence) {
        this.ring = new AtomicReferenceArray<>(RING_CAPACITY);
        this.currentTailSeq = new AtomicLong(initialSequence);
        this.reclamationFloor = new AtomicLong(initialSequence);
    }

    public DeltaSegmentStore() {
        this(1L);
    }

    public int availableSlots() {
        long tail = currentTailSeq.get();
        long floor = reclamationFloor.get();
        long uncompacted = tail - floor;
        int available = RING_CAPACITY - (int) uncompacted;
        return Math.max(0, available);
    }

    public long currentTailSequence() {
        return currentTailSeq.get();
    }

    public long reclamationFloor() {
        return reclamationFloor.get();
    }

    public void advanceReclamationFloor(long newFloor) {
        long prev;
        do {
            prev = reclamationFloor.get();
            if (newFloor <= prev) {
                return;
            }
        } while (!reclamationFloor.compareAndSet(prev, newFloor));
    }

    public void appendSegment(FrozenDeltaSegment seg) {
        long seq = seg.sequence();
        int slot = (int) (seq & RING_MASK);
        FrozenDeltaSegment existing = ring.get(slot);

        if (existing != null && existing.sequence() >= reclamationFloor.get()) {
            throw new IllegalStateException("Ring directory slot occupied at seq=" + seq
                    + " by active sequence=" + existing.sequence()
                    + ", reclamationFloor=" + reclamationFloor.get());
        }

        ring.lazySet(slot, seg);
        currentTailSeq.set(seq);
    }

    public void append(FrozenDeltaSegment seg) {
        appendSegment(seg);
    }

    public void clear() {
        for (int i = 0; i < RING_CAPACITY; i++) {
            ring.set(i, null);
        }
        currentTailSeq.set(1L);
        reclamationFloor.set(1L);
    }

    public FrozenDeltaSegment getSegment(long seq) {
        if (seq < reclamationFloor.get() || seq > currentTailSeq.get()) {
            return null;
        }
        int slot = (int) (seq & RING_MASK);
        FrozenDeltaSegment seg = ring.get(slot);
        if (seg != null && seg.sequence() == seq) {
            return seg;
        }
        return null;
    }

    public List<FrozenDeltaSegment> acquireRange(long firstSeq, long lastSeq) {
        if (firstSeq > lastSeq) {
            return List.of();
        }
        if (firstSeq < reclamationFloor.get()) {
            throw new IllegalStateException("Requested firstSeq " + firstSeq
                    + " is below reclamationFloor " + reclamationFloor.get());
        }
        if (lastSeq > currentTailSeq.get()) {
            throw new IllegalStateException("Requested lastSeq " + lastSeq
                    + " exceeds currentTailSeq " + currentTailSeq.get());
        }

        List<FrozenDeltaSegment> segments = new ArrayList<>((int) (lastSeq - firstSeq + 1));
        for (long s = firstSeq; s <= lastSeq; s++) {
            int slot = (int) (s & RING_MASK);
            FrozenDeltaSegment seg = ring.get(slot);
            if (seg == null || seg.sequence() != s) {
                throw new IllegalStateException("Missing or mismatched segment in ring directory for sequence " + s);
            }
            segments.add(seg);
        }
        return segments;
    }

    public void cleanRetiredSlots() {
        long floor = reclamationFloor.get();
        for (int i = 0; i < RING_CAPACITY; i++) {
            FrozenDeltaSegment seg = ring.get(i);
            if (seg != null && seg.sequence() < floor) {
                ring.compareAndSet(i, seg, null);
            }
        }
    }
}
