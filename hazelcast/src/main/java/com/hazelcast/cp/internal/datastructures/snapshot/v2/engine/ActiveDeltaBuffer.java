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

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.FrozenDeltaSegment;

/**
 * Pre-allocated fixed-capacity delta buffer executed strictly on the partition thread.
 */
public final class ActiveDeltaBuffer {

    private final int capacity;
    private final DeltaEntry[] entries;
    private int size;
    private long totalBytes;

    public ActiveDeltaBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive");
        }
        this.capacity = capacity;
        this.entries = new DeltaEntry[capacity];
    }

    public void append(DeltaEntry entry) {
        if (size >= capacity) {
            throw new IllegalStateException("ActiveDeltaBuffer capacity exceeded: " + capacity);
        }
        entries[size++] = entry;
        totalBytes += entry.estimatedSize();
    }

    public int size() {
        return size;
    }

    public long totalBytes() {
        return totalBytes;
    }

    public int capacity() {
        return capacity;
    }

    public boolean hasEntries() {
        return size > 0;
    }

    /**
     * Detaches the active delta buffer into an immutable {@link FrozenDeltaSegment} in O(1) time
     * by transferring array ownership directly without any dynamic array copying.
     */
    public FrozenDeltaSegment detach(long sequence, long fromRaftIndex, long toRaftIndex, int snapshotTerm) {
        return new FrozenDeltaSegment(sequence, fromRaftIndex, toRaftIndex, snapshotTerm, size, totalBytes, entries);
    }
}
