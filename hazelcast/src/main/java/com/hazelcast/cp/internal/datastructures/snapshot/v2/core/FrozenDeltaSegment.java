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

package com.hazelcast.cp.internal.datastructures.snapshot.v2.core;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Immutable frozen delta segment with encapsulated private entry array and read-only iterator.
 */
public final class FrozenDeltaSegment {

    private final long sequence;
    private final long fromRaftIndex;
    private final long toRaftIndex;
    private final int snapshotTerm;
    private final int entryCount;
    private final long byteSize;
    private final DeltaEntry[] entries;

    public FrozenDeltaSegment(long sequence, long fromRaftIndex, long toRaftIndex,
                              int snapshotTerm, int entryCount, long byteSize, DeltaEntry[] entries) {
        this.sequence = sequence;
        this.fromRaftIndex = fromRaftIndex;
        this.toRaftIndex = toRaftIndex;
        this.snapshotTerm = snapshotTerm;
        this.entryCount = entryCount;
        this.byteSize = byteSize;
        this.entries = entries;
    }

    public long sequence() {
        return sequence;
    }

    public long fromRaftIndex() {
        return fromRaftIndex;
    }

    public long toRaftIndex() {
        return toRaftIndex;
    }

    public int snapshotTerm() {
        return snapshotTerm;
    }

    public int entryCount() {
        return entryCount;
    }

    public long byteSize() {
        return byteSize;
    }

    public DeltaEntry getEntry(int index) {
        if (index < 0 || index >= entryCount) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + entryCount);
        }
        return entries[index];
    }

    public Iterator<DeltaEntry> iterator() {
        return new Iterator<>() {
            private int cursor;

            @Override
            public boolean hasNext() {
                return cursor < entryCount;
            }

            @Override
            public DeltaEntry next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return entries[cursor++];
            }
        };
    }
}
