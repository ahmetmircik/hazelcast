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

import com.hazelcast.internal.serialization.Data;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Canonical immutable delta entry.
 */
public record DeltaEntry(
    short serviceId,
    Data objectId,
    long objectGeneration,
    byte opCode,
    Data key,
    Data value,
    long raftIndex,
    int raftOrdinal
) implements Serializable, Comparable<DeltaEntry> {

    public static final byte OP_PUT = 0x01;
    public static final byte OP_REMOVE = 0x02;
    public static final byte OP_SERVICE_STATE = 0x03;
    public static final long BASE_ESTIMATED_SIZE = 36L;

    public static final Comparator<DeltaEntry> RAFT_ORDER = Comparator
            .comparingLong(DeltaEntry::raftIndex)
            .thenComparingInt(DeltaEntry::raftOrdinal);

    @Override
    public int compareTo(DeltaEntry other) {
        return RAFT_ORDER.compare(this, other);
    }

    public long estimatedSize() {
        return BASE_ESTIMATED_SIZE
                + (objectId != null ? objectId.totalSize() : 0L)
                + (key != null ? key.totalSize() : 0L)
                + (value != null ? value.totalSize() : 0L);
    }
}
