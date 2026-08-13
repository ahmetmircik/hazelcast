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

package com.hazelcast.cp.internal.datastructures.snapshot;

import com.hazelcast.internal.serialization.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Bounded, per-group {@link DeltaSink} implementation. The Raft thread
 * appends {@link DeltaRecord}s during apply (O(1) per op); a snapshot worker
 * drains them off-thread into the durable incremental store. The buffer is
 * bounded by {@code capacity}; the coordinator must check {@link #isFull()}
 * and apply the snapshot-lag backpressure (reject new appends) before the
 * buffer can overflow.
 * <p>
 * The op index of emitted records is set with {@link #beginBatch(long)} by
 * the coordinator when it opens the {@link DeltaContext}; it must equal the
 * context's index so that the durable snapshot index {@code S} can advance by
 * batch.
 */
public final class SnapshotDeltaAccumulator implements DeltaSink {

    private final int capacity;
    private final List<DeltaRecord> pending = new ArrayList<>();

    private long currentOpIndex;

    public SnapshotDeltaAccumulator(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1, got: " + capacity);
        }
        this.capacity = capacity;
    }

    /**
     * Sets the Raft op index for records appended until the next call.
     * Called by the coordinator before applying the entry.
     */
    public void beginBatch(long opIndex) {
        this.currentOpIndex = opIndex;
    }

    @Override
    public void put(String serviceName, String objectName, Data key, Data value, long timestamp) {
        add(new DeltaRecord(DeltaRecord.Kind.PUT, serviceName, objectName, key, value, timestamp, currentOpIndex));
    }

    @Override
    public void delete(String serviceName, String objectName, Data key) {
        add(new DeltaRecord(DeltaRecord.Kind.DELETE, serviceName, objectName, key, null, -1L, currentOpIndex));
    }

    @Override
    public void removeAll(String serviceName, String objectName) {
        add(new DeltaRecord(DeltaRecord.Kind.REMOVE_ALL, serviceName, objectName, null, null, -1L, currentOpIndex));
    }

    private synchronized void add(DeltaRecord record) {
        if (pending.size() >= capacity) {
            throw new IllegalStateException("Delta buffer is full (capacity=" + capacity
                    + "); snapshot lag backpressure must reject new appends before this happens");
        }
        pending.add(record);
    }

    /**
     * Removes and returns all accumulated records. Called by the snapshot
     * worker off-thread.
     */
    public synchronized List<DeltaRecord> drain() {
        List<DeltaRecord> drained = new ArrayList<>(pending);
        pending.clear();
        return drained;
    }

    /**
     * Re-queues records that a failed drain could not persist, so they are
     * not lost and are retried on the next drain.
     */
    public synchronized void requeue(List<DeltaRecord> records) {
        pending.addAll(0, records);
    }

    public synchronized boolean isFull() {
        return pending.size() >= capacity;
    }

    public synchronized int size() {
        return pending.size();
    }
}
