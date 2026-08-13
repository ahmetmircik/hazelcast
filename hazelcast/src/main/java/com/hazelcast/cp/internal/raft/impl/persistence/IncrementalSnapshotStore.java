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

package com.hazelcast.cp.internal.raft.impl.persistence;

import com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord;

import java.io.IOException;
import java.util.List;

/**
 * The durable incremental snapshot store of the redesign (Option B/F): a
 * per-group store that consumes key-level {@link DeltaRecord}s produced by
 * the Raft thread and makes them durable on disk.
 * <p>
 * {@code S = durableSnapshotIndex()} is the highest op index whose state is
 * durably materialized. S advances only after the delta batch has been
 * fsync'd (group commit): the design's guarantee "log truncation only after
 * durable publish" is anchored on this method. The Raft thread only appends
 * (never blocks on disk); the snapshot worker calls {@link #flush()}
 * off-thread.
 *
 * @see FileIncrementalSnapshotStore
 */
public interface IncrementalSnapshotStore extends AutoCloseable {

    /**
     * Appends a batch of delta records with the given batch index (the
     * highest op index of the batch). The Raft thread calls this during
     * apply; records are buffered until {@link #flush()}.
     */
    void appendDeltas(List<DeltaRecord> deltas, long batchIndex) throws IOException;

    /**
     * Fsyncs the appended deltas and advances {@link #durableSnapshotIndex()}
     * to the batch index of the last appended batch. Called by the snapshot
     * worker off-thread.
     */
    void flush() throws IOException;

    /**
     * The durable snapshot index {@code S}: the highest op index whose state
     * is durably materialized.
     */
    long durableSnapshotIndex();

    void close() throws IOException;

    void delete() throws IOException;
}
