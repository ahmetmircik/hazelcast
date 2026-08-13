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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.internal.datastructures.snapshot.DeltaContext;
import com.hazelcast.cp.internal.datastructures.snapshot.DeltaRecord;
import com.hazelcast.cp.internal.datastructures.snapshot.SnapshotDeltaAccumulator;
import com.hazelcast.cp.internal.raft.impl.persistence.IncrementalSnapshotStore;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The per-group snapshot pipeline coordinator of the redesign (Option F,
 * threading-model Domain 1/2 boundary). It ties the four pieces together:
 * <ol>
 *   <li>the Raft thread opens a {@link DeltaContext} around apply
 *       ({@link #beginApply(long)}) so service store decorators emit
 *       key-level deltas into the bounded {@link SnapshotDeltaAccumulator}
 *       (O(1) per op, never blocking);</li>
 *   <li>when the accumulator passes the drain threshold, ONE drain task is
 *       submitted to the worker executor (at most one in flight —
 *       coalescing, bounded queue);</li>
 *   <li>the worker appends the drained batch to the durable
 *       {@link IncrementalSnapshotStore} and {@link IncrementalSnapshotStore#flush() flush}es,
 *       advancing the durable snapshot index S off the Raft thread;</li>
 *   <li>a completion/failure callback runs on the Raft thread (the real
 *       wiring passes {@code raftIntegration.submit(...)}, preserving
 *       happens-before with the group's other tasks).</li>
 * </ol>
 * Failure handling: if the durable append fails, the drained records are
 * re-queued into the sink (never lost) and the failure notifier runs; the
 * next apply retries the drain.
 */
public final class SnapshotPipeline implements AutoCloseable {

    private static final int DEFAULT_DRAIN_THRESHOLD = 128;

    private final SnapshotDeltaAccumulator sink;
    private final IncrementalSnapshotStore store;
    private final Executor worker;
    private final Runnable completionNotifier;
    private final Runnable failureNotifier;
    private final int drainThreshold;

    private final AtomicBoolean drainScheduled = new AtomicBoolean();
    private volatile long lastAppliedIndex;

    public SnapshotPipeline(IncrementalSnapshotStore store, Executor worker,
                            Runnable completionNotifier, Runnable failureNotifier) {
        this(store, worker, completionNotifier, failureNotifier, DEFAULT_DRAIN_THRESHOLD);
    }

    public SnapshotPipeline(IncrementalSnapshotStore store, Executor worker,
                            Runnable completionNotifier, Runnable failureNotifier,
                            int drainThreshold) {
        this.store = store;
        this.worker = worker;
        this.completionNotifier = completionNotifier;
        this.failureNotifier = failureNotifier;
        this.drainThreshold = drainThreshold;
        this.sink = new SnapshotDeltaAccumulator(Integer.MAX_VALUE);
    }

    /**
     * Opens the delta context for applying the entry at {@code opIndex}.
     * Must be called on the Raft thread; the returned context must be
     * closed (try-with-resources) after the operation was applied.
     */
    public DeltaContext beginApply(long opIndex) {
        this.lastAppliedIndex = opIndex;
        sink.beginBatch(opIndex);
        return DeltaContext.begin(sink, opIndex);
    }

    /**
     * Called on the Raft thread after the entry was applied. Schedules a
     * worker drain when the pending delta count reached the threshold and no
     * drain is already in flight.
     */
    public void onApplied() {
        if (sink.size() >= drainThreshold && drainScheduled.compareAndSet(false, true)) {
            worker.execute(this::drain);
        }
    }

    private void drain() {
        List<DeltaRecord> deltas = sink.drain();
        try {
            if (!deltas.isEmpty()) {
                store.appendDeltas(deltas, lastAppliedIndex);
                store.flush();
            }
            completionNotifier.run();
        } catch (IOException e) {
            sink.requeue(deltas);
            failureNotifier.run();
        } finally {
            drainScheduled.set(false);
        }
    }

    /**
     * The durable snapshot index {@code S} (memory tier read).
     */
    public long durableSnapshotIndex() {
        return store.durableSnapshotIndex();
    }

    /**
     * Number of deltas waiting to be drained (memory-growth visibility).
     */
    public int pendingDeltaCount() {
        return sink.size();
    }

    public boolean isDrainInFlight() {
        return drainScheduled.get();
    }

    @Override
    public void close() throws IOException {
        store.close();
    }
}
