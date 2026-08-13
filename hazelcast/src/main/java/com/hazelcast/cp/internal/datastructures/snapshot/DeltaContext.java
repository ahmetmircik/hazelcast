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

/**
 * The Raft-thread-scoped context under which delta emission happens. The
 * snapshot coordinator opens a context around
 * {@code raftIntegration.runOperation(op, index)} during
 * {@code applyLogEntry}; service store decorators read
 * {@link #current()} and emit into the context's {@link DeltaSink} only when
 * a context is active.
 * <p>
 * The context is deliberately NOT active during snapshot install/restore, so
 * restore never emits deltas (the durable store is seeded directly from the
 * received snapshot). It is thread-confined (a {@link ThreadLocal}); apply is
 * strictly serialized on the Raft thread, so a single context per Raft
 * thread is safe even when several CP groups share one partition thread.
 *
 * @see DeltaSink
 * @see SnapshotDeltaAccumulator
 */
public final class DeltaContext implements AutoCloseable {

    private static final ThreadLocal<DeltaContext> CURRENT = new ThreadLocal<>();

    private final DeltaSink sink;
    private final long opIndex;

    private DeltaContext(DeltaSink sink, long opIndex) {
        this.sink = sink;
        this.opIndex = opIndex;
    }

    /**
     * Opens a context for the current thread. Must be closed with
     * {@link #end()} once the op has been applied.
     *
     * @param sink    the bounded sink to emit into
     * @param opIndex the Raft index of the entry being applied
     * @return the opened context
     */
    public static DeltaContext begin(DeltaSink sink, long opIndex) {
        DeltaContext ctx = new DeltaContext(sink, opIndex);
        CURRENT.set(ctx);
        return ctx;
    }

    /**
     * Closes the context if it is the one currently open on this thread.
     */
    public void end() {
        if (CURRENT.get() == this) {
            CURRENT.remove();
        }
    }

    @Override
    public void close() {
        end();
    }

    /**
     * Returns the context open on the current thread, or {@code null} when
     * no delta emission is expected (e.g. during snapshot restore).
     */
    public static DeltaContext current() {
        return CURRENT.get();
    }

    public DeltaSink sink() {
        return sink;
    }

    public long opIndex() {
        return opIndex;
    }
}
