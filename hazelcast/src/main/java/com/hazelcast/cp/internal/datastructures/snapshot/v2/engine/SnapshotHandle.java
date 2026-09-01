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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Independent per-consumer closeable handle for {@link SnapshotRepresentation}.
 */
public final class SnapshotHandle implements AutoCloseable {

    private final SnapshotRepresentation representation;
    private final AtomicBoolean closed;

    public SnapshotHandle(SnapshotRepresentation representation) {
        this.representation = representation;
        this.closed = new AtomicBoolean(false);
    }

    public SnapshotRepresentation representation() {
        return representation;
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            representation.releaseHandle();
        }
    }
}
