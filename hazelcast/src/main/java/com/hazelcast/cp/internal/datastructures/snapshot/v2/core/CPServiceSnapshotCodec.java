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

import java.io.IOException;
import java.util.Iterator;

/**
 * Bounded streaming snapshot codec contract implemented by CP services.
 */
public interface CPServiceSnapshotCodec<B, D> {
    short serviceId();
    short codecVersion();
    double conservativeWorkspaceMultiplier();
    long maxRestoreWorkspaceBytes(long declaredBytes);
    B createEmptyBase();
    B materializeBase(B existingBase, Iterator<D> deltas);
    void applyDeltaToLive(Object liveContainer, D delta);
    Object swapLiveContainer(Object currentContainer, Object newContainer);
    void writeBase(B base, SnapshotFrameSink sink) throws IOException;
    B readBase(SnapshotFrameSource source, long declaredBytes, ServiceWorkspace workspace) throws IOException;
}
