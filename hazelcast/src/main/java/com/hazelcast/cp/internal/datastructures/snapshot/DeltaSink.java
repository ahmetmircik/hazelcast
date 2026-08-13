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

/**
 * Bounded sink for key-level snapshot deltas. Implementations accumulate
 * {@link DeltaRecord}s produced while the Raft thread applies committed
 * entries; a snapshot worker drains them off-thread into the durable
 * incremental store.
 * <p>
 * Emission happens at the mutation point (a service store decorator), so the
 * emitted deltas equal exactly the mutations caused by committed entries, in
 * order, single-threaded. The sink is per-group and bounded: the Raft thread
 * only appends (O(1) per op) and never blocks on it.
 *
 * @see DeltaContext
 * @see SnapshotDeltaAccumulator
 */
public interface DeltaSink {

    /**
     * Records that {@code key} in {@code serviceName}/{@code objectName} was
     * associated with {@code value}.
     *
     * @param serviceName service scope (e.g. the CP map service name)
     * @param objectName  object scope (e.g. the map name)
     * @param key         the affected key
     * @param value       the new value
     * @param timestamp   leader timestamp for purge-enabled objects,
     *                    {@code -1} otherwise
     */
    void put(String serviceName, String objectName, Data key, Data value, long timestamp);

    /**
     * Records that {@code key} in {@code serviceName}/{@code objectName} was
     * removed.
     */
    void delete(String serviceName, String objectName, Data key);

    /**
     * Records that the entire object {@code objectName} (e.g. a destroyed CP
     * map) was removed from {@code serviceName}.
     */
    void removeAll(String serviceName, String objectName);
}
