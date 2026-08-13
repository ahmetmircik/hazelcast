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

import com.hazelcast.cp.CPGroupId;

/**
 * Central place for the snapshot-redesign enablement properties (Option F).
 * Everything is gated on a single system property so the wiring is
 * behavior-neutral by default:
 * <ul>
 *   <li>{@code -Dhazelcast.cp.snapshot.enabled=true} activates the file-backed
 *       {@code RaftStateStore}, the per-group delta pipeline
 *       ({@code RaftIntegration.getSnapshotPipelineFactory}) and the
 *       {@code DeltaEmittingCPMapStore} wrapping;</li>
 *   <li>{@code -Dhazelcast.cp.snapshot.dir=...} selects the root directory
 *       (default: {@code <java.io.tmpdir>/hazelcast-cp-snapshot}); the
 *       platform replaces this with its configured data directory.</li>
 * </ul>
 */
public final class SnapshotPipelineConfig {

    public static final String SNAPSHOT_ENABLED_PROPERTY = "hazelcast.cp.snapshot.enabled";
    public static final String SNAPSHOT_DIR_PROPERTY = "hazelcast.cp.snapshot.dir";
    public static final String SNAPSHOT_LAG_MULTIPLIER_PROPERTY = "hazelcast.cp.snapshot.lag.multiplier";

    /** Default lag cap: the snapshot cadence times this multiplier. */
    private static final int DEFAULT_SNAPSHOT_LAG_MULTIPLIER = 4;

    /**
     * Lazy read so tests (and operators) can toggle the feature at runtime.
     */
    public static boolean isSnapshotEnabled() {
        return Boolean.getBoolean(SNAPSHOT_ENABLED_PROPERTY);
    }

    /**
     * The hard bound on how far the durable/in-flight snapshot may lag the
     * commit index before new appends are rejected (the design's bounded
     * backpressure, Option F §3.1). Returns {@code cadence * multiplier}
     * (default multiplier 4), or a non-positive value when the property is
     * set to disable the explicit cap (the fixed-capacity Raft ringbuffer
     * then remains the implicit bound).
     */
    public static long maxSnapshotLag(long commitIndexAdvanceCountToSnapshot) {
        int multiplier = Integer.getInteger(SNAPSHOT_LAG_MULTIPLIER_PROPERTY, DEFAULT_SNAPSHOT_LAG_MULTIPLIER);
        return multiplier * commitIndexAdvanceCountToSnapshot;
    }

    public static final String SNAPSHOT_DIR = System.getProperty(
            SNAPSHOT_DIR_PROPERTY,
            System.getProperty("java.io.tmpdir") + "/hazelcast-cp-snapshot");

    private SnapshotPipelineConfig() {
    }

    /**
     * A filesystem-safe directory name for a CP group.
     */
    public static String groupDirName(CPGroupId groupId) {
        return groupId.getName().replaceAll("[^A-Za-z0-9._-]", "_") + "-" + groupId.getId();
    }
}
