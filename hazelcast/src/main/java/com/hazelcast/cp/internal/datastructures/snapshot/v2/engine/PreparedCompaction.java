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

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotRepresentationId;

import java.io.Serializable;

/**
 * Immutable compaction result record containing full 7-field Lineage-CAS validation metadata.
 */
public record PreparedCompaction(
    long groupIncarnation,
    SnapshotRepresentationId expectedPublishedRepId,
    long expectedBaseGen,
    long targetSeq,
    long targetIndex,
    int targetTerm,
    long currentTailSeqAtCapture,
    long expectedFirstSeq,
    SnapshotRepresentation compactedRepresentation
) implements Serializable {}
