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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable base image reference.
 */
public record BaseRef(
    long baseGeneration,
    long baseIndex,
    int baseTerm,
    long totalEstimatedBytes,
    Map<Short, ServiceBaseRef> serviceBases
) implements Serializable {

    public BaseRef {
        serviceBases = Collections.unmodifiableMap(serviceBases != null ? serviceBases : Map.of());
    }

    public BaseRef(long baseIndex, long baseGeneration, Map<Short, ServiceBaseRef> serviceBases) {
        this(baseGeneration, baseIndex, 1, 0L, serviceBases);
    }

    public static BaseRef empty() {
        return new BaseRef(0L, 0L, 0, 0L, Collections.emptyMap());
    }
}
