/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.util.MemoryInfoAccessor;

/**
 * Factory for {@link EvictionOperator} instances.
 */
public final class EvictionOperatorFactory {

    private EvictionOperatorFactory() {
    }

    public static EvictionOperator create(MapServiceContext mapServiceContext) {
        final EvictionOperator evictionOperator = new EvictionOperatorImpl();
        final EvictableChecker evictableChecker = new EvictableCheckerImpl(mapServiceContext);
        evictionOperator.setEvictableChecker(evictableChecker);
        evictionOperator.setMapServiceContext(mapServiceContext);

        return evictionOperator;
    }

    public static EvictionOperator create(MemoryInfoAccessor memoryInfoAccessor, MapServiceContext mapServiceContext) {
        final EvictionOperator evictionOperator = new EvictionOperatorImpl();
        final EvictableChecker evictableChecker = new EvictableCheckerImpl(memoryInfoAccessor, mapServiceContext);
        evictionOperator.setEvictableChecker(evictableChecker);
        evictionOperator.setMapServiceContext(mapServiceContext);

        return evictionOperator;
    }
}
