/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.core.EntryEventType.INVALIDATION;


/**
 * Contains common functionality of a {@code NearCacheInvalidator}
 */
abstract class AbstractNearCacheInvalidator implements NearCacheInvalidator {

    protected final NodeEngine nodeEngine;
    protected final EventService eventService;
    protected final SerializationService serializationService;
    protected final InvalidationObserver observer;
    private final String localMemberUuid;

    AbstractNearCacheInvalidator(MapServiceContext mapServiceContext) {
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.localMemberUuid = nodeEngine.getLocalMember().getUuid();
        this.eventService = nodeEngine.getEventService();
        this.serializationService = nodeEngine.getSerializationService();
        this.observer = new InvalidationObserver(mapServiceContext);
    }

    @Override
    public final void invalidate(Data key, String mapName, String sourceUuid) {
        assert key != null;
        assert mapName != null;
        assert sourceUuid != null;

        invalidateInternal(newKeyInvalidation(key, mapName, sourceUuid), getPartitionId(key));

    }

    @Override
    public final void clear(String mapName, String sourceUuid) {
        assert mapName != null;
        assert sourceUuid != null;

        invalidateInternal(newClearInvalidation(mapName, sourceUuid), getPartitionId(mapName));
    }

    protected final Invalidation newKeyInvalidation(Data key, String mapName, String sourceUuid) {
        long sequence = observer.nextSequence(mapName);
        String uuid = pickUuid(sourceUuid, localMemberUuid);
        return new SingleNearCacheInvalidation(toHeapData(key), mapName, uuid, sequence);
    }

    protected final Invalidation newClearInvalidation(String mapName, String sourceUuid) {
        long sequence = observer.nextSequence(mapName);
        String uuid = pickUuid(sourceUuid, localMemberUuid);
        return new ClearNearCacheInvalidation(mapName, uuid, sequence);
    }

    // TODO: maybe a switch required if eventual-consistency is configurable.
    private static String pickUuid(String sourceUuid, String localMemberUuid) {
        return localMemberUuid;
    }

    protected final int getPartitionId(Object o) {
        return nodeEngine.getPartitionService().getPartitionId(o);
    }

    protected abstract void invalidateInternal(Invalidation invalidation, int orderKey);

    protected final boolean canSendInvalidation(final EventFilter filter, final String sourceUuid) {
        if (!(filter instanceof EventListenerFilter)) {
            return false;
        }

        if (!filter.eval(INVALIDATION.getType())) {
            return false;
        }

//        EventFilter unwrappedEventFilter = ((EventListenerFilter) filter).getEventFilter();
//        if (unwrappedEventFilter.eval(sourceUuid)) {
//            return false;
//        }

        return true;
    }

    public InvalidationObserver getObserver() {
        return observer;
    }

    private Data toHeapData(Data key) {
        return serializationService.toData(key);
    }

    @Override
    public void destroy(String mapName, String sourceUuid) {
        // nop.
    }

    @Override
    public void reset() {
        // nop.
    }

    @Override
    public void shutdown() {
        // nop.
    }

}
