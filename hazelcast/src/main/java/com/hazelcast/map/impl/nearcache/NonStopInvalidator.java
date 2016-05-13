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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.core.Member;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.Operation;

import java.util.Collection;
import java.util.List;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Sends invalidations to near-caches immediately.
 */
public class NonStopInvalidator extends AbstractNearCacheInvalidator {

    public NonStopInvalidator(MapServiceContext mapServiceContext, NearCacheProvider nearCacheProvider) {
        super(mapServiceContext, nearCacheProvider);
    }

    @Override
    public void invalidate(MapContainer mapContainer, Data key, String sourceUuid) {
        invalidateInternal(mapContainer, key, null, sourceUuid);
    }

    @Override
    public void invalidate(MapContainer mapContainer, List<Data> keys, String sourceUuid) {
        invalidateInternal(mapContainer, null, keys, sourceUuid);
    }

    @Override
    public void clear(MapContainer mapContainer, boolean owner, String sourceUuid) {
        if (owner) {
            // only send invalidation event to clients, server near-caches are cleared by ClearOperation.
            invalidateClient(mapContainer, null, null, sourceUuid);
        }

        clearLocal(mapContainer);
    }

    @Override
    public void destroy(MapContainer mapContainer) {
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

    private void invalidateInternal(MapContainer mapContainer, Data key, List<Data> keys, String sourceUuid) {
        invalidateMember(mapContainer, key, keys, sourceUuid);
        invalidateClient(mapContainer, key, keys, sourceUuid);
        invalidateLocal(mapContainer, key, keys);
    }

    protected void invalidateClient(MapContainer mapContainer, Data key, List<Data> keys, String sourceUuid) {
        if (!hasInvalidationListener(mapContainer)) {
            return;
        }

        String mapName = mapContainer.getName();

        Invalidation invalidation = null;
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
        for (EventRegistration registration : registrations) {
            EventFilter filter = registration.getFilter();
            if (filter instanceof EventListenerFilter && filter.eval(INVALIDATION.getType())) {
                if (invalidation == null) {
                    invalidation = newInvalidation(mapName, key, keys, sourceUuid);
                }
                Object orderKey = getOrderKey(mapName, invalidation);
                eventService.publishEvent(SERVICE_NAME, registration, invalidation, orderKey.hashCode());
            }
        }
    }

    private static Invalidation newInvalidation(String mapName, Data key, List<Data> keys, String sourceUuid) {
        if (key != null) {
            return new SingleNearCacheInvalidation(mapName, key, sourceUuid);
        }

        if (keys != null) {
            BatchNearCacheInvalidation batch = new BatchNearCacheInvalidation(mapName, keys.size());
            for (Data data : keys) {
                batch.add(new SingleNearCacheInvalidation(mapName, data, sourceUuid));
            }

            return batch;
        }

        // if key and keys are null, that means a cleaning invalidation must be created.
        return new CleaningNearCacheInvalidation(mapName, sourceUuid);
    }

    protected void invalidateMember(MapContainer mapContainer, Data key, List<Data> keys, String sourceUuid) {
        if (!isMemberNearCacheInvalidationEnabled(mapContainer)) {
            return;
        }

        Operation operation = null;
        Collection<Member> members = clusterService.getMembers();
        for (Member member : members) {
            if (member.localMember() || member.getUuid().equals(sourceUuid)) {
                continue;
            }

            if (operation == null) {
                operation = createSingleOrBatchInvalidationOperation(mapContainer.getName(), key, keys);
            }

            operationService.send(operation, member.getAddress());
        }
    }

}


