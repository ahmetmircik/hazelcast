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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ConstructorFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.util.Collections.EMPTY_LIST;

/**
 * Sends invalidations to near-caches in batches.
 */
public class BatchInvalidator extends AbstractNearCacheInvalidator {

    private static final String INVALIDATION_EXECUTOR_NAME = BatchInvalidator.class.getName();

    /**
     * Creates an invalidation-queue for a map.
     */
    private final ConstructorFunction<String, InvalidationQueue> invalidationQueueConstructor
            = new ConstructorFunction<String, InvalidationQueue>() {
        @Override
        public InvalidationQueue createNew(String mapName) {
            return new InvalidationQueue();
        }
    };

    /**
     * map-name to invalidation-queue mappings.
     */
    private final ConcurrentMap<String, InvalidationQueue> invalidationQueues
            = new ConcurrentHashMap<String, InvalidationQueue>();

    private String listenerRegistrationId;
    private final int batchSize;

    BatchInvalidator(MapServiceContext mapServiceContext, NearCacheProvider nearCacheProvider) {
        super(mapServiceContext, nearCacheProvider);
        this.batchSize = getBatchSize();
        startBackgroundBatchProcessor();
        handleBatchesOnNodeShutdown();
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
            invalidateClient(mapContainer, new CleaningNearCacheInvalidation(mapContainer.getName(), sourceUuid));
        }

        clearLocal(mapContainer);
    }

    private void invalidateInternal(MapContainer mapContainer, Data key, List<Data> keys, String sourceUuid) {
        accumulateOrInvalidate(mapContainer, key, keys, sourceUuid);
        invalidateLocal(mapContainer, key, keys);
    }

    @Override
    public void destroy(MapContainer mapContainer) {
        InvalidationQueue invalidationQueue = invalidationQueues.remove(mapContainer.getName());
        if (invalidationQueue != null) {
            invalidateClient(mapContainer, new CleaningNearCacheInvalidation(mapContainer.getName(), null));
        }
    }

    @Override
    public void shutdown() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.shutdownExecutor(INVALIDATION_EXECUTOR_NAME);

        HazelcastInstance node = nodeEngine.getHazelcastInstance();
        LifecycleService lifecycleService = node.getLifecycleService();
        lifecycleService.removeLifecycleListener(listenerRegistrationId);

        invalidationQueues.clear();
    }

    @Override
    public void reset() {
        invalidationQueues.clear();
    }

    public void accumulateOrInvalidate(MapContainer mapContainer, Data key, List<Data> keys, String sourceUuid) {
        if (!mapContainer.isInvalidationEnabled()) {
            return;
        }

        String mapName = mapContainer.getName();
        InvalidationQueue invalidationQueue = getOrPutIfAbsent(invalidationQueues, mapName, invalidationQueueConstructor);

        if (key != null) {
            invalidationQueue.offer(new SingleNearCacheInvalidation(mapName, toHeapData(key), sourceUuid));
        }

        if (keys != null) {
            for (Data data : keys) {
                invalidationQueue.offer(new SingleNearCacheInvalidation(mapName, toHeapData(data), sourceUuid));
            }
        }

        if (invalidationQueue.size() >= batchSize) {
            sendBatch(mapContainer, invalidationQueue);
        }
    }

    private void sendBatch(MapContainer mapContainer, InvalidationQueue invalidationQueue) {
        if (invalidationQueue == null) {
            return;
        }
        // If still in progress, no need to another attempt. So just return.
        if (!invalidationQueue.tryAcquire()) {
            return;
        }

        int size = Math.min(batchSize, invalidationQueue.size());
        BatchNearCacheInvalidation batch = new BatchNearCacheInvalidation(mapContainer.getName(), size);
        for (int i = 0; i < size; i++) {
            SingleNearCacheInvalidation invalidation = invalidationQueue.poll();
            if (invalidation == null) {
                break;
            }
            batch.add(invalidation);
        }

        try {
            invalidateMember(mapContainer, batch);
            invalidateClient(mapContainer, batch);
        } finally {
            invalidationQueue.release();
        }
    }

    private void invalidateClient(MapContainer mapContainer, Invalidation invalidation) {
        if (!hasInvalidationListener(mapContainer)) {
            return;
        }

        String mapName = invalidation.getName();

        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, mapName);
        for (EventRegistration registration : registrations) {
            EventFilter filter = registration.getFilter();
            if (filter instanceof EventListenerFilter && filter.eval(INVALIDATION.getType())) {
                Object orderKey = getOrderKey(mapName, invalidation);
                eventService.publishEvent(SERVICE_NAME, registration, invalidation, orderKey.hashCode());
            }
        }
    }

    protected void invalidateMember(MapContainer mapContainer, BatchNearCacheInvalidation batch) {
        if (!isMemberNearCacheInvalidationEnabled(mapContainer)) {
            return;
        }

        String mapName = batch.getName();

        Operation operation = null;
        Collection<Member> members = clusterService.getMembers();
        for (Member member : members) {
            if (member.localMember()) {
                continue;
            }

            if (operation == null) {
                operation = createSingleOrBatchInvalidationOperation(mapName, null, getKeys(batch));
            }

            operationService.send(operation, member.getAddress());
        }
    }

    public static List<Data> getKeys(BatchNearCacheInvalidation batch) {
        return getKeysExcludingSource(batch, null);
    }

    public static List<Data> getKeysExcludingSource(BatchNearCacheInvalidation batch, String excludedSourceUuid) {
        List<SingleNearCacheInvalidation> invalidations = batch.getInvalidations();

        List<Data> keyList = null;
        for (SingleNearCacheInvalidation invalidation : invalidations) {
            if (excludedSourceUuid == null || !invalidation.getSourceUuid().equals(excludedSourceUuid)) {
                if (keyList == null) {
                    keyList = new ArrayList<Data>(invalidations.size());
                }
                keyList.add(invalidation.getKey());
            }
        }

        return keyList == null ? EMPTY_LIST : keyList;
    }


    private void handleBatchesOnNodeShutdown() {
        HazelcastInstance node = nodeEngine.getHazelcastInstance();
        LifecycleService lifecycleService = node.getLifecycleService();
        listenerRegistrationId = lifecycleService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleEvent.LifecycleState.SHUTTING_DOWN) {
                    Set<Map.Entry<String, InvalidationQueue>> entries = invalidationQueues.entrySet();
                    for (Map.Entry<String, InvalidationQueue> entry : entries) {
                        MapContainer mapContainer = mapServiceContext.getMapContainer(entry.getKey());
                        sendBatch(mapContainer, entry.getValue());
                    }
                }
            }
        });
    }

    private void startBackgroundBatchProcessor() {
        int periodSeconds = getBackgroundProcessorRunPeriodSeconds();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(INVALIDATION_EXECUTOR_NAME,
                new MapBatchInvalidationEventSender(), periodSeconds, periodSeconds, TimeUnit.SECONDS);

    }

    private int getBatchSize() {
        return nodeEngine.getProperties().getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
    }

    private int getBackgroundProcessorRunPeriodSeconds() {
        return nodeEngine.getProperties().getInteger(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
    }

    /**
     * A background runner which runs periodically and consumes invalidation queues.
     */
    private class MapBatchInvalidationEventSender implements Runnable {

        @Override
        public void run() {
            for (Map.Entry<String, InvalidationQueue> entry : invalidationQueues.entrySet()) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                String mapName = entry.getKey();
                InvalidationQueue invalidationQueue = entry.getValue();
                if (invalidationQueue.size() > 0) {
                    MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
                    sendBatch(mapContainer, invalidationQueue);
                }
            }
        }
    }

    private static class InvalidationQueue extends ConcurrentLinkedQueue<SingleNearCacheInvalidation> {

        private final AtomicInteger elementCount = new AtomicInteger(0);
        private final AtomicBoolean flushingInProgress = new AtomicBoolean(false);

        @Override
        public int size() {
            return elementCount.get();
        }

        @Override
        public boolean offer(SingleNearCacheInvalidation invalidation) {
            boolean offered = super.offer(invalidation);
            if (offered) {
                elementCount.incrementAndGet();
            }
            return offered;
        }

        @Override
        public SingleNearCacheInvalidation poll() {
            SingleNearCacheInvalidation invalidation = super.poll();
            if (invalidation != null) {
                elementCount.decrementAndGet();
            }
            return invalidation;
        }

        public boolean tryAcquire() {
            return flushingInProgress.compareAndSet(false, true);
        }

        public void release() {
            flushingInProgress.set(false);
        }

        @Override
        public boolean add(SingleNearCacheInvalidation invalidation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SingleNearCacheInvalidation remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection<? extends SingleNearCacheInvalidation> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }
    }
}
