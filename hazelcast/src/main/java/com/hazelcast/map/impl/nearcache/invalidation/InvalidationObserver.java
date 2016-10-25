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

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.InvalidationStateOperationFactory;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.MapUtil;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.SECONDS;

public class InvalidationObserver implements Runnable {

    private final MapServiceContext mapServiceContext;
    private final ILogger logger;
    private final ExecutionService executionService;
    private final OperationService operationService;
    private final Member localMember;
    private final PartitionSequencer partitionSequencer;
    private final Set<String> nearCachedMapNames = newSetFromMap(new ConcurrentHashMap());
    private final ConcurrentMap<String, String> mapNameToListenerId = new ConcurrentHashMap<String, String>();

    private volatile boolean running;

    private Map<String, Integer> hashMap;
    private Map<Integer, Object> partitionIdToSeqMap;

    public InvalidationObserver(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.executionService = nodeEngine.getExecutionService();
        this.operationService = nodeEngine.getOperationService();
        this.localMember = nodeEngine.getLocalMember();
        this.partitionSequencer = new PartitionSequencer(nodeEngine.getPartitionService().getPartitionCount());
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;
        scheduleNextRun();
    }

    public synchronized void stop() {
        running = false;
    }

    public void register(String mapName, NearCache nearCache) {
        if (nearCachedMapNames.add(mapName)) {
            listenInvalidationsFromMap(mapName, nearCache);
            start();
        }
    }

    public void deregister(String mapName) {
        if (nearCachedMapNames.remove(mapName)) {
            String remove = mapNameToListenerId.remove(mapName);
            mapServiceContext.removeEventListener(mapName, remove);
        }
    }

    private void listenInvalidationsFromMap(String mapName, NearCache nearCache) {
        InvalidationHandler defaultHandler = new DefaultInvalidationHandler(nearCache);
        InvalidationHandler sequencedHandler = new SequencedInvalidationHandler(partitionSequencer, defaultHandler, logger);
        MapListener listener = new NearCacheInvalidationListener(sequencedHandler);
        EventFilter eventFilter = new UuidFilter(localMember.getUuid());

        String listenerId = mapServiceContext.addEventListener(listener, eventFilter, mapName);
        mapNameToListenerId.put(mapName, listenerId);
    }

    public long nextSequence(String mapName, int partitionId) {
        return partitionSequencer.nextSequence(mapName, partitionId);
    }

    public long currentSequence(String mapName, int partitionId) {
        return partitionSequencer.currentSequence(mapName, partitionId);
    }

    private void scheduleNextRun() {
        executionService.schedule(this, 1, SECONDS);
    }

    @Override
    public void run() {
        try {
            runInternal();
        } finally {
            if (running) {
                scheduleNextRun();
            }
        }
    }

    protected void runInternal() {
        if (isEmpty(nearCachedMapNames)) {
            return;
        }

        String[] mapNames = nearCachedMapNames.toArray(new String[nearCachedMapNames.size()]);
        OperationFactory operationFactory = new InvalidationStateOperationFactory(mapNames);

        try {
            partitionIdToSeqMap = operationService.invokeOnAllPartitions(SERVICE_NAME, operationFactory);

            Map<String, Integer> hashMap = MapUtil.createHashMap(mapNames.length);
            int index = 0;
            for (String mapName : mapNames) {
                hashMap.put(mapName, index++);
            }
            this.hashMap = hashMap;
            print(partitionIdToSeqMap);

        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
    }

    private void print(Map<Integer, Object> map) {
        Iterator<Map.Entry<Integer, Object>> i = map.entrySet().iterator();
        if (!i.hasNext()) {
            System.err.println("{}");
            return;
        }


        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (; ; ) {
            Map.Entry<Integer, Object> e = i.next();
            Integer key = e.getKey();
            long[] sequences = (long[]) e.getValue();
            sb.append(key);
            sb.append('=');
            sb.append(Arrays.toString(sequences));
            if (!i.hasNext()) {
                sb.append('}').toString();
                break;
            }
            sb.append(',').append(' ');
        }

        System.err.println(sb.toString());
    }


}
