/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.InterceptorRegistry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.LOADED;
import static com.hazelcast.map.impl.mapstore.MapDataStores.EMPTY_MAP_DATA_STORE;

public class PutMachine {
    private final ExecutionService executionService;
    private final MapServiceContext mapServiceContext;
    private final RecordStore recordStore;
    private final SerializationService ss;
    private final InterceptorRegistry interceptorRegistry;
    private final ValueComparator valueComparator;
    private final MapDataStore mapDataStore;
    private final ExpirySystem expirySystem;
    private final Storage storage;
    private final CompositeMutationObserver mutationObserver;

    // temp
    Record record;
    Operation operation;
    volatile Object oldValue;
    volatile State state = State.START;

    // params
    Data key;
    volatile Object newValue;
    long ttl;
    long maxIdle;
    PutParams staticPutParams;
    long now = Clock.currentTimeMillis();

    // TODO do configurable
    boolean offloadEnabled = true;

    public PutMachine(Operation operation) {
        this.operation = operation;
        this.recordStore = ((MapOperation) operation).getRecordStore();
        MapContainer mapContainer = recordStore.getMapContainer();
        this.mapServiceContext = mapContainer
                .getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.ss = nodeEngine.getSerializationService();
        this.executionService = nodeEngine.getExecutionService();
        this.interceptorRegistry = mapContainer.getInterceptorRegistry();
        this.valueComparator = mapServiceContext.getValueComparatorOf(recordStore.getInMemoryFormat());
        this.expirySystem = recordStore.getExpirySystem();
        this.mapDataStore = recordStore.getMapDataStore();
        this.storage = recordStore.getStorage();
        this.mutationObserver = recordStore.getMutationObserver();
    }

    public void init(PutParams staticPutParams, Data key, Object value, long ttl,
                     long maxIdle) {
        this.key = key;
        this.newValue = value;
        this.ttl = ttl;
        this.maxIdle = maxIdle;
        this.staticPutParams = staticPutParams;
    }

    public Object getOldValue() {
        return oldValue;
    }

    private void nextState(State next) {
        state = next;
    }

    public CallStatus run() {
        do {
            switch (state) {
                case START:
                    start();
                    if (mustLoadOldValue()) {
                        nextState(State.LOAD);
                    } else {
                        nextState(State.STORE);
                    }
                    break;
                case LOAD:
                    // TODO max offloaded request count, do we need a limit?
                    if (offloadEnabled) {
                        return offload(State.META, true);
                    } else {
                        load();
                        nextState(State.META);
                    }
                    break;
                case META:
                    meta();
                    break;
                case STORE:
                    if (offloadEnabled) {
                        return offload(State.FINAL, false);
                    } else {
                        store();
                        nextState(State.FINAL);
                    }
                    break;
                case FINAL:
                    finish();
                    nextState(State.DONE);
                    break;
            }
        } while (state != State.DONE);

        return CallStatus.RESPONSE;
    }

    private Offload offload(State next, boolean isLoad) {
        return new LoadStoreOffload(operation, next, isLoad);
    }

    private class LoadStoreOffload extends Offload {
        private final boolean isLoad;
        private final State next;

        private LoadStoreOffload(Operation operation, State next, boolean isLoad) {
            super(operation);
            this.isLoad = isLoad;
            this.next = next;
        }

        @Override
        public void start() throws Exception {
            execute(() -> {
                try {
                    if (isLoad) {
                        load();
                    } else {
                        store();
                    }
                    nextState(next);
                    resume(operation);
                } catch (Exception e) {
                    sendErrorResponse(e);
                }
            });

        }
    }

    private void meta() {
        System.out.println("PutMachine.meta");
        // For method replace, if current value is not expected one, return.
        if (staticPutParams.isPutIfEqual()
                && !valueComparator.isEqual(staticPutParams.getExpectedValue(),
                oldValue, ss)) {
            nextState(State.FINAL);
            return;
        }

        // Intercept put on owner partition.
        if (!staticPutParams.isBackup()) {
            newValue = mapServiceContext.interceptPut(interceptorRegistry, oldValue, newValue);
            nextState(State.STORE);
        }
    }

    void putNew() {
        Record record = recordStore.createRecord(key, newValue, now);
        if (mapDataStore.isPostProcessingMapStore()) {
            storage.updateRecordValue(key, record, newValue);
        }

        storage.put(key, record);
        expirySystem.add(key, ttl, maxIdle, staticPutParams.getExpiryTime(), now, now);

        EntryEventType entryEventType = staticPutParams.isPutFromLoad() ? LOADED : ADDED;
        if (entryEventType == EntryEventType.LOADED) {
            mutationObserver.onLoadRecord(key, record, staticPutParams.isBackup());
        } else {
            mutationObserver.onPutRecord(key, record, oldValue, staticPutParams.isBackup());
        }
    }

    void update() {
        recordStore.updateStatsOnPut(staticPutParams.isCountAsAccess(), now);
        record.onUpdate(now);

        if (staticPutParams.isCountAsAccess()) {
            record.onAccess(now);
        }

        storage.updateRecordValue(key, record, newValue);
        expirySystem.add(key, ttl, maxIdle, staticPutParams.getExpiryTime(), now, now);
        mutationObserver.onUpdateRecord(key, record, oldValue, newValue, staticPutParams.isBackup());
    }

    PutMachine load() {
        System.out.println("PutInternal.load");
        oldValue = recordStore.getMapDataStore().load(key);
        return this;
    }

    PutMachine store() {
        System.out.println("PutInternal.store");

        if (mapDataStore != EMPTY_MAP_DATA_STORE && staticPutParams.isStore()) {
            long expirationTime = expirySystem.calculateExpirationTime(ttl, maxIdle, now, now);
            newValue = mapDataStore.add(key, newValue, expirationTime, now, staticPutParams.getTransactionId());
        }
        return this;
    }

    PutMachine start() {
        System.out.println("PutMachine.start");
        if (staticPutParams.isCheckIfLoaded()) {
            // TODO ?
            recordStore.checkIfLoaded();
        }

        // Get record by checking expiry, if expired, evict record.
        record = recordStore.getRecordOrNull(key, now, staticPutParams.isBackup());
        oldValue = record != null ? record.getValue() : null;
        return this;
    }

    private boolean mustLoadOldValue() {
        if (mapDataStore == EMPTY_MAP_DATA_STORE) {
            return false;
        }

        if (staticPutParams.isPutIfAbsent()) {
            return true;
        }

        if (staticPutParams.isPutIfExists()) {
            return true;
        }

        return record == null && (staticPutParams.isLoad() ? true : false);
    }

    PutMachine finish() {
        System.out.println("PutInternal.finish");
        if (record == null) {
            putNew();
        } else {
            update();
        }
        return this;
    }

    enum State {
        START,
        LOAD,
        META,
        STORE,
        FINAL,
        DONE

    }

    // TODO use striped executor
    private void execute(Runnable task) {
        System.out.println("PutInternal.execute");
        executionService.execute("offloaded-map-store", task);

    }

    private void resume(Operation operation) {
        System.out.println("PutInternal.resume");
        recordStore.getMapContainer().getMapServiceContext()
                .getNodeEngine().getOperationService().execute(operation);

    }

    private void setNextState(Operation op, State state) {
        System.out.println("PutInternal.setNextState");
        this.state = state;

    }

    private void sendErrorResponse(Exception e) {
        System.out.println("PutInternal.sendErrorResponse");

    }


}
