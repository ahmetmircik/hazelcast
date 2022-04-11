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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.EntryLoader;
import com.hazelcast.map.impl.InterceptorRegistry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.LOADED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.mapstore.MapDataStores.EMPTY_MAP_DATA_STORE;
import static com.hazelcast.map.impl.record.Record.UNSET;

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
    private final MapEventPublisher mapEventPublisher;
    private final String name;
    private final boolean hasMapStore;

    // temp
    Record record;
    Operation operation;

    volatile Object oldValue;
    volatile Object newValue;
    volatile State state = State.START;

    // params
    Data key;
    long ttl = UNSET;
    long maxIdle = UNSET;
    boolean putTransient;
    Address callerAddress;
    Object expect;
    PutParams staticPutParams;
    UUID txnId;
    UUID ownerUuid;
    long version;
    private long threadId;
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
        this.mapEventPublisher = mapServiceContext.getMapEventPublisher();
        this.name = mapContainer.getName();
        this.hasMapStore = mapDataStore != EMPTY_MAP_DATA_STORE;
        this.offloadEnabled = hasMapStore
                && mapContainer.getMapConfig().getMapStoreConfig().getWriteDelaySeconds() == 0;
    }

    public PutMachine setKey(Data key) {
        this.key = key;
        return this;
    }

    public PutMachine setNewValue(Object newValue) {
        this.newValue = newValue;
        return this;
    }

    public PutMachine setExpect(Object expect) {
        this.expect = expect;
        return this;
    }

    public PutMachine setTxnId(UUID uuid) {
        this.txnId = uuid;
        return this;
    }

    public PutMachine setOwnerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
        return this;
    }

    public PutMachine setVersion(long version) {
        this.version = version;
        return this;
    }

    public PutMachine setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public PutMachine setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    public PutMachine setPutTransient(boolean putTransient) {
        this.putTransient = putTransient;
        return this;
    }

    public PutMachine setStaticPutParams(PutParams staticPutParams) {
        this.staticPutParams = staticPutParams;
        return this;
    }

    public PutMachine setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public boolean isDone() {
        return state == State.DONE;
    }

    public Object getOldValue() {
        return oldValue;
    }

    private void nextState(State next) {
        state = next;
    }

    public CallStatus run() {
        if (hasMapStore) {
            return runWithMapStore();
        }

        return runNormal();
    }

    public CallStatus runNormal() {
        start();
        State nextState = findNextState();
        if (nextState == State.STORE) {
            meta();
        }

        if (nextState != State.DONE) {
            finish();
        }
        nextState(State.DONE);
        return CallStatus.RESPONSE;
    }


    @Nullable
    private CallStatus runWithMapStore() {
        do {
            switch (state) {
                case START:
                    checkIfLoaded();
                    start();
                    if (mustLoadOldValue()) {
                        nextState(State.LOAD);
                    } else {
                        nextState(State.META);
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
                    preStoreForPutIf();

                    State nextState = findNextState();

                    if (nextState == State.STORE) {
                        meta();
                    }

                    nextState(nextState);
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

    private State findNextState() {
        if (staticPutParams.isPutIfEqual()) {
            boolean equal = valueComparator.isEqual(expect, oldValue, ss);
            if (!equal) {
                oldValue = null;
            }
            return equal ? State.STORE : State.DONE;
        }

        if (!staticPutParams.isPutIfAbsent() && !staticPutParams.isPutIfExists()) {
            return State.STORE;
        }

        if (staticPutParams.isPutIfAbsent()) {
            return oldValue == null ? State.STORE : State.DONE;
        }

        if (staticPutParams.isPutIfExists()) {
            return oldValue != null ? State.STORE : State.DONE;
        }

        if (!canTxnProceed()) {
            return State.DONE;
        }

        return State.DONE;
    }

    private Offload offload(State next, boolean isLoad) {
        return new LoadStoreOffload(operation, next, isLoad);
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
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

    private static long expirationTimeToTtl(long definedExpirationTime) {
        return definedExpirationTime - System.currentTimeMillis();
    }

    private void meta() {
        log("PutMachine.meta");
        // Intercept put on owner partition.
        if (!staticPutParams.isBackup()) {
            newValue = mapServiceContext.interceptPut(interceptorRegistry, oldValue, newValue);
        }
    }

    // used to pre-store loaded value for putIfAbsent and putIfExists
    private void preStoreOfLoadedValue() {
        preStoreForPutIf();
    }

    // putIfAbsent and putIfExist
    public Record preStoreForPutIf() {
        if (record != null || oldValue == null) {
            return null;
        }

        long ttl = UNSET;
        if (mapDataStore.isWithExpirationTime()) {
            EntryLoader.MetadataAwareValue loaderEntry = (EntryLoader.MetadataAwareValue) oldValue;
            long proposedTtl = expirationTimeToTtl(loaderEntry.getExpirationTime());
            if (proposedTtl <= 0) {
                oldValue = null;
            } else {
                oldValue = loaderEntry.getValue();
                ttl = proposedTtl;
            }
        }

        if (!(staticPutParams.isPutIfAbsent()
                || staticPutParams.isPutIfExists())) {
            return record;
        }

        putStorage(oldValue);
        putExpirySystem(ttl, UNSET, UNSET);
        publishLoadedEvent(record);

        recordStore.evictEntries(key);
        // here, we are only publishing events for loaded
        // entries. This is required for notifying query-caches
        // otherwise query-caches cannot see loaded entries
        if (!staticPutParams.isBackup() && recordStore.hasQueryCache()) {
            recordStore.addEventToQueryCache(key, record);
        }
        return record;
    }

    private void publishLoadedEvent(Record record) {
        if (!staticPutParams.isBackup() && mapEventPublisher.hasEventListener(name)) {
            mapEventPublisher.publishEvent(staticPutParams.getCallerAddress(), name, EntryEventType.LOADED,
                    key, null, record.getValue(), null);
        }
    }

    private void notifyMutationObserver(EntryEventType entryEventType) {
        switch (entryEventType) {
            case ADDED:
                mutationObserver.onPutRecord(key, record, oldValue, staticPutParams.isBackup());
                break;
            case UPDATED:
                mutationObserver.onUpdateRecord(key, record, oldValue, newValue, staticPutParams.isBackup());
                break;
            case LOADED:
                mutationObserver.onLoadRecord(key, record, staticPutParams.isBackup());
                break;
        }
    }

    private void putExpirySystem(long ttl, long maxIdle, long expiryTime) {
        expirySystem.add(key, ttl, maxIdle, expiryTime, now, now);
    }

    private void putStorage(Object value) {
        record = recordStore.createRecord(key, value, now);
        if (mapDataStore.isPostProcessingMapStore()) {
            updateStorage(value);
        }

        storage.put(key, record);
    }

    private void put() {
        putStorage(newValue);
        putExpirySystem(ttl, maxIdle, staticPutParams.getExpiryTime());
        notifyMutationObserver(staticPutParams.isPutFromLoad() ? LOADED : ADDED);
    }

    void update() {
        updateStats();
        updateStorage(newValue);
        putExpirySystem(ttl, maxIdle, staticPutParams.getExpiryTime());
        notifyMutationObserver(UPDATED);
    }

    private void updateStats() {
        recordStore.updateStatsOnPut(staticPutParams.isCountAsAccess(), now);
        record.onUpdate(now);

        if (staticPutParams.isCountAsAccess()) {
            record.onAccess(now);
        }
    }

    private void updateStorage(Object value) {
        storage.updateRecordValue(key, record, value);
    }

    PutMachine load() {
        log("PutInternal.load");
        if (mapDataStore == EMPTY_MAP_DATA_STORE) {
            return this;
        }
        oldValue = recordStore.getMapDataStore().load(key);
        return this;
    }

    PutMachine store() {
        log("PutInternal.store");
        if (!staticPutParams.isStore() || mapDataStore == EMPTY_MAP_DATA_STORE) {
            return this;
        }

        long expirationTime = expirySystem.calculateExpirationTime(ttl, maxIdle, now, now);
        newValue = mapDataStore.add(key, newValue, expirationTime, now, txnId);
        return this;
    }

    void log(String msg) {
        System.out.println(msg);
    }

    private void checkIfLoaded() {
        if (!staticPutParams.isCheckIfLoaded()) {
            return;
            // TODO ?
        }

        recordStore.checkIfLoaded();
    }

    PutMachine start() {
        log("PutMachine.start");
        // Get record by checking expiry, if expired, evict record.
        record = recordStore.getRecordOrNull(key, now, staticPutParams.isBackup());
        oldValue = record != null ? record.getValue() : null;
        return this;
    }

    void unlock() {
        recordStore.unlock(key, ownerUuid, threadId, operation.getCallId());
    }

    boolean canTxnProceed() {
        return txnId != null
                && (record == null || version == record.getVersion());
    }


    void flow() {
        unlock();
        start();
        if (canTxnProceed()) {
            finish();
        }

    }

    private boolean mustLoadOldValue() {
        if (!staticPutParams.isLoad()
                || mapDataStore == EMPTY_MAP_DATA_STORE
                || record != null) {
            return false;
        }

        if (staticPutParams.isPutIfAbsent()) {
            return true;
        }

        if (staticPutParams.isPutIfExists()) {
            return true;
        }

        return staticPutParams.isLoad();
    }

    PutMachine finish() {
        log("PutInternal.finish");
        if (record == null) {
            put();
        } else {
            update();
        }
        oldValue = mapServiceContext.toData(oldValue);
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
        log("PutInternal.execute");
        executionService.execute("offloaded-map-store", task);

    }

    private void resume(Operation operation) {
        log("PutInternal.resume");
        recordStore.getMapContainer().getMapServiceContext()
                .getNodeEngine().getOperationService().execute(operation);

    }

    private void setNextState(Operation op, State state) {
        log("PutInternal.setNextState");
        this.state = state;

    }

    private void sendErrorResponse(Exception e) {
        log("PutInternal.sendErrorResponse");

    }


}
