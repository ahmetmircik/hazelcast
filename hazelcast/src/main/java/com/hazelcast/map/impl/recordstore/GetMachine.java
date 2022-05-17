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
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
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

public class GetMachine {

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
    private final InvalidationQueue<Operation> waitingMapStoreOperations;

    // temp
    Record record;
    Operation operation;

    volatile Object oldValue;
    volatile Object newValue;
    volatile State state = State.READ_MEMORY;

    // params
    boolean putTransient;
    long ttl = UNSET;
    long maxIdle = UNSET;
    long version;
    long threadId;
    long expiryTime;
    long now = Clock.currentTimeMillis();
    Data key;
    Address callerAddress;
    Object expect;
    PutParams staticPutParams;
    UUID txnId;
    UUID ownerUuid;

    // TODO do configurable
    final boolean offloadEnabled;

    public GetMachine(Operation operation) {
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
        this.waitingMapStoreOperations = recordStore.getWaitingMapStoreOperations();
        this.mapEventPublisher = mapServiceContext.getMapEventPublisher();
        this.name = mapContainer.getName();
        this.hasMapStore = mapDataStore != EMPTY_MAP_DATA_STORE;
        this.offloadEnabled = hasMapStore
                && mapContainer.getMapConfig().getMapStoreConfig().getWriteDelaySeconds() == 0;
    }

    public GetMachine setKey(Data key) {
        this.key = key;
        return this;
    }

    public GetMachine setNewValue(Object newValue) {
        this.newValue = newValue;
        return this;
    }

    public GetMachine setExpect(Object expect) {
        this.expect = expect;
        return this;
    }

    public GetMachine setTxnId(UUID uuid) {
        this.txnId = uuid;
        return this;
    }

    public GetMachine setOwnerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
        return this;
    }

    public GetMachine setVersion(long version) {
        this.version = version;
        return this;
    }

    public GetMachine setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public GetMachine setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
    }

    public GetMachine setPutTransient(boolean putTransient) {
        this.putTransient = putTransient;
        return this;
    }

    public GetMachine setStaticPutParams(PutParams staticPutParams) {
        this.staticPutParams = staticPutParams;
        return this;
    }

    public GetMachine setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public boolean isDone() {
        return state == State.DONE;
    }

    public Object getOldValue() {
        return oldValue;
    }

    private void goToState(State next) {
        state = next;
    }

    public CallStatus run() {
        if (hasMapStore) {
            return runWithMapStore();
        }

        return runNormal();
    }

    public CallStatus runNormal() {
        readMemory();

        recordStore.accessRecord(key, record, now);

        oldValue = mapServiceContext.interceptGet(interceptorRegistry, oldValue);
        return CallStatus.RESPONSE;
    }

//    @Override
//    public Object get(Data key, boolean backup, Address callerAddress, boolean touch) {
//        checkIfLoaded();
//        long now = getNow();
//
//        Record record = getRecordOrNull(key, now, backup);
//        if (record != null && touch) {
//            accessRecord(key, record, now);
//        } else if (record == null && mapDataStore != EMPTY_MAP_DATA_STORE) {
//            record = loadRecordOrNull(key, backup, callerAddress);
//            record = evictIfExpired(key, now, backup) ? null : record;
//        }
//        Object value = record == null ? null : record.getValue();
//        value = mapServiceContext.interceptGet(interceptorRegistry, value);
//
//        return value;
//    }


    @Nullable
    private CallStatus runWithMapStore() {
        do {
            switch (state) {
                case READ_MEMORY:
                    checkIfLoaded();
                    readMemory();
                    if (mustLoadOldValue()) {
                        goToState(offloadEnabled ? State.LOAD_ASYNC : State.LOAD_SYNC);
                    } else {
                        goToState(State.DO_META_OPS);
                    }
                    break;
                case LOAD_SYNC:
                    load();
                    goToState(State.DO_META_OPS);
                    break;
                case LOAD_ASYNC:
                    // TODO max offloaded request count, do we need a limit?
                    return loadAsyncThenGoTo(State.DO_META_OPS);
                case DO_META_OPS:
                    preStoreForPutIf();

                    if (canGoDoneState()) {
                        goToState(State.DONE);
                    } else {
                        doMetaOps();
                        goToState(offloadEnabled ? State.STORE_ASYNC : State.STORE_SYNC);
                    }
                    break;
                case WRITE_MEMORY:
                    writeMemory();
                    goToState(State.DONE);
                    break;
            }
        } while (state != State.DONE);

        log("Finished running: " + operation.getClass().getSimpleName() + ", identity: " + System.identityHashCode(operation));

        if (recordStore.getRunningOffloadOp().compareAndSet(true, false)) {
            resume(waitingMapStoreOperations.poll());
        }

        return CallStatus.RESPONSE;
    }

    private boolean canGoDoneState() {
        if (staticPutParams.isPutVanilla()) {
            return false;
        }

        if (staticPutParams.isPutIfEqual()) {
            boolean equal = valueComparator.isEqual(expect, oldValue, ss);
            if (!equal) {
                oldValue = null;
                return true;
            }
            return false;
        }

        if (staticPutParams.isPutIfAbsent()) {
            return oldValue != null;
        }

        if (staticPutParams.isPutIfExists()) {
            return oldValue == null;
        }

        if (canTxnProceed()) {
            return false;
        }

        return true;
    }

    private Offload loadAsyncThenGoTo(State next) {
        return offload(next, true);
    }

    private Offload storeAsyncThenGoTo(State next) {
        return offload(next, false);
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
            if (!recordStore.getRunningOffloadOp().compareAndSet(false, true)) {
                waitingMapStoreOperations.offer(offloadedOperation());
                return;
            }

            execute(() -> {
                try {
                    if (isLoad) {
                        load();
                    } else {
                        store();
                    }
                    goToState(next);
                    resume(offloadedOperation());
                } catch (Exception e) {
                    sendErrorResponse(e);
                }
            });
        }
    }

    private static long expirationTimeToTtl(long definedExpirationTime) {
        return definedExpirationTime - System.currentTimeMillis();
    }

    private void doMetaOps() {
        log("PutMachine.meta");
        // Intercept put on owner partition.
        if (!staticPutParams.isBackup()) {
            newValue = mapServiceContext.interceptPut(interceptorRegistry, oldValue, newValue);
        }
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
            mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED,
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
        putExpirySystem(ttl, maxIdle, expiryTime);
        notifyMutationObserver(staticPutParams.isPutFromLoad() ? LOADED : ADDED);
    }

    void update() {
        updateStats();
        updateStorage(newValue);
        putExpirySystem(ttl, maxIdle, expiryTime);
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

    GetMachine load() {
        log("PutInternal.load");
        if (!staticPutParams.isLoad()) {
            return this;
        }
        oldValue = recordStore.getMapDataStore().load(key);
        return this;
    }

    void store() {
        log("PutInternal.store");
        if (staticPutParams.isStore()) {
            long expirationTime = expirySystem.calculateExpirationTime(ttl, maxIdle, now, now);
            newValue = mapDataStore.add(key, newValue, expirationTime, now, txnId);
            return;
        }

        if (staticPutParams.isPutTransient()) {
            mapDataStore.addTransient(key, now);
            return;
        }
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

    void readMemory() {
        log("PutMachine.start");
        // Get record by checking expiry, if expired, evict record.
        record = recordStore.getRecordOrNull(key, now, staticPutParams.isBackup());
        oldValue = record != null ? record.getValue() : null;
    }

    public boolean canTxnProceed() {
        return txnId != null
                && (record == null || version == record.getVersion());
    }

    private boolean mustLoadOldValue() {
        if (!staticPutParams.isLoad() || record != null) {
            // we don't want to load old value or
            // we already have old value
            return false;
        }

        return true;
    }

    void writeMemory() {
        log("PutInternal.finish");
        if (record == null) {
            put();
        } else {
            update();
        }
        // TODO do we need to create on-heap copy for common path?
        oldValue = mapServiceContext.toData(oldValue);
    }

    enum State {
        READ_MEMORY,
        LOAD_SYNC,
        LOAD_ASYNC,
        DO_META_OPS,
        STORE_SYNC,
        STORE_ASYNC,
        WRITE_MEMORY,
        DONE
    }

    // TODO use striped executor
    private void execute(Runnable task) {
        log("PutInternal.execute");
        executionService.execute("offloaded-map-store", task);
    }

    private void resume(Operation operation) {
        if (operation == null) {
            return;
        }

        log("PutInternal.resume: " + operation.getClass().getSimpleName() + ", identity: " + System.identityHashCode(operation));
        recordStore.getMapContainer().getMapServiceContext()
                .getNodeEngine().getOperationService().execute(operation);

    }

    private void sendErrorResponse(Throwable t) {
        log("PutInternal.sendErrorResponse");
        operation.sendResponse(t);
    }
}
