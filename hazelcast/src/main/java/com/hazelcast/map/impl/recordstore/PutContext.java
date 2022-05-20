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
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.LockAwareOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpirySystem;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.LOADED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.mapstore.MapDataStores.EMPTY_MAP_DATA_STORE;
import static com.hazelcast.map.impl.record.Record.UNSET;

public abstract class PutContext extends LockAwareOperation implements BackupAwareOperation {

    protected ExecutionService executionService;
    protected SerializationService ss;
    protected InterceptorRegistry interceptorRegistry;
    protected ValueComparator valueComparator;
    protected MapDataStore mapDataStore;
    protected ExpirySystem expirySystem;
    protected Storage storage;
    protected MutationObserver mutationObserver;
    protected boolean hasMapStore;
    protected InvalidationQueue<Operation> waitingMapStoreOperations;

    // temp
    Record record;
    protected volatile boolean initialized;

    protected volatile Object oldValue;
    protected volatile Object newValue;
    protected volatile State state = State.READ_MEMORY;

    // params
    boolean putTransient;
    long ttl = UNSET;
    long maxIdle = UNSET;
    long version;
    long expiryTime;
    long now = Clock.currentTimeMillis();
    Data key;
    Address callerAddress;
    Object expect;
    StaticParams staticParams;
    UUID txnId;
    UUID ownerUuid;

    // TODO do configurable
    boolean offloadEnabled;

    public PutContext() {
    }

    public PutContext(String name, Data dataKey, Data dataValue) {
        super(name, dataKey, dataValue);
    }

    public void initContext() {
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
        this.hasMapStore = mapDataStore != EMPTY_MAP_DATA_STORE;
        this.offloadEnabled = hasMapStore
                && mapContainer.getMapConfig().getMapStoreConfig().getWriteDelaySeconds() == 0;
    }

    public PutContext setKey(Data key) {
        this.key = key;
        return this;
    }

    public PutContext setNewValue(Object newValue) {
        this.newValue = newValue;
        return this;
    }

    public PutContext setExpect(Object expect) {
        this.expect = expect;
        return this;
    }

    public PutContext setTxnId(UUID uuid) {
        this.txnId = uuid;
        return this;
    }

    public PutContext ownerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
        return this;
    }

    public PutContext version(long version) {
        this.version = version;
        return this;
    }

    public PutContext setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public PutContext setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
    }

    public PutContext setPutTransient(boolean putTransient) {
        this.putTransient = putTransient;
        return this;
    }

    public PutContext setStaticPutParams(StaticParams staticStaticParams) {
        this.staticParams = staticStaticParams;
        return this;
    }

    public PutContext setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public boolean isDone() {
        return state == State.DONE;
    }

    private void goToState(State next) {
        log("Going next state: " + next.name());
        state = next;
    }

    public void doRun() {
        if (hasMapStore) {
            doRunWithMapStore();
        } else {
            doRunVanilla();
        }
    }

    public void doRunVanilla() {
        readMemory();

        if (!canGoDoneState()) {
            doMetaOps();
            writeMemory();
        }
        goToState(State.DONE);
        callStatus = CallStatus.RESPONSE;
    }


    @Nullable
    private void doRunWithMapStore() {
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
                    callStatus = loadAsyncThenGoTo(State.DO_META_OPS);
                    return;
                case DO_META_OPS:
                    preStoreForPutIf();

                    if (canGoDoneState()) {
                        goToState(State.DONE);
                    } else {
                        doMetaOps();
                        goToState(offloadEnabled ? State.STORE_ASYNC : State.STORE_SYNC);
                    }
                    break;
                case STORE_SYNC:
                    store();
                    goToState(State.WRITE_MEMORY);
                    break;
                case STORE_ASYNC:
                    // TODO max offloaded request count, do we need a limit?
                    callStatus = storeAsyncThenGoTo(State.WRITE_MEMORY);
                    return;
                case WRITE_MEMORY:
                    writeMemory();
                    goToState(State.DONE);
                    break;
            }
        } while (state != State.DONE);

        log("Finished running: " + this.getClass().getSimpleName()
                + ", identity: " + System.identityHashCode(this));
        recordStore.getRunningOffloadOp().set(null);
        resume(waitingMapStoreOperations.poll());

        callStatus = CallStatus.RESPONSE;
    }

    private boolean canGoDoneState() {
        if (staticParams.isPutVanilla()) {
            return false;
        }

        if (staticParams.isPutIfEqual()) {
            boolean equal = valueComparator.isEqual(expect, oldValue, ss);
            if (!equal) {
                oldValue = null;
                return true;
            }
            return false;
        }

        if (staticParams.isPutIfAbsent()) {
            return oldValue != null;
        }

        if (staticParams.isPutIfExists()) {
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
        return new LoadStoreOffload(this, next, isLoad);
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
            log("LoadStoreOffload." + (isLoad ? "load" : "store"));
            AtomicReference activeOffload = recordStore.getRunningOffloadOp();
            Operation offloadedOperation = offloadedOperation();

            if (activeOffload.get() != offloadedOperation
                    && !activeOffload.compareAndSet(null, offloadedOperation)) {
                log("LoadStoreOffload " + (isLoad ? "load" : "store") + " --> put into waitingMapStoreOperations");
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
                } catch (Throwable t) {
                    recordStore.getRunningOffloadOp().set(null);
                    resume(waitingMapStoreOperations.poll());

                    sendErrorResponse(t);
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
        if (!staticParams.isBackup()) {
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

        if (!(staticParams.isPutIfAbsent()
                || staticParams.isPutIfExists())) {
            return record;
        }

        putStorage(oldValue);
        putExpirySystem(ttl, UNSET, UNSET);
        publishLoadedEvent(record);

        recordStore.evictEntries(key);
        // here, we are only publishing events for loaded
        // entries. This is required for notifying query-caches
        // otherwise query-caches cannot see loaded entries
        if (!staticParams.isBackup() && recordStore.hasQueryCache()) {
            recordStore.addEventToQueryCache(key, record);
        }
        return record;
    }

    private void publishLoadedEvent(Record record) {
        if (!staticParams.isBackup() && mapEventPublisher.hasEventListener(name)) {
            mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED,
                    key, null, record.getValue(), null);
        }
    }

    private void notifyMutationObserver(EntryEventType entryEventType) {
        switch (entryEventType) {
            case ADDED:
                mutationObserver.onPutRecord(key, record, oldValue, staticParams.isBackup());
                break;
            case UPDATED:
                mutationObserver.onUpdateRecord(key, record, oldValue, newValue, staticParams.isBackup());
                break;
            case LOADED:
                mutationObserver.onLoadRecord(key, record, staticParams.isBackup());
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
        notifyMutationObserver(staticParams.isPutFromLoad() ? LOADED : ADDED);
    }

    void update() {
        updateStats();
        updateStorage(newValue);
        putExpirySystem(ttl, maxIdle, expiryTime);
        notifyMutationObserver(UPDATED);
    }

    private void updateStats() {
        recordStore.updateStatsOnPut(staticParams.isCountAsAccess(), now);
        record.onUpdate(now);

        if (staticParams.isCountAsAccess()) {
            record.onAccess(now);
        }
    }

    private void updateStorage(Object value) {
        storage.updateRecordValue(key, record, value);
    }

    PutContext load() {
        log("PutInternal.load");
        if (!staticParams.isLoad()) {
            return this;
        }
        oldValue = recordStore.getMapDataStore().load(key);
        return this;
    }

    void store() {
        log("PutInternal.store");
        if (staticParams.isStore()) {
            long expirationTime = expirySystem.calculateExpirationTime(ttl, maxIdle, now, now);
            newValue = mapDataStore.add(key, newValue, expirationTime, now, txnId);
            return;
        }

        if (staticParams.isTransient()) {
            mapDataStore.addTransient(key, now);
            return;
        }
    }

    void log(String msg) {
        System.out.println(msg);
    }

    private void checkIfLoaded() {
        if (!staticParams.isCheckIfLoaded()) {
            return;
            // TODO ?
        }

        recordStore.checkIfLoaded();
    }

    void readMemory() {
        log("PutMachine.readMemory");
        // Get record by checking expiry, if expired, evict record.
        record = recordStore.getRecordOrNull(key, now, staticParams.isBackup());
        oldValue = record != null ? record.getValue() : null;
    }

    public boolean canTxnProceed() {
        return txnId != null
                && (record == null || version == record.getVersion());
    }

    private boolean mustLoadOldValue() {
        if (!staticParams.isLoad() || record != null) {
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

        log("Resuming next op from waitingMapStoreOperations");

        log("PutInternal.resume: " + operation.getClass().getSimpleName()
                + ", identity: " + System.identityHashCode(operation));
        recordStore.getMapContainer().getMapServiceContext()
                .getNodeEngine().getOperationService().execute(operation);

    }

    private void sendErrorResponse(Throwable t) {
        log("PutInternal.sendErrorResponse");
        this.sendResponse(t);
    }
}
