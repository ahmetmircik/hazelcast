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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.mapstore.writebehind.TxnReservedCapacityCounter;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntries;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.mapstore.writethrough.WriteThroughStore;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

// create per record-store
public class AsyncMapDataStore implements MapDataStore<Data, Object> {

    private final int partitionId;
    private final WriteThroughStore writeThroughStore;
    private final ExecutorService executorService;

    public AsyncMapDataStore(MapStoreContext mapStoreContext,
                             ExecutorService executorService, int partitionId) {
        this.writeThroughStore = new WriteThroughStore(mapStoreContext);
        this.executorService = executorService;
        this.partitionId = partitionId;
    }

    // TODO new name?
    InvalidationQueue queue = new InvalidationQueue();

    Operation latest;

    @Override
    public void registerOp(Operation operation) {
        latest = operation;
    }

    @Override
    public void deregisterOp(Operation operation) {
        latest = null;
    }

    @Override
    public Object add(Data key, Object value, long expirationTime, long now, UUID transactionId) {
        queue.add(DelayedEntries.newAddedDelayedEntry(key, value, expirationTime, now, partitionId, transactionId));
        return null;
    }

    @Override
    public Object addBackup(Data key, Object value, long expirationTime, long now, UUID transactionId) {
        return value;
    }

    @Override
    public void addForcibly(DelayedEntry<Data, Object> delayedEntry) {
        throw new IllegalStateException("No addForcibly call is expected from a write-through store!");
    }

    @Override
    public void addTransient(Data key, long now) {
        // NOP intentionally
    }

    @Override
    public void remove(Data key, long now, UUID transactionId) {
        // TODO
    }

    @Override
    public void removeBackup(Data key, long now, UUID transactionId) {
        // NOP intentionally
    }

    @Override
    public void reset() {
        // NOP intentionally
    }

    @Override
    public Object load(Data key) {
        // TODO
        queue.add(new Load(key));
        return null;
    }

    @Override
    public Map loadAll(Collection keys) {
        // TODO
        return null;
    }

    @Override
    public void removeAll(Collection keys) {
        // TODO
    }

    @Override
    public boolean loadable(Data key) {
        return true;
    }

    @Override
    public int notFinishedOperationsCount() {
        return 0;
    }

    @Override
    public boolean isPostProcessingMapStore() {
        return false;
    }

    @Override
    public long softFlush() {
        // Only write-behind configured map-stores are flushable.
        return 0;
    }

    @Override
    public void hardFlush() {
        // Only write-behind configured map-stores are flushable.
    }

    @Override
    public Object flush(Data key, Object value, boolean backup) {
        return value;
    }

    @Override
    public boolean isWithExpirationTime() {
        // TODO
        return false;
    }

    @Override
    public TxnReservedCapacityCounter getTxnReservedCapacityCounter() {
        return null;
    }

    @Override
    public boolean hasQueuedOperation() {
        return queue.size() > 0;
    }

    @Override
    public void processQueuedOperations() {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                Object poll;
                while ((poll = queue.poll()) != null) {
                    if (poll instanceof Load) {
                        Object value = writeThroughStore.load((Data) poll);

                    }
                }
            }
        });
    }
}
