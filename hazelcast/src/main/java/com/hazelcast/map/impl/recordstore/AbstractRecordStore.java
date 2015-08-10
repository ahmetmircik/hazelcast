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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.Map;
import java.util.Set;

/**
 * Contains record store common parts.
 */
abstract class AbstractRecordStore implements RecordStore<Record> {

    protected static final long DEFAULT_TTL = -1L;

    protected final InternalRecordStore<Data, Record> internalRecordStore;

    protected final RecordFactory recordFactory;

    protected final String name;

    protected final MapContainer mapContainer;

    protected final MapServiceContext mapServiceContext;

    protected final SerializationService serializationService;

    protected final int partitionId;

    protected AbstractRecordStore(MapContainer mapContainer, int partitionId) {
        this.mapContainer = mapContainer;
        this.partitionId = partitionId;
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        this.name = mapContainer.getName();
        this.recordFactory = mapContainer.getRecordFactory();
        this.internalRecordStore = createInternalRecordStore(recordFactory,
                mapContainer.getMapConfig().getInMemoryFormat());
    }

    @Override
    public InternalRecordStore createInternalRecordStore(RecordFactory recordFactory, InMemoryFormat memoryFormat) {
        return new InternalRecordStoreImpl(recordFactory, memoryFormat);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public MapContainer getMapContainer() {
        return mapContainer;
    }

    @Override
    public long getHeapCost() {
        return internalRecordStore.getSizeEstimator().getSize();
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected Record createRecord(Object value, long ttl, long now) {
        return mapContainer.createRecord(value, ttl, now);
    }

    protected Record createRecord(Object value, long now) {
        return mapContainer.createRecord(value, DEFAULT_TTL, now);
    }

    protected void accessRecord(Record record, long now) {
        record.setLastAccessTime(now);
        record.onAccess();
    }

    protected void accessRecord(Record record) {
        final long now = getNow();
        accessRecord(record, now);
    }

    // returns old value.
    protected Object updateRecord(Data key, Record record, Object value, long now) {
        accessRecord(record, now);
        record.setLastUpdateTime(now);
        record.onUpdate();
        return internalRecordStore.updateRecordValue(key, record, value);
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }


    protected void saveIndex(Data dataKey, Record record) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            SerializationService ss = mapServiceContext.getNodeEngine().getSerializationService();
            QueryableEntry queryableEntry = new QueryEntry(ss, dataKey, dataKey, record.getValue());
            indexService.saveEntryIndex(queryableEntry);
        }
    }


    protected void removeIndex(Data key) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            indexService.removeEntryIndex(key);
        }
    }

    protected void removeIndex(Set<Data> keys) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : keys) {
                indexService.removeEntryIndex(key);
            }
        }
    }

    /**
     * Removes indexes by excluding keysToPreserve.
     *
     * @param keysToRemove   remove these keys from index.
     * @param keysToPreserve do not remove these keys.
     */
    protected void removeIndexByPreservingKeys(Set<Data> keysToRemove, Set<Data> keysToPreserve) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : keysToRemove) {
                if (!keysToPreserve.contains(key)) {
                    indexService.removeEntryIndex(key);
                }
            }
        }
    }

    protected LockStore createLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService == null) {
            return null;
        }
        return lockService.createLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
    }

    protected void onStore(Record record) {
        if (record != null) {
            record.onStore();
        }
    }

    protected RecordStoreLoader createRecordStoreLoader(MapStoreContext mapStoreContext) {
        return mapStoreContext.getMapStoreWrapper() == null
                ? RecordStoreLoader.EMPTY_LOADER : new BasicRecordStoreLoader(this);
    }

    protected void clearRecordsMap(Map<Data, Record> excludeRecords) {
        InMemoryFormat inMemoryFormat = recordFactory.getStorageFormat();
        switch (inMemoryFormat) {
            case BINARY:
            case OBJECT:
                internalRecordStore.clear();
                if (excludeRecords != null && !excludeRecords.isEmpty()) {
                    Set<Map.Entry<Data, Record>> entries = excludeRecords.entrySet();
                    for (Map.Entry<Data, Record> e : entries) {
                        internalRecordStore.put(e.getKey(), e.getValue());
                    }
                }
                break;

            default:
                throw new IllegalArgumentException("Unknown storage format: " + inMemoryFormat);
        }

    }

    protected Data toData(Object value) {
        return mapServiceContext.toData(value);
    }

    public void setSizeEstimator(SizeEstimator sizeEstimator) {
        this.internalRecordStore.setSizeEstimator(sizeEstimator);
    }
}
