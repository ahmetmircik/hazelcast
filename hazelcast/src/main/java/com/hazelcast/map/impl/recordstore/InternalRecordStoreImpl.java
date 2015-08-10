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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapSizeEstimator;
import com.hazelcast.map.impl.SizeEstimator;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InternalRecordStoreImpl<V extends Record> implements InternalRecordStore<Data, V> {

    private SizeEstimator sizeEstimator;

    private final RecordFactory<V> recordFactory;
    // Concurrency level is 1 since at most one thread can write at a time.
    private final ConcurrentMap<Data, V> records = new ConcurrentHashMap<Data, V>(1000, 0.75f, 1);

    public InternalRecordStoreImpl(RecordFactory<V> recordFactory, InMemoryFormat memoryFormat) {
        this.recordFactory = recordFactory;
        this.sizeEstimator = new MapSizeEstimator(memoryFormat);
    }

    @Override
    public void clear() {
        records.clear();

        sizeEstimator.reset();
    }

    @Override
    public Collection<V> values() {
        return records.values();
    }

    @Override
    public void put(Data key, V record) {
        V previousRecord = records.put(key, record);

        if (previousRecord == null) {
            updateSizeEstimator(calculateHeapCost(key));
        }

        updateSizeEstimator(-calculateHeapCost(previousRecord));
        updateSizeEstimator(calculateHeapCost(record));
    }

    @Override
    public Object updateRecordValue(Data key, V record, Object value) {
        updateSizeEstimator(-calculateHeapCost(record));

        Object oldValue = record.getValue();
        recordFactory.setValue(record, value);

        updateSizeEstimator(calculateHeapCost(record));

        return oldValue;
    }

    @Override
    public V get(Data key) {
        return records.get(key);
    }

    @Override
    public Set<Data> keySet() {
        return records.keySet();
    }

    @Override
    public Set<Map.Entry<Data, V>> entrySet() {
        return records.entrySet();
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    public void destroy() {
        clear();
    }

    @Override
    public SizeEstimator getSizeEstimator() {
        return sizeEstimator;
    }

    @Override
    public boolean containsKey(Data key) {
        return records.containsKey(key);
    }

    @Override
    public Object remove(Data key) {
        V record = records.remove(key);

        updateSizeEstimator(-calculateHeapCost(record));
        updateSizeEstimator(-calculateHeapCost(key));

        Object oldValue = record.getValue();
        if (record != null) {
            record.invalidate();
        }
        return oldValue;
    }

    protected void updateSizeEstimator(long recordSize) {
        sizeEstimator.add(recordSize);
    }

    protected long calculateHeapCost(Object obj) {
        return sizeEstimator.calculateSize(obj);
    }

    public void setSizeEstimator(SizeEstimator sizeEstimator) {
        this.sizeEstimator = sizeEstimator;
    }

}
