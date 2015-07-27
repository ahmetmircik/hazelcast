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

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InternalRecordStoreImpl<V extends Record> implements InternalRecordStore<Data, V> {

    // Concurrency level is 1 since at most one thread can write at a time.
    private final ConcurrentMap<Data, V> records = new ConcurrentHashMap<Data, V>(1000, 0.75f, 1);

    public InternalRecordStoreImpl() {
    }

    @Override
    public void clear() {
        records.clear();
    }

    @Override
    public Collection<V> values() {
        return records.values();
    }

    @Override
    public void add(V record) {
        records.put(record.getKey(), record);
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
    public boolean containsKey(Data key) {
        return records.containsKey(key);
    }

    @Override
    public Object remove(Data key) {
        V remove = records.remove(key);
        Object oldValue = remove.getValue();
        if (remove != null) {
            remove.invalidate();
        }
        return oldValue;
    }
}
