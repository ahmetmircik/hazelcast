/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore.rocks;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.map.impl.EntryCostEstimator;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.LazyEvictableEntryView;
import com.hazelcast.map.impl.recordstore.Storage;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.map.impl.OwnedEntryCostEstimatorFactory.ZERO_SIZE_ESTIMATOR;

public class RocksDBStorage<R extends Record> implements Storage<Data, R> {

    private final RocksDB rocksDB;
    private final InternalSerializationService ss;
    private final InMemoryFormat inMemoryFormat;
    private final byte[] prefixByteArray;

    private EntryCostEstimator entryCostEstimator;
    private long size = 0;

    public RocksDBStorage(RocksDB rocksDB, InternalSerializationService ss,
                          InMemoryFormat inMemoryFormat,
                          String mapName, int partitionId) {
        this.ss = ss;
        this.inMemoryFormat = inMemoryFormat;
        // TODO not supported for now
        this.entryCostEstimator = ZERO_SIZE_ESTIMATOR;
        String partitionFileName = partitionId + "_" + mapName;
        this.prefixByteArray = partitionFileName.getBytes();

        this.rocksDB = rocksDB;
    }

    @Override
    public void put(Data key, R record) {
        try {
            rocksDB.put(toKeyWithPrefix(key), recordToByteArray(record));
            size++;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public R updateRecordValue(Data key, R record, Object value) {
        record.setValue(inMemoryFormat == BINARY
                ? ss.toData(value) : ss.toObject(value));
        try {
            rocksDB.put(toKeyWithPrefix(key), recordToByteArray(record));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public R get(Data key) {
        try {
            byte[] bytes = rocksDB.get(toKeyWithPrefix(key));
            if (bytes == null) {
                return null;
            }
            return byteArrayToRecord(bytes);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public R getIfSameKey(Data key) {
        throw new UnsupportedOperationException("StorageImpl#getIfSameKey");
    }

    @Override
    public void removeRecord(Data key, R record) {
        try {
            rocksDB.delete(toKeyWithPrefix(key));
            size--;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean containsKey(Data key) {
        return rocksDB.keyMayExist(toKeyWithPrefix(key), null);
    }

    @Override
    public Iterator<Map.Entry<Data, R>> mutationTolerantIterator() {
        RocksIterator rocksIterator = rocksDB.newIterator();
        rocksIterator.seek(prefixByteArray);

        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return rocksIterator.isValid();
            }

            @Override
            public Map.Entry<Data, R> next() {
                byte[] key = fromKeyWithPrefix(rocksIterator.key());
                byte[] value = rocksIterator.value();

                Data dataKey = new HeapData(key);
                R record = byteArrayToRecord(value);

                rocksIterator.next();

                return new Map.Entry<>() {
                    @Override
                    public Data getKey() {
                        return dataKey;
                    }

                    @Override
                    public R getValue() {
                        return record;
                    }

                    @Override
                    public R setValue(R value) {
                        throw new UnsupportedOperationException("not implemented");
                    }
                };
            }
        };
    }

    @Override
    public int size() {
        return (int) size;
    }

    @Override
    public boolean isEmpty() {
        Iterator<Map.Entry<Data, R>> iterator = mutationTolerantIterator();
        return !iterator.hasNext();
    }

    @Override
    public void clear(boolean isDuringShutdown) {
        Iterator<Map.Entry<Data, R>> iterator = mutationTolerantIterator();
        while (iterator.hasNext()) {
            Map.Entry<Data, R> next = iterator.next();
            try {
                rocksDB.delete(toKeyWithPrefix(next.getKey()));
                size--;
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void destroy(boolean isDuringShutdown) {
        try {
            rocksDB.syncWal();
            rocksDB.close();
            size = 0;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    //TODO
    @Override
    public MapKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        throw new UnsupportedOperationException("TODO");
    }

    //TODO
    @Override
    public MapEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public EntryCostEstimator getEntryCostEstimator() {
        return entryCostEstimator;
    }

    @Override
    public void setEntryCostEstimator(EntryCostEstimator entryCostEstimator) {
        this.entryCostEstimator = entryCostEstimator;
    }

    @Override
    public Iterable<EntryView> getRandomSamples(int sampleCount) {
        throw new UnsupportedOperationException("Since no eviction is supported");
    }

    @Override
    public Data extractDataKeyFromLazy(EntryView entryView) {
        return ((LazyEvictableEntryView) entryView).getDataKey();
    }

    @Override
    public Data toBackingDataKeyFormat(Data key) {
        return key;
    }

    private byte[] recordToByteArray(R record) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectDataOutputStream out = new ObjectDataOutputStream(outputStream, ss);
        try {
            Records.writeRecord(out, record, ss.toData(record.getValue()));
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private R byteArrayToRecord(byte[] bytes) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        ObjectDataInputStream in = new ObjectDataInputStream(inputStream, ss);
        try {
            return (R) Records.readRecord(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] toKeyWithPrefix(Data key) {
        byte[] keyByteArray = key.toByteArray();
        byte[] concatenated = new byte[prefixByteArray.length + keyByteArray.length];
        System.arraycopy(prefixByteArray, 0, concatenated, 0, prefixByteArray.length);
        System.arraycopy(keyByteArray, 0, concatenated, prefixByteArray.length, keyByteArray.length);

        return concatenated;
    }

    private byte[] fromKeyWithPrefix(byte[] response) {
        int originalKeyLen = response.length - prefixByteArray.length;
        byte[] extractedKey = new byte[originalKeyLen];
        System.arraycopy(response, prefixByteArray.length, extractedKey, 0, originalKeyLen);

        return extractedKey;
    }
}
