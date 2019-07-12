/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.LazyEntryViewFromRecord;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

/**
 * Evictor helper methods.
 */
public class EvictorImpl implements Evictor {
    protected final EvictionChecker evictionChecker;
    protected final IPartitionService partitionService;
    protected final MapEvictionPolicy mapEvictionPolicy;

    private final int batchSize;

    private final int poolSize = 5;
    private final ArrayList<EntryView> pool = new ArrayList<>(poolSize);

    public EvictorImpl(MapEvictionPolicy mapEvictionPolicy,
                       EvictionChecker evictionChecker,
                       IPartitionService partitionService, int batchSize) {
        this.evictionChecker = checkNotNull(evictionChecker);
        this.partitionService = checkNotNull(partitionService);
        this.mapEvictionPolicy = checkNotNull(mapEvictionPolicy);
        this.batchSize = batchSize;
    }

    @Override
    public void evict(RecordStore recordStore, Data excludedKey) {
        assertRunningOnPartitionThread();

        do {
            Iterable<EntryView> samples = getSamples(recordStore);
            EntryView<Data, Object> evictableEntry = selectEvictableEntry(samples);
            if (evictEntry(recordStore, evictableEntry)) {
                return;
            }

            Iterator<EntryView> iterator = pool.iterator();
            while (iterator.hasNext()) {
                EntryView nextPooled = iterator.next();
                iterator.remove();

                if (evictEntry(recordStore, nextPooled)) {
                    return;
                }
            }
        } while (recordStore.size() > 0);
    }

    @Override
    public void forceEvict(RecordStore recordStore) {

    }

    private EntryView selectEvictableEntry(Iterable<EntryView> samples) {
        for (EntryView<Data, Object> candidate : samples) {

            pool.add(candidate);
            Collections.sort(pool, mapEvictionPolicy);
            while (pool.size() > poolSize) {
                pool.remove(pool.size() - 1);
            }

        }
        return pool.remove(0);
    }

    private boolean evictEntry(RecordStore recordStore, EntryView selectedEntry) {
        Record record = getRecordFromEntryView(selectedEntry);
        Data key = record.getKey();

        if (recordStore.isLocked(record.getKey())) {
            return false;
        }

        boolean backup = isBackup(recordStore);
        Object evictedValue = recordStore.evict(key, backup);
        if (evictedValue == null) {
            return false;
        }

        if (!backup) {
            recordStore.doPostEvictionOperations(record);
        }

        return true;
    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        assertRunningOnPartitionThread();

        return evictionChecker.checkEvictable(recordStore);
    }

    // this method is overridden in another context.
    protected Record getRecordFromEntryView(EntryView selectedEntry) {
        return ((LazyEntryViewFromRecord) selectedEntry).getRecord();
    }

    protected boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    protected Iterable<EntryView> getSamples(RecordStore recordStore) {
        Storage storage = recordStore.getStorage();
        return storage.getRandomSamples(SAMPLE_COUNT);
    }

    protected static long getNow() {
        return Clock.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "EvictorImpl{"
                + ", mapEvictionPolicy=" + mapEvictionPolicy
                + ", batchSize=" + batchSize
                + '}';
    }

    public static void main(String[] args) {
        int keySpace = 12;
        int poolMax = 16;

        ArrayList<Integer> shuffled = new ArrayList<>();
        for (int i = 0; i < keySpace; i++) {
            shuffled.add(i);
        }
        Collections.shuffle(shuffled);

        ArrayList<Integer> pool = new ArrayList<>();
        for (Integer key : shuffled) {
            customAdd(key, pool, poolMax);
        }

        System.err.println("Custom");
        System.err.println(pool);

        ArrayList<Integer> control = new ArrayList<>(poolMax);
        for (Integer key : shuffled) {
            control.add(key);
        }
        Collections.sort(control);

        System.err.println("Control");
        System.err.println(control);

        System.err.println(control.equals(pool));
    }

    static void customAdd(Integer candidate, ArrayList<Integer> pool, int poolMax) {
        if (pool.isEmpty()) {
            pool.add(candidate);
            return;
        }

        int scanIndex = 0;
        int insertAt = -1;
        for (Integer pooledEntry : pool) {
            if (candidate < pooledEntry) {
                insertAt = scanIndex;
                break;
            }
            scanIndex++;
        }

        if (insertAt == -1) {
            if (scanIndex < poolMax - 1) {
                pool.add(candidate);
            }
            return;
        }

        if (pool.size() < poolMax) {
            pool.add(candidate);
        } else {
            pool.remove(poolMax - 1);
        }
        Collections.sort(pool);
    }

    static void customAdd2(Integer candidate, Integer[] pool, MutableInteger poolSize, int poolMax) {
        if (poolSize.value == 0) {
            pool[0] = candidate;
            poolSize.value++;
            return;
        }

        int scanIndex = 0;
        int insertAt = -1;
        for (int i = 0; i < poolSize.value; i++) {
            if (candidate < pool[i]) {
                insertAt = scanIndex;
                break;
            }
            scanIndex++;
        }

        if (insertAt == -1) {
            if (scanIndex < poolMax - 1) {
                pool[poolSize.value] = candidate;
                poolSize.value++;
            }
            return;
        }

        if (poolSize.value < poolMax) {
            poolSize.value++;
        }

        for (int i = poolSize.value - 1; i > insertAt; i--) {
            Integer prev = pool[i - 1];
            pool[i] = prev;
        }
        pool[insertAt] = candidate;
        return;
    }
}
