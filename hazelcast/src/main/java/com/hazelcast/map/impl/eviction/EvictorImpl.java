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
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.LazyEntryViewFromRecord;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.spi.properties.GroupProperty.MAP_EVICTION_BATCH_SIZE;

/**
 * Evictor helper methods.
 */
public class EvictorImpl implements Evictor {

    public static final int DEFAULT_SAMPLE_COUNT = 15;
    public static final int DEFAULT_POOL_SIZE = 5;
    public static final HazelcastProperty NEW_SAMPLING
            = new HazelcastProperty("hazelcast.map.eviction.new.sampling", false);
    public static final HazelcastProperty NEW_SAMPLING_POOL_SIZE
            = new HazelcastProperty("hazelcast.map.eviction.new.sampling.pool.size", DEFAULT_POOL_SIZE);
    public static final HazelcastProperty SAMPLE_COUNT
            = new HazelcastProperty("hazelcast.map.eviction.sample.count", DEFAULT_SAMPLE_COUNT);

    protected final EvictionChecker evictionChecker;
    protected final IPartitionService partitionService;
    protected final MapEvictionPolicy mapEvictionPolicy;

    private final int batchSize;

    private final int poolSize;
    private final int sampleCount;
    private final boolean newSampling;
    private final List<EntryView> pool;

    public EvictorImpl(MapEvictionPolicy mapEvictionPolicy,
                       EvictionChecker evictionChecker,
                       IPartitionService partitionService, HazelcastProperties properties) {
        this.evictionChecker = checkNotNull(evictionChecker);
        this.partitionService = checkNotNull(partitionService);
        this.mapEvictionPolicy = checkNotNull(mapEvictionPolicy);
        this.batchSize = properties.getInteger(MAP_EVICTION_BATCH_SIZE);
        this.sampleCount = properties.getInteger(SAMPLE_COUNT);
        this.newSampling = properties.getBoolean(NEW_SAMPLING);
        this.poolSize = properties.getInteger(NEW_SAMPLING_POOL_SIZE);
        this.pool = new ArrayList<>(poolSize);
    }

    @Override
    public void evict(RecordStore recordStore, Data excludedKey) {
        if (newSampling) {
            evictWithNewSampling(recordStore, excludedKey);
        } else {
            evictWithOldSampling(recordStore, excludedKey);
        }
    }

    private void evictWithOldSampling(RecordStore recordStore, Data excludedKey) {
        assertRunningOnPartitionThread();

        for (int i = 0; i < batchSize; i++) {
            EntryView evictableEntry = selectEvictableEntry(recordStore, excludedKey);
            if (evictableEntry == null) {
                return;
            }
            evictEntry(recordStore, evictableEntry);
        }
    }

    private EntryView selectEvictableEntry(RecordStore recordStore, Data excludedKey) {
        Iterable<EntryView> samples = getSamples(recordStore);
        EntryView excluded = null;
        EntryView selected = null;

        for (EntryView candidate : samples) {
            if (excludedKey != null && excluded == null && getDataKey(candidate).equals(excludedKey)) {
                excluded = candidate;
                continue;
            }

            if (selected == null) {
                selected = candidate;
            } else if (mapEvictionPolicy.compare(candidate, selected) < 0) {
                selected = candidate;
            }
        }

        return selected == null ? excluded : selected;
    }

    private static Data getDataKey(EntryView candidate) {
        return getRecordFromEntryView(candidate).getKey();
    }

    private void evictWithNewSampling(RecordStore recordStore, Data excludedKey) {
        assertRunningOnPartitionThread();

        Iterable<EntryView> samples = getSamples(recordStore);
        EntryView<Data, Object> evictableEntry = selectEvictableEntryNew(recordStore, samples);
        evictEntry(recordStore, evictableEntry);
    }

    private EntryView selectEvictableEntryNew(RecordStore recordStore,
                                              Iterable<EntryView> samples) {
        if (pool.size() == 0 && poolSize > 0) {
            Iterator<Record> iterator = recordStore.iterator();
            while (iterator.hasNext()) {
                pool.add(getEntryView(recordStore, iterator.next().getKey()));
                if (pool.size() == poolSize) {
                    break;
                }
            }
        }

        for (EntryView sample : samples) {
            pool.add(sample);
        }

        Collections.sort(pool, mapEvictionPolicy);

        EntryView removed;
        do {
            removed = pool.remove(0);
        } while (recordStore.getRecord(getRecordFromEntryView(removed).getKey()) == null);

        while (pool.size() > poolSize) {
            pool.remove(pool.size() - 1);
        }

        assert poolSize >= pool.size() : pool.size();

        return removed;
    }

    private EntryView getEntryView(RecordStore recordStore, Object key) {
        MapServiceContext mapServiceContext = recordStore.getMapContainer().getMapServiceContext();
        Data dataKey = mapServiceContext.toData(key);
        Record record = recordStore.getRecordOrNull(dataKey);
        if (record == null) {
            return null;
        }
        return new LazyEntryViewFromRecord(record,
                mapServiceContext.getNodeEngine().getSerializationService());
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
    public void forceEvict(RecordStore recordStore) {

    }

    @Override
    public boolean checkEvictable(RecordStore recordStore) {
        assertRunningOnPartitionThread();

        return evictionChecker.checkEvictable(recordStore);
    }

    // this method is overridden in another context.
    protected static Record getRecordFromEntryView(EntryView selectedEntry) {
        return ((LazyEntryViewFromRecord) selectedEntry).getRecord();
    }

    protected boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    protected Iterable<EntryView> getSamples(RecordStore recordStore) {
        Storage storage = recordStore.getStorage();
        return storage.getRandomSamples(sampleCount);
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
}
