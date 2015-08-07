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

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.EvictionOperator;
import com.hazelcast.map.impl.eviction.MaxSizeChecker;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateExpirationWithDelay;
import static com.hazelcast.map.impl.ExpirationTimeSetter.calculateMaxIdleMillis;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTime;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains eviction specific functionality.
 */
abstract class AbstractEvictableRecordStore extends AbstractRecordStore {

    /**
     * Number of reads before clean up.
     * A nice number such as 2^n - 1.
     */
    private static final int POST_READ_CHECK_POINT = 63;

    /**
     * Iterates over a pre-set entry count/percentage in one round.
     * Used in expiration logic for traversing entries. Initializes lazily.
     */
    private Iterator<Map.Entry<Data, Record>> expirationIterator;

    /**
     * If there is no clean-up caused by puts after some time,
     * count a number of gets and start eviction.
     */
    private int readCountBeforeCleanUp;

    /**
     * used in LRU eviction logic.
     */
    private long lruAccessSequenceNumber;

    /**
     * Last run time of cleanup operation.
     */
    private long lastEvictionTime;

    private volatile boolean hasEntryWithCustomTTL;

    protected AbstractEvictableRecordStore(MapContainer mapContainer, int partitionId) {
        super(mapContainer, partitionId);
    }

    public boolean isEvictionEnabled() {
        EvictionPolicy evictionPolicy = getEvictionPolicy();
        return !EvictionPolicy.NONE.equals(evictionPolicy);
    }

    private EvictionPolicy getEvictionPolicy() {
        MapConfig mapConfig = mapContainer.getMapConfig();
        return mapConfig.getEvictionPolicy();
    }

    /**
     * Returns {@code true} if this record store has at least one candidate entry
     * for expiration (idle or tll) otherwise returns {@code false}.
     */
    private boolean isRecordStoreExpirable() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        return hasEntryWithCustomTTL || mapConfig.getMaxIdleSeconds() > 0
                || mapConfig.getTimeToLiveSeconds() > 0;
    }

    /**
     * @see com.hazelcast.instance.GroupProperties#PROP_MAP_EXPIRY_DELAY_SECONDS
     */
    private long getBackupExpiryDelayMillis() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        GroupProperties groupProperties = nodeEngine.getGroupProperties();
        GroupProperties.GroupProperty delaySecondsProperty = groupProperties.MAP_EXPIRY_DELAY_SECONDS;
        int delaySeconds = delaySecondsProperty.getInteger();
        return TimeUnit.SECONDS.toMillis(delaySeconds);
    }

    @Override
    public void evictExpiredEntries(int percentage, boolean backup) {
        final long now = getNow();
        final int size = size();
        final int maxIterationCount = getMaxIterationCount(size, percentage);
        final int maxRetry = 3;
        int loop = 0;
        int evictedEntryCount = 0;
        while (true) {
            evictedEntryCount += evictExpiredEntriesInternal(maxIterationCount, now, backup);
            if (evictedEntryCount >= maxIterationCount) {
                break;
            }
            loop++;
            if (loop > maxRetry) {
                break;
            }
        }
    }

    @Override
    public boolean isExpirable() {
        return isRecordStoreExpirable();
    }

    /**
     * Intended to put an upper bound to iterations. Used in evictions.
     *
     * @param size       of iterate-able.
     * @param percentage percentage of size.
     * @return 100 If calculated iteration count is less than 100, otherwise returns calculated iteration count.
     */
    private int getMaxIterationCount(int size, int percentage) {
        final int defaultMaxIterationCount = 100;
        final float oneHundred = 100F;
        float maxIterationCount = size * (percentage / oneHundred);
        if (maxIterationCount <= defaultMaxIterationCount) {
            return defaultMaxIterationCount;
        }
        return Math.round(maxIterationCount);
    }

    // TODO refactor this method, record variable may be confusing.
    private int evictExpiredEntriesInternal(int maxIterationCount, long now, boolean backup) {
        int evictedCount = 0;
        int checkedEntryCount = 0;
        initExpirationIterator();
        List<Map.Entry<Data, Record>> entries = new ArrayList<Map.Entry<Data, Record>>();
        while (expirationIterator.hasNext()) {
            if (checkedEntryCount >= maxIterationCount) {
                break;
            }
            checkedEntryCount++;

            Map.Entry<Data, Record> entry = expirationIterator.next();
            Record record = entry.getValue();
            Data dataKey = entry.getKey();
            if (isRecordExpirable(dataKey, record, now, backup)) {
                entries.add(entry);
                evictedCount++;
            }
        }

        for (Map.Entry<Data, Record> e : entries) {
            Data key = e.getKey();
            Record recordValue = e.getValue();
            doPostEvictOperations(key, recordValue, backup);
            getEvictionOperator().fireEvent(key, recordValue, name, mapServiceContext);
            internalRecordStore.remove(key);
        }


        return evictedCount;
    }

    private void initExpirationIterator() {
        if (expirationIterator == null || !expirationIterator.hasNext()) {
            expirationIterator = internalRecordStore.entrySet().iterator();
        }
    }

    protected void resetAccessSequenceNumber() {
        lruAccessSequenceNumber = 0L;
    }

    /**
     * TODO make checkEvictable fast by carrying threshold logic to partition.
     * This cleanup adds some latency to write operations.
     * But it sweeps records much better under high write loads.
     * <p/>
     *
     * @param now now in time.
     */
    @Override
    public void evictEntries(long now, boolean backup) {
        if (isEvictionEnabled()) {
            cleanUp(now, backup);
        }
    }

    /**
     * If there is no clean-up caused by puts after some time,
     * try to clean-up from gets.
     *
     * @param now now.
     */
    protected void postReadCleanUp(long now, boolean backup) {
        if (isEvictionEnabled()) {
            readCountBeforeCleanUp++;
            if ((readCountBeforeCleanUp & POST_READ_CHECK_POINT) == 0) {
                cleanUp(now, backup);
            }
        }

    }

    /**
     * Makes eviction clean-up logic.
     *
     * @param now    now in millis.
     * @param backup <code>true</code> if running on a backup partition, otherwise <code>false</code>
     */
    private void cleanUp(long now, boolean backup) {
        if (size() == 0) {
            return;
        }
        if (shouldEvict(now)) {
            removeEvictableRecords(backup);
            lastEvictionTime = now;
            readCountBeforeCleanUp = 0;
        }
    }

    protected boolean shouldEvict(long now) {
        return isEvictionEnabled() && inEvictableTimeWindow(now) && isEvictable();
    }

    private void removeEvictableRecords(boolean backup) {
        final int evictableSize = getEvictableSize();
        if (evictableSize < 1) {
            return;
        }
        final MapConfig mapConfig = mapContainer.getMapConfig();
        getEvictionOperator().removeEvictableRecords(this, evictableSize, mapConfig, backup);
    }

    private int getEvictableSize() {
        final int size = size();
        if (size < 1) {
            return 0;
        }
        final int evictableSize = getEvictionOperator().evictableSize(size, mapContainer.getMapConfig());
        if (evictableSize < 1) {
            return 0;
        }
        return evictableSize;
    }


    private EvictionOperator getEvictionOperator() {
        return mapServiceContext.getEvictionOperator();
    }


    /**
     * Eviction waits at least {@link MapConfig#minEvictionCheckMillis} milliseconds to run.
     *
     * @return <code>true</code> if in that time window,
     * otherwise <code>false</code>
     */
    private boolean inEvictableTimeWindow(long now) {
        long minEvictionCheckMillis = getMinEvictionCheckMillis();
        return minEvictionCheckMillis == 0L
                || (now - lastEvictionTime) > minEvictionCheckMillis;
    }

    private long getMinEvictionCheckMillis() {
        MapConfig mapConfig = mapContainer.getMapConfig();
        return mapConfig.getMinEvictionCheckMillis();
    }

    private boolean isEvictable() {
        final EvictionOperator evictionOperator = getEvictionOperator();
        final MaxSizeChecker maxSizeChecker = evictionOperator.getMaxSizeChecker();
        return maxSizeChecker.checkEvictable(mapContainer, partitionId);
    }

    protected void markRecordStoreExpirable(long ttl) {
        if (ttl > 0L && ttl < Long.MAX_VALUE) {
            hasEntryWithCustomTTL = true;
        }
    }

    abstract Object evictInternal(Data key, boolean backup);

    abstract Object evictInternal(Data key, Record removedRecord, boolean backup);

    /**
     * Check if record is reachable according to ttl or idle times.
     * If not reachable return null.
     *
     * @param record {@link com.hazelcast.map.impl.record.Record}
     * @return null if evictable.
     */
    protected Record getOrNullIfExpired(Data key, Record record, long now, boolean backup) {
        if (!isRecordStoreExpirable()) {
            return record;
        }
        if (record == null) {
            return null;
        }
        if (isLocked(key)) {
            return record;
        }
        if (!isExpired(record, now, backup)) {
            return record;
        }
        evictInternal(key, backup);
        if (!backup) {
            getEvictionOperator().fireEvent(key, record, name, mapServiceContext);
        }
        return null;
    }

    @Override
    public boolean isRecordExpirable(Data key, Record record, long now, boolean backup) {
        checkNotNull(record, "record cannot be null");

        return isRecordStoreExpirable()
                && !isLocked(key)
                && isExpired(record, now, backup);
    }

    public boolean isExpired(Record record, long now, boolean backup) {
        return record == null
                || isIdleExpired(record, now, backup) == null
                || isTTLExpired(record, now, backup) == null;
    }

    private Record isIdleExpired(Record record, long now, boolean backup) {
        if (record == null) {
            return null;
        }
        // lastAccessTime : updates on every touch (put/get).
        final long lastAccessTime = record.getLastAccessTime();
        final long maxIdleMillis = calculateMaxIdleMillis(mapContainer.getMapConfig());
        final long idleMillis = calculateExpirationWithDelay(maxIdleMillis,
                getBackupExpiryDelayMillis(), backup);
        final long elapsedMillis = now - lastAccessTime;
        return elapsedMillis >= idleMillis ? null : record;
    }

    private Record isTTLExpired(Record record, long now, boolean backup) {
        if (record == null) {
            return null;
        }
        final long ttl = record.getTtl();
        // when ttl is zero or negative, it should remain eternally.
        if (ttl < 1L) {
            return record;
        }
        final long lastUpdateTime = record.getLastUpdateTime();
        final long ttlMillis = calculateExpirationWithDelay(ttl,
                getBackupExpiryDelayMillis(), backup);
        final long elapsedMillis = now - lastUpdateTime;
        return elapsedMillis >= ttlMillis ? null : record;
    }


    void increaseRecordEvictionCriteriaNumber(Record record, EvictionPolicy evictionPolicy) {
        switch (evictionPolicy) {
            case LRU:
                ++lruAccessSequenceNumber;
                record.setEvictionCriteriaNumber(lruAccessSequenceNumber);
                break;
            case LFU:
                record.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber() + 1L);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate eviction policy [" + evictionPolicy + ']');
        }
    }

    @Override
    protected void accessRecord(Record record, long now) {
        super.accessRecord(record, now);
        increaseRecordEvictionCriteriaNumber(record, getEvictionPolicy());

        final long maxIdleMillis = calculateMaxIdleMillis(mapContainer.getMapConfig());
        setExpirationTime(record, maxIdleMillis);
    }

    protected void mergeRecordExpiration(Record record, EntryView mergingEntry) {
        final long ttlMillis = mergingEntry.getTtl();
        record.setTtl(ttlMillis);

        final long lastAccessTime = mergingEntry.getLastAccessTime();
        record.setLastAccessTime(lastAccessTime);

        final long lastUpdateTime = mergingEntry.getLastUpdateTime();
        record.setLastUpdateTime(lastUpdateTime);

        final long maxIdleMillis = calculateMaxIdleMillis(mapContainer.getMapConfig());
        setExpirationTime(record, maxIdleMillis);

        markRecordStoreExpirable(record.getTtl());
    }


    /**
     * Read only iterator. Iterates by checking whether a record expired or not.
     */

    protected final class RecordStoreIterator implements Iterator<Map.Entry<Data, Record>> {
        private final long now;
        private final boolean checkExpiration;
        private final boolean backup;
        private final Iterator<Map.Entry<Data, Record>> iterator;
        private Map.Entry<Data, Record> nextEntry;
        private Map.Entry<Data, Record> lastReturned;


        protected RecordStoreIterator(Set<Map.Entry<Data, Record>> entries, long now, boolean backup) {
            this(entries, now, true, backup);
        }

        protected RecordStoreIterator(Set<Map.Entry<Data, Record>> entries) {
            this(entries, -1L, false, false);
        }

        private RecordStoreIterator(Set<Map.Entry<Data, Record>> entries, long now,
                                    boolean checkExpiration, boolean backup) {
            this.iterator = entries.iterator();
            this.now = now;
            this.checkExpiration = checkExpiration;
            this.backup = backup;
            advance();
        }

        @Override
        public boolean hasNext() {
            return nextEntry != null;
        }

        @Override
        public Map.Entry<Data, Record> next() {
            if (nextEntry == null) {
                throw new NoSuchElementException();
            }
            lastReturned = nextEntry;
            advance();
            return lastReturned;
        }

        @Override
        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }

            internalRecordStore.remove(lastReturned.getKey());
            lastReturned = null;
        }

        private void advance() {
            final long now = this.now;
            final boolean checkExpiration = this.checkExpiration;
            final Iterator<Map.Entry<Data, Record>> iterator = this.iterator;

            while (iterator.hasNext()) {
                nextEntry = iterator.next();
                if (nextEntry != null) {
                    if (!checkExpiration) {
                        return;
                    }

                    if (!isExpired(nextEntry.getValue(), now, backup)) {
                        return;
                    }
                }
            }
            nextEntry = null;
        }
    }
}
