/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheRecordStore;
import com.hazelcast.cache.impl.nearcache.impl.DefaultNearCache;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;

public class PartitionedNearCache<K, V> extends DefaultNearCache<K, V> {

    private final InternalPartitionService partitionService;
    private final int partitionCount;

    public PartitionedNearCache(String name, NearCacheConfig nearCacheConfig, NearCacheContext nearCacheContext) {
        super(name, nearCacheConfig, nearCacheContext);

        this.partitionService = nearCacheContext.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();
    }

    @Override
    protected NearCacheRecordStore<K, V> createNearCacheRecordStore(NearCacheConfig nearCacheConfig,
                                                                    NearCacheContext nearCacheContext) {
        return new Partitions(partitionCount);
    }

    private class Partitions<K, V> implements NearCacheRecordStore<K, V> {

        private final NearCacheRecordStore[] partitions;
        private final NearCacheStatsImpl nearCacheStats;

        public Partitions(int partitionCount) {
            this.partitions = createPartitions(partitionCount);
            this.nearCacheStats = new NearCacheStatsImpl();
        }

        private NearCacheRecordStore[] createPartitions(int partitionCount) {
            NearCacheRecordStore[] partitions = new NearCacheRecordStore[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                partitions[i] = PartitionedNearCache.super.createNearCacheRecordStore(nearCacheConfig, nearCacheContext);
            }
            return partitions;
        }

        @Override
        public V get(K key) {
            return findPartitionOf(key).get(key);
        }

        @Override
        public void put(K key, V value) {
            findPartitionOf(key).put(key, value);
        }

        @Override
        public boolean remove(K key) {
            return findPartitionOf(key).remove(key);
        }

        @Override
        public void clear() {
            for (NearCacheRecordStore partition : partitions) {
                partition.clear();
            }

            nearCacheStats.setOwnedEntryCount(0);
            nearCacheStats.setOwnedEntryMemoryCost(0L);
        }

        @Override
        public void destroy() {
            for (NearCacheRecordStore partition : partitions) {
                partition.destroy();
            }

            nearCacheStats.setOwnedEntryCount(0);
            nearCacheStats.setOwnedEntryMemoryCost(0L);
        }

        @Override
        public NearCacheStats getNearCacheStats() {
            return nearCacheStats;
        }

        @Override
        public Object selectToSave(Object... candidates) {
            return partitions[0].selectToSave(candidates);
        }

        @Override
        public int size() {
            int size = 0;
            for (NearCacheRecordStore partition : partitions) {
                size += partition.size();
            }
            return size;
        }

        @Override
        public void doExpiration() {
            for (NearCacheRecordStore partition : partitions) {
                partition.doExpiration();
            }
        }

        @Override
        public void doEvictionIfRequired() {
            for (NearCacheRecordStore partition : partitions) {
                partition.doEvictionIfRequired();
            }
        }

        @Override
        public void doEviction() {
            for (NearCacheRecordStore partition : partitions) {
                partition.doEviction();
            }
        }

        private NearCacheRecordStore<K, V> findPartitionOf(K key) {
            int partitionId = partitionService.getPartitionId(key);
            return partitions[partitionId];
        }
    }

}
