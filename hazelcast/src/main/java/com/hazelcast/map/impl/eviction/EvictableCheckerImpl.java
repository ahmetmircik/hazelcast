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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.MaxSizeConfig.MaxSizePolicy;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.MemoryInfoAccessor;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Checks whether a specific threshold is exceeded or not
 * according to configured {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy}
 * to start eviction process.
 *
 * @see EvictionOperatorImpl#evictableChecker
 */
public class EvictableCheckerImpl implements EvictableChecker {

    protected static final int ONE_HUNDRED_PERCENT = 100;
    protected static final int EVICTION_START_THRESHOLD_PERCENTAGE = 95;
    protected static final int ONE_KILOBYTE = 1024;
    protected static final int ONE_MEGABYTE = ONE_KILOBYTE * ONE_KILOBYTE;

    protected final MemoryInfoAccessor memoryInfoAccessor;
    protected final MapServiceContext mapServiceContext;


    public EvictableCheckerImpl(MapServiceContext mapServiceContext) {
        this(new RuntimeMemoryInfoAccessor(), mapServiceContext);
    }

    public EvictableCheckerImpl(MemoryInfoAccessor memoryInfoAccessor, MapServiceContext mapServiceContext) {
        this.memoryInfoAccessor = memoryInfoAccessor;
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public boolean isEvictable(MapContainer mapContainer, int partitionId) {
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
        boolean result;
        switch (maxSizePolicy) {
            case PER_NODE:
                result = isEvictablePerNode(mapContainer);
                break;
            case PER_PARTITION:
                result = isEvictablePerPartition(mapContainer, partitionId);
                break;
            case USED_HEAP_PERCENTAGE:
                result = isEvictableHeapPercentage(mapContainer);
                break;
            case USED_HEAP_SIZE:
                result = isEvictableHeapSize(mapContainer);
                break;
            case FREE_HEAP_PERCENTAGE:
                result = isEvictableFreeHeapPercentage(mapContainer);
                break;
            case FREE_HEAP_SIZE:
                result = isEvictableFreeHeapSize(mapContainer);
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate max size policy [" + maxSizePolicy + ']');
        }
        return result;
    }


    protected boolean isEvictablePerNode(MapContainer mapContainer) {
        long nodeTotalSize = 0;

        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final String mapName = mapContainer.getName();
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final List<Integer> partitionIds = findPartitionIds();
        for (int partitionId : partitionIds) {
            final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
            if (container == null) {
                continue;
            }
            nodeTotalSize += getRecordStoreSize(mapName, container);
            if (nodeTotalSize >= maxSize) {
                return true;
            }
        }
        return false;
    }

    protected boolean isEvictablePerPartition(final MapContainer mapContainer, int partitionId) {
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final String mapName = mapContainer.getName();
        final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        if (container == null) {
            return false;
        }
        final int size = getRecordStoreSize(mapName, container);
        return size >= maxSize;
    }

    protected boolean isEvictableHeapSize(final MapContainer mapContainer) {
        final long usedHeapSize = getUsedHeapSize(mapContainer);
        if (usedHeapSize == -1L) {
            return false;
        }
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return maxSize < (1D * usedHeapSize / ONE_MEGABYTE);
    }

    protected boolean isEvictableFreeHeapSize(final MapContainer mapContainer) {
        final long currentFreeHeapSize = getAvailableMemory();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double minFreeHeapSize = getApproximateMaxSize(maxSizeConfig.getSize());
        return minFreeHeapSize > (1D * currentFreeHeapSize / ONE_MEGABYTE);
    }

    protected boolean isEvictableHeapPercentage(final MapContainer mapContainer) {
        final long usedHeapSize = getUsedHeapSize(mapContainer);
        if (usedHeapSize == -1L) {
            return false;
        }
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double maxSize = getApproximateMaxSize(maxSizeConfig.getSize());
        final long total = getTotalMemory();
        return maxSize < (1D * ONE_HUNDRED_PERCENT * usedHeapSize / total);
    }

    protected boolean isEvictableFreeHeapPercentage(final MapContainer mapContainer) {
        final long currentFreeHeapSize = getAvailableMemory();
        final MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
        final double freeHeapPercentage = getApproximateMaxSize(maxSizeConfig.getSize());
        final long total = getTotalMemory();
        return freeHeapPercentage > (1D * ONE_HUNDRED_PERCENT * currentFreeHeapSize / total);
    }

    protected long getTotalMemory() {
        return memoryInfoAccessor.getTotalMemory();
    }

    protected long getFreeMemory() {
        return memoryInfoAccessor.getFreeMemory();
    }

    protected long getMaxMemory() {
        return memoryInfoAccessor.getMaxMemory();
    }

    protected long getAvailableMemory() {
        final long totalMemory = getTotalMemory();
        final long freeMemory = getFreeMemory();
        final long maxMemory = getMaxMemory();
        return freeMemory + (maxMemory - totalMemory);
    }

    protected long getUsedHeapSize(final MapContainer mapContainer) {
        long heapCost = 0L;
        final String mapName = mapContainer.getName();
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final List<Integer> partitionIds = findPartitionIds();
        for (int partitionId : partitionIds) {
            final PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
            if (container == null) {
                continue;
            }
            heapCost += getRecordStoreHeapCost(mapName, container);
        }
        heapCost += mapContainer.getNearCacheSizeEstimator().getSize();
        return heapCost;
    }

    protected int getRecordStoreSize(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0;
        }
        return existingRecordStore.size();
    }

    protected long getRecordStoreHeapCost(String mapName, PartitionContainer partitionContainer) {
        final RecordStore existingRecordStore = partitionContainer.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0L;
        }
        return existingRecordStore.getHeapCost();
    }

    /**
     * used when deciding evictable or not.
     */
    protected static double getApproximateMaxSize(int maxSizeFromConfig) {
        // because not to exceed the max size much we start eviction early.
        // so decrease the max size with ratio .95 below
        return 1D * maxSizeFromConfig * EVICTION_START_THRESHOLD_PERCENTAGE / ONE_HUNDRED_PERCENT;
    }

    /**
     * Get max size setting form config for given policy
     *
     * @return max size or -1 if policy is different or not set
     */
    public static double getApproximateMaxSize(MaxSizeConfig maxSizeConfig, MaxSizePolicy policy) {
        if (maxSizeConfig.getMaxSizePolicy() == policy) {
            return getApproximateMaxSize(maxSizeConfig.getSize());
        }
        return -1D;
    }

    protected List<Integer> findPartitionIds() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitionCount = partitionService.getPartitionCount();
        List<Integer> partitionIds = null;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (isOwnerOrBackup(partitionId)) {
                if (partitionIds == null) {
                    partitionIds = new ArrayList<Integer>();
                }
                partitionIds.add(partitionId);
            }
        }
        return partitionIds == null ? Collections.<Integer>emptyList() : partitionIds;
    }


    protected boolean isOwnerOrBackup(int partitionId) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final InternalPartition partition = partitionService.getPartition(partitionId, false);
        final Address thisAddress = nodeEngine.getThisAddress();
        return partition.isOwnerOrBackup(thisAddress);
    }
}


