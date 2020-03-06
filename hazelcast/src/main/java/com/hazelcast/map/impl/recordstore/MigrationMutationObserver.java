/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// TODO all states should be kept in this class,
//  for the whole state scope see MapReplicationOperation

// TODO who will clean state's map in this class after migration
public class MigrationMutationObserver implements MutationObserver {

    public static final String PROP_DURING_MIGRATION_BUFFER_SIZE = "hazelcast.during.migration.buffer.size";
    public static final String PROP_DURING_MIGRATION_BUFFER_TRY_COUNT = "hazelcast.during.migration.buffer.try.count";
    private static final HazelcastProperty DURING_MIGRATION_BUFFER_SIZE
            = new HazelcastProperty(PROP_DURING_MIGRATION_BUFFER_SIZE, 1 << 10);
    private static final HazelcastProperty DURING_MIGRATION_BUFFER_TRY_COUNT
            = new HazelcastProperty(PROP_DURING_MIGRATION_BUFFER_TRY_COUNT, 3);

    private final int partitionId;
    private final int bufferSize;
    private final int bufferTryCount;
    private final String name;
    private final ILogger logger;
    private final InternalPartition partition;
    private final LinkedHashMap<Data, Record> state = new LinkedHashMap<>();
    private AtomicInteger consumeCounter = new AtomicInteger();

    public MigrationMutationObserver(InternalPartitionService partitionService,
                                     int partitionId, ILogger logger, String name,
                                     HazelcastProperties properties) {
        this.name = name;
        this.partition = partitionService.getPartition(partitionId);
        this.logger = logger;
        this.partitionId = partitionId;
        this.bufferSize = properties.getInteger(DURING_MIGRATION_BUFFER_SIZE);
        this.bufferTryCount = properties.getInteger(DURING_MIGRATION_BUFFER_TRY_COUNT);
    }

    @Override
    public void onPutRecord(@Nonnull Data key, Record record, Object oldValue, boolean backup) {
        add(key, record);
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull Record record, boolean populateIndex) {
//        throw new UnsupportedOperationException();
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull Record record, Object oldValue, Object newValue, boolean backup) {
        add(key, record);
    }

    @Override
    public void onRemoveRecord(@Nonnull Data key, Record record) {
        add(key, null);
    }

    @Override
    public void onEvictRecord(@Nonnull Data key, @Nonnull Record record) {
        add(key, null);
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        add(key, record);
    }

    @Override
    public void onReset() {
        //throw new UnsupportedOperationException();
    }

    // TODO implement this for map.clear
    @Override
    public void onClear() {
        // throw new UnsupportedOperationException();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        // throw new UnsupportedOperationException();
    }

    private void add(@Nonnull Data key, Record record) {
        if (!partition.isMigrating()) {
            return;
        }

        state.put(key, record);

        if (state.size() % 1000 == 0) {
            logger.info(String.format("mapName=%s, partitionId=%d, " +
                    "bufferSize=%d, consumeCounter=%d", name, partitionId, state.size(), consumeCounter.get()));
        }
    }

    public Map<Data, Record> getState() {
        return state;
    }

    public boolean isUpdateBufferFull() {
        return consumeCounter.get() > bufferTryCount
                || state.size() >= bufferSize;
    }

    public void increaseConsumeCounter() {
        consumeCounter.incrementAndGet();
    }
}
