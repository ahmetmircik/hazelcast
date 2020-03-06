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
import com.hazelcast.map.impl.record.Record;

import javax.annotation.Nonnull;
import java.util.LinkedHashMap;
import java.util.Map;

// TODO all states should be kept in this class,
//  for the whole state scope see MapReplicationOperation

// TODO who will clean state's map in this class after migration
public class MigrationMutationObserver implements MutationObserver {

    private static final int BUFFER_SIZE = 1 << 18;
    private static final int MAX_CONSUME_COUNT = 3;

    private final InternalPartition partition;
    private final LinkedHashMap<Data, Record> state = new LinkedHashMap<>();
    private int consumeCounter;

    public MigrationMutationObserver(InternalPartitionService partitionService, int partitionId) {
        this.partition = partitionService.getPartition(partitionId);
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
    }

    public Map<Data, Record> getState() {
        return state;
    }

    public boolean isUpdateBufferFull() {
        return false;
//        return consumeCounter > MAX_CONSUME_COUNT
//                || state.size() >= BUFFER_SIZE;
    }

    public void increaseConsumeCounter() {
        consumeCounter++;
    }
}
