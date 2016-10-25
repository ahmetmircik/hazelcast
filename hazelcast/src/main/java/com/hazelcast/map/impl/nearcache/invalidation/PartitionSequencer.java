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

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongArray;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkPositive;

public class PartitionSequencer {

    private final int partitionCount;
    private final ConcurrentMap<String, AtomicLongArray> sequenceGenerators = new ConcurrentHashMap<String, AtomicLongArray>();
    private final ConstructorFunction<String, AtomicLongArray> sequenceGeneratorConstructor
            = new ConstructorFunction<String, AtomicLongArray>() {
        @Override
        public AtomicLongArray createNew(String arg) {
            return new AtomicLongArray(partitionCount);
        }
    };
    private final ConcurrentMap<Integer, String> partitionUuids = new ConcurrentHashMap<Integer, String>();

    public PartitionSequencer(int partitionCount) {
        this.partitionCount = checkPositive(partitionCount, "partitionCount should be positive but found " + partitionCount);
    }

    public long currentSequence(String mapName, int partitionId) {
        return sequenceGenerator(mapName).get(partitionId);
    }

    public long nextSequence(String mapName, int partitionId) {
        return sequenceGenerator(mapName).incrementAndGet(partitionId);
    }

    public boolean setIfNextSequence(String mapName, int partitionId, long sequence) {
        return sequenceGenerator(mapName).compareAndSet(partitionId, sequence - 1L, sequence);
    }

    public boolean setOrCheckUuid(int partitionId, String givenUuid) {
        String existingUuid = partitionUuids.putIfAbsent(partitionId, givenUuid);
        return existingUuid == null || existingUuid.equals(givenUuid);
    }

    private AtomicLongArray sequenceGenerator(String mapName) {
        return getOrPutIfAbsent(sequenceGenerators, mapName, sequenceGeneratorConstructor);
    }
}
