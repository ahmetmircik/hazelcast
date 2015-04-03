package com.hazelcast.map.impl;
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

import com.hazelcast.core.IFunction;
import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.atomic.AtomicLongArray;

public class InvalidationCounter {

    private static final IFunction<Data, Integer> MURMUR = new IFunction<Data, Integer>() {
        public Integer apply(Data key) {
            return key.getPartitionHash();
        }
    };

    private static final IFunction<Data, Integer> IDENTITY = new IFunction<Data, Integer>() {
        public Integer apply(Data key) {
            return System.identityHashCode(key);
        }
    };

    private static final IFunction[] HASHES = {MURMUR, IDENTITY};

    private final int mask;

    private final AtomicLongArray counters;

    public InvalidationCounter(int partitionCount) {
        this.mask = partitionCount - 1;
        this.counters = new AtomicLongArray(partitionCount);
    }

    public void increase(Data key) {
        for (int i = 0; i < HASHES.length; i++) {
            Integer hash = (Integer) HASHES[i].apply(key);
            int slot = getSlot(hash);
            counters.incrementAndGet(slot);
        }
    }

    public long getCount(Data key) {
        long sum = 0;
        for (int i = 0; i < HASHES.length; i++) {
            Integer hash = (Integer) HASHES[i].apply(key);
            int slot = getSlot(hash);
            sum += counters.get(slot);
        }
        return sum;
    }

    public int hashCount() {
        return HASHES.length;
    }

    private int getSlot(Integer hash) {
        int abs = getAbs(hash);
        return abs & mask;
    }

    private static int getAbs(int hash) {
        if (hash == Integer.MIN_VALUE) {
            hash = 0;
        }
        return Math.abs(hash);
    }

}

