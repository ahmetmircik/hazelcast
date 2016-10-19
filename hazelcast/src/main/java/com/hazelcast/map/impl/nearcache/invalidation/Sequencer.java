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
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class Sequencer {

    private final ConcurrentMap<String, AtomicLong> sequenceGenerators = new ConcurrentHashMap<String, AtomicLong>();
    private final ConstructorFunction<String, AtomicLong> sequenceGeneratorConstructor
            = new ConstructorFunction<String, AtomicLong>() {
        @Override
        public AtomicLong createNew(String arg) {
            return new AtomicLong();
        }
    };

    public Sequencer() {
    }

    public long currentSequence(String mapName) {
        return sequenceGenerator(mapName).get();
    }

    public long nextSequence(String mapName) {
        return sequenceGenerator(mapName).incrementAndGet();
    }

    public boolean setIfNextSequence(String mapName, long sequence) {
        return sequenceGenerator(mapName).compareAndSet(sequence - 1L, sequence);
    }

    private AtomicLong sequenceGenerator(String mapName) {
        return getOrPutIfAbsent(sequenceGenerators, mapName, sequenceGeneratorConstructor);
    }
}
