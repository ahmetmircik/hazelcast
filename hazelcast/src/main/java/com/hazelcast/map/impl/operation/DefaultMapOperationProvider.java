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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;

import java.util.List;
import java.util.Set;

/**
 * TODO add a proper JavaDoc
 */
public final class DefaultMapOperationProvider implements MapOperationProvider {

    private static final DefaultMapOperationProvider INSTANCE = new DefaultMapOperationProvider();

    private DefaultMapOperationProvider() {
    }

    public static DefaultMapOperationProvider get() {
        return INSTANCE;
    }

    @Override
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl) {
        return new PutOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        return new TryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl) {
        return new SetOperation(name, dataKey, value, ttl);
    }

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl) {
        return new PutIfAbsentOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl) {
        return new PutTransientOperation(name, key, value, ttl);
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key) {
        return new RemoveOperation(name, key);
    }

    @Override
    public MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout) {
        return new TryRemoveOperation(name, dataKey, timeout);
    }

    @Override
    public MapOperation createWanOriginatedDeleteOperation(String name, Data key) {
        return new WanOriginatedDeleteOperation(name, key);
    }

    @Override
    public MapOperation createReplaceOperation(String name, Data dataKey, Data value) {
        return new ReplaceOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value) {
        return new RemoveIfSameOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        return new ReplaceIfSameOperation(name, dataKey, expect, update);
    }

    @Override
    public MapOperation createDeleteOperation(String name, Data key) {
        return new DeleteOperation(name, key);
    }

    @Override
    public MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        return new EntryOperation(name, dataKey, entryProcessor);
    }

    @Override
    public MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup) {
        return new EvictOperation(name, dataKey, asyncBackup);
    }

    @Override
    public MapOperation createContainsKeyOperation(String name, Data dataKey) {
        return new ContainsKeyOperation(name, dataKey);
    }

    @Override
    public MapOperation createContainsValueOperation(String name, Data testValue) {
        return new ContainsValueOperation(name, testValue);
    }

    @Override
    public OperationFactory createContainsValueOperationFactory(String name, Data testValue) {
        return new ContainsValueOperationFactory(name, testValue);
    }

    @Override
    public MapOperation createGetAllOperation(String name, Set<Data> keys) {
        return new GetAllOperation(name, keys);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(String name, Set<Data> keys) {
        return new MapGetAllOperationFactory(name, keys);
    }

    @Override
    public MapOperation createGetEntryViewOperation(String name, Data dataKey) {
        return new GetEntryViewOperation(name, dataKey);
    }

    @Override
    public MapOperation createGetOperation(String name, Data dataKey) {
        return new GetOperation(name, dataKey);
    }

    @Override
    public MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        return new LoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntrySet entrySet, boolean initialLoad) {
        return new PutAllOperation(name, entrySet, initialLoad);
    }

    @Override
    public OperationFactory createPutAllOperationFactory(String name, MapEntrySet entrySet) {
        return new MapPutAllOperationFactory(name, entrySet);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence) {
        return new PutFromLoadAllOperation(name, keyValueSequence);
    }
}
