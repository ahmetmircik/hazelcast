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

public interface MapOperationProvider {

    MapOperation createPutOperation(String name, Data key, Data value, long ttl);

    MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout);

    MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl);

    MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl);

    MapOperation createPutTransientOperation(String name, Data key, Data value, long ttl);

    MapOperation createRemoveOperation(String name, Data key);

    MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout);

    MapOperation createWanOriginatedDeleteOperation(String name, Data key);

    MapOperation createReplaceOperation(String name, Data dataKey, Data value);

    MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value);

    MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update);

    MapOperation createDeleteOperation(String name, Data key);

    MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor);

    MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup);

    MapOperation createContainsKeyOperation(String name, Data dataKey);

    MapOperation createContainsValueOperation(String name, Data testValue);

    OperationFactory createContainsValueOperationFactory(String name, Data testValue);

    MapOperation createGetAllOperation(String name, Set<Data> keys);

    OperationFactory createGetAllOperationFactory(String name, Set<Data> keys);

    MapOperation createGetEntryViewOperation(String name, Data dataKey);

    MapOperation createGetOperation(String name, Data dataKey);

    MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues);

    MapOperation createPutAllOperation(String name, MapEntrySet entrySet, boolean initialLoad);

    OperationFactory createPutAllOperationFactory(String name, MapEntrySet entrySet);

    MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence);
}

