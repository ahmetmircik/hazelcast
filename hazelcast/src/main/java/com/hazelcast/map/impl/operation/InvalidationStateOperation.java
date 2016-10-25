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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.NearCacheProvider;
import com.hazelcast.map.impl.nearcache.invalidation.InvalidationObserver;
import com.hazelcast.map.impl.nearcache.invalidation.NearCacheInvalidator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

import static com.hazelcast.map.impl.MapDataSerializerHook.F_ID;
import static com.hazelcast.map.impl.MapDataSerializerHook.INVALIDATION_STATE;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class InvalidationStateOperation extends Operation
        implements IdentifiedDataSerializable, ReadonlyOperation, PartitionAwareOperation {

    private String[] nearCachedMapNames;
    private long[] sequences;

    public InvalidationStateOperation() {
    }

    public InvalidationStateOperation(String... nearCachedMapNames) {
        this.nearCachedMapNames = checkNotNull(nearCachedMapNames, "nearCachedMapNames cannot be empty");
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() {
        InvalidationObserver observer = getInvalidationObserver();
        sequences = new long[nearCachedMapNames.length];
        int index = 0;
        do {
            sequences[index] = observer.currentSequence(nearCachedMapNames[index], getPartitionId());
            index++;
        } while (index < nearCachedMapNames.length);
    }

    private InvalidationObserver getInvalidationObserver() {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        NearCacheInvalidator nearCacheInvalidator = nearCacheProvider.getNearCacheInvalidator();
        return nearCacheInvalidator.getObserver();
    }

    @Override
    public Object getResponse() {
        return sequences;
    }

    @Override
    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        nearCachedMapNames = in.readUTFArray();
    }

    @Override
    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeUTFArray(nearCachedMapNames);
    }

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public int getId() {
        return INVALIDATION_STATE;
    }

}
