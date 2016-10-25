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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IMapEvent;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Root interface for Near Cache invalidation data.
 */
public abstract class Invalidation implements IMapEvent, IdentifiedDataSerializable {

    private String mapName;
    private String sourceUuid;

    public Invalidation() {
    }

    public Invalidation(String mapName, String sourceUuid) {
        this.mapName = checkNotNull(mapName, "mapName cannot be null");
        this.sourceUuid = checkNotNull(sourceUuid, "sourceUuid cannot be null");
    }

    public abstract void consumedBy(InvalidationHandler invalidationHandler);

    public final String getSourceUuid() {
        return sourceUuid;
    }

    @Override
    public String getName() {
        return mapName;
    }

    @Override
    public Member getMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public EntryEventType getEventType() {
        return INVALIDATION;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeUTF(sourceUuid);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        sourceUuid = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public String toString() {
        return "mapName='" + mapName + "', sourceUuid='" + sourceUuid;
    }
}
