/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.record;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitioningStrategy;

public class DataRecordFactory implements RecordFactory<Data> {

    private final MapContainer mapContainer;
    private final SerializationService serializationService;
    private final PartitioningStrategy partitionStrategy;

    public DataRecordFactory(MapContainer mapContainer, SerializationService serializationService,
                             PartitioningStrategy partitionStrategy) {
        this.mapContainer = mapContainer;
        this.serializationService = serializationService;
        this.partitionStrategy = partitionStrategy;

    }

    @Override
    public Record<Data> newRecord(Data key, Object value) {
        assert value != null : "value can not be null";

        MapConfig mapConfig = mapContainer.getMapConfig();
        boolean statisticsEnabled = mapConfig.isStatisticsEnabled();
        CacheDeserializedValues cacheDeserializedValues = mapConfig.getCacheDeserializedValues();

        // TODO maxIdle, ttl?
        boolean minimal = !statisticsEnabled
                && !mapConfig.getHotRestartConfig().isEnabled()
                && mapContainer.getEvictor() == Evictor.NULL_EVICTOR
                && mapConfig.getMetadataPolicy() != MetadataPolicy.CREATE_ON_UPDATE;

        final Data valueData = serializationService.toData(value, partitionStrategy);
        Record<Data> record;
        switch (cacheDeserializedValues) {
            case NEVER:
                if (minimal) {
                    record = new MinimalDataRecord();
                    record.setKey(key);
                    record.setValue(valueData);
                } else {
                    record = statisticsEnabled
                            ? new DataRecordWithStats(valueData)
                            : new DataRecord(valueData);
                }
                break;
            default:
                record = statisticsEnabled
                        ? new CachedDataRecordWithStats(valueData)
                        : new CachedDataRecord(valueData);
        }
        record.setKey(key);
        return record;
    }

    @Override
    public void setValue(Record<Data> record, Object value) {
        assert value != null : "value can not be null";

        final Data v;
        if (value instanceof Data) {
            v = (Data) value;
        } else {
            v = serializationService.toData(value, partitionStrategy);
        }
        record.setValue(v);
    }
}
