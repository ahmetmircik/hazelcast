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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class MapRemainingReplicationOperation extends Operation
        implements IdentifiedDataSerializable {

    private Map<String, Map<Data, Record>> states = new HashMap<>();

    private transient NativeOutOfMemoryError oome;

    public MapRemainingReplicationOperation() {
    }

    public MapRemainingReplicationOperation(PartitionContainer container,
                                            int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);

        ConcurrentMap<String, RecordStore> maps = container.getMaps();
        for (RecordStore recordStore : maps.values()) {
            Map<Data, Record> state = ((DefaultRecordStore) recordStore)
                    .getMigrationMutationObserver().getState();
            states.put(recordStore.getName(), new HashMap<>(state));
            state.clear();
        }
    }

    @Override
    public void run() {
        try {
            for (Map.Entry<String, Map<Data, Record>> entry : states.entrySet()) {
                String mapName = entry.getKey();
                Map<Data, Record> state = entry.getValue();
                for (Map.Entry<Data, Record> dataEntry : state.entrySet()) {
                    Object key = getSerializationService(mapName).toObject(dataEntry.getKey());
                    Object value = getSerializationService(mapName).toObject(dataEntry.getValue());

                }
            }
        } catch (Throwable e) {
            getLogger().severe("map replication operation failed for partitionId="
                    + getPartitionId(), e);

            disposePartition();

            if (e instanceof NativeOutOfMemoryError) {
                oome = (NativeOutOfMemoryError) e;
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        disposePartition();

        if (oome != null) {
            getLogger().warning(oome.getMessage());
        }
    }

    private void disposePartition() {
//        for (String mapName : mapReplicationStateHolder.data.keySet()) {
//            dispose(mapName);
//        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        disposePartition();
        super.onExecutionFailure(e);
    }

    private void dispose(String mapName) {
        int partitionId = getPartitionId();
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionId, mapName);
        if (recordStore != null) {
            recordStore.disposeDeferredBlocks();
        }
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(states.size());

        for (Map.Entry<String, Map<Data, Record>> entry : states.entrySet()) {
            String mapName = entry.getKey();
            SerializationService ss = getSerializationService(mapName);
            Map<Data, Record> state = entry.getValue();

            out.writeUTF(mapName);
            out.writeInt(state.size());
            for (Map.Entry<Data, Record> objectEntry : state.entrySet()) {
                Data key = objectEntry.getKey();
                Record record = objectEntry.getValue();

                IOUtil.writeData(out, key);
                out.writeBoolean(record == null);

                if (record != null) {
                    Records.writeRecord(out, record, ss.toData(record.getValue()));
                }
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int stateMapSize = in.readInt();

        Map<Data, Record> map = new LinkedHashMap<>();

        for (int i = 0; i < stateMapSize; i++) {
            String mapName = in.readUTF();
            int stateSize = in.readInt();
            for (int j = 0; j < stateSize; j++) {
                Data key = IOUtil.readData(in);
                boolean isNull = in.readBoolean();
                if (isNull) {
                    map.put(key, null);
                } else {
                    Record record = Records.readRecord(in);
                    map.put(key, record);
                }

            }
            states.put(mapName, map);
        }
    }

    private SerializationService getSerializationService(String mapName) {
        return getRecordStore(mapName).getMapContainer().getMapServiceContext()
                .getNodeEngine().getSerializationService();
    }

    RecordStore getRecordStore(String mapName) {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getRecordStore(getPartitionId(),
                mapName, true);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMAINING_REPLICATION;
    }
}
