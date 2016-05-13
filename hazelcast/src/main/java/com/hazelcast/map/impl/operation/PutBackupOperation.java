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

import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordInfo;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public final class PutBackupOperation extends MutatingKeyBasedMapOperation
        implements BackupOperation, IdentifiedDataSerializable {

    // todo unlockKey is a logic just used in transactional put operations.
    // todo It complicates here there should be another Operation for that logic. e.g. TxnSetBackup
    private static final int BITMASK_UNLOCK_KEY = 1;
    private static final int BITMASK_PUT_TRANSIENT = 1 << 1;
    private static final int BITMASK_DISABLE_WAN_REP_EVENT = 1 << 2;
    private static final int BITMASK_RECORD_INFO_EXISTS = 1 << 3;

    private byte flagHolder;
    private RecordInfo recordInfo;

    public PutBackupOperation(String name, Data dataKey, Data dataValue, RecordInfo recordInfo, boolean putTransient) {
        this(name, dataKey, dataValue, recordInfo, false, putTransient);
    }

    public PutBackupOperation(String name, Data dataKey, Data dataValue,
                              RecordInfo recordInfo, boolean unlockKey, boolean putTransient) {
        this(name, dataKey, dataValue, recordInfo, unlockKey, putTransient, false);
    }

    public PutBackupOperation(String name, Data dataKey, Data dataValue,
                              RecordInfo recordInfo, boolean unlockKey, boolean putTransient,
                              boolean disableWanReplicationEvent) {
        super(name, dataKey, dataValue);

        this.recordInfo = recordInfo;

        setFlag(unlockKey, BITMASK_UNLOCK_KEY);
        setFlag(putTransient, BITMASK_PUT_TRANSIENT);
        setFlag(disableWanReplicationEvent, BITMASK_DISABLE_WAN_REP_EVENT);
        setFlag(recordInfo != null, BITMASK_RECORD_INFO_EXISTS);
    }

    public PutBackupOperation() {
    }

    @Override
    public void run() {
        ttl = recordInfo != null ? recordInfo.getTtl() : ttl;
        final Record record = recordStore.putBackup(dataKey, dataValue, ttl, isFlagSet(BITMASK_PUT_TRANSIENT));
        if (recordInfo != null) {
            Records.applyRecordInfo(record, recordInfo);
        }
        if (isFlagSet(BITMASK_UNLOCK_KEY)) {
            recordStore.forceUnlock(dataKey);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (recordInfo != null) {
            evict();
        }
        if (!isFlagSet(BITMASK_DISABLE_WAN_REP_EVENT)) {
            publishWANReplicationEventBackup(mapServiceContext, mapEventPublisher);
        }

    }

    private void publishWANReplicationEventBackup(MapServiceContext mapServiceContext, MapEventPublisher mapEventPublisher) {
        if (!mapContainer.isWanReplicationEnabled()) {
            return;
        }

        Record record = recordStore.getRecord(dataKey);
        if (record == null) {
            return;
        }

        final Data valueConvertedData = mapServiceContext.toData(dataValue);
        final EntryView entryView = EntryViews.createSimpleEntryView(dataKey, valueConvertedData, record);
        mapEventPublisher.publishWanReplicationUpdateBackup(mapContainer, entryView);
    }


    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PUT_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeByte(flagHolder);
        if (recordInfo != null) {
            recordInfo.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        flagHolder = in.readByte();

        boolean hasRecordInfo = isFlagSet(BITMASK_RECORD_INFO_EXISTS);
        if (hasRecordInfo) {
            recordInfo = new RecordInfo();
            recordInfo.readData(in);
        }

    }

    private void setFlag(boolean exists, int bitmask) {
        if (exists) {
            flagHolder |= bitmask;
        } else {
            flagHolder &= ~bitmask;
        }
    }

    private boolean isFlagSet(int bitmask) {
        return (flagHolder & bitmask) != 0;
    }
}
