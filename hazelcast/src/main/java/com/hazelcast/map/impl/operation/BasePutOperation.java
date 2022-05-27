/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Operation;

import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.spi.impl.operationservice.CallStatus.RESPONSE;
import static com.hazelcast.spi.impl.operationservice.CallStatus.VOID;
import static com.hazelcast.spi.impl.operationservice.CallStatus.WAIT;

public abstract class BasePutOperation
        extends LockAwareOperation implements BackupAwareOperation {

    protected transient Object oldValue;
    protected transient EntryEventType eventType;
    protected transient Record recordToBackup;

    public BasePutOperation(String name, Data dataKey, Data value) {
        super(name, dataKey, value);
    }

    public BasePutOperation() {
    }

    protected StaticParams getStaticParams() {
        throw new UnsupportedOperationException();
    }

    public CallStatus call() throws Exception {
        if (this instanceof BlockingOperation) {
            BlockingOperation blockingOperation = (BlockingOperation) this;
            if (blockingOperation.shouldWait()) {
                return WAIT;
            }
        }

        if (offloadEnabled) {
            // TODO: afterRunInternal
            // TODO: concurrent map-store load/store ordering

            State state = new State(recordStore, this);
            state.setStaticPutParams(getStaticParams())
                    .setKey(dataKey)
                    .setNewValue(dataValue)
                    .setTtl(getTtl())
                    .setMaxIdle(getMaxIdle())
                    .setPartitionId(getPartitionId());

            CascadingRunMachine machine = new CascadingRunMachine(this);
            for (PutStep step : PutStep.VALUES) {
                machine.addStep(step.createExecutionWith(state));
            }
            return machine;
        }

        run();
        return returnsResponse() ? RESPONSE : VOID;
    }

    @Override
    protected void afterRunInternal() {
        Object value = isPostProcessing(recordStore)
                ? recordStore.getRecord(dataKey).getValue() : dataValue;
        mapServiceContext.interceptAfterPut(mapContainer.getInterceptorRegistry(), dataValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name, getEventType(),
                dataKey, oldValue, value);
        invalidateNearCache(dataKey);
        publishWanUpdate(dataKey, value);
        evict(dataKey);
        super.afterRunInternal();
    }

    // overridden in extension classes
    protected long getTtl() {
        return UNSET;
    }

    // overridden in extension classes
    protected long getMaxIdle() {
        return UNSET;
    }

    private EntryEventType getEventType() {
        if (eventType == null) {
            eventType = oldValue == null
                    ? EntryEventType.ADDED : EntryEventType.UPDATED;
        }
        return eventType;
    }

    @Override
    public boolean shouldBackup() {
        recordToBackup = recordStore.getRecord(dataKey);
        return recordToBackup != null;
    }

    @Override
    public Operation getBackupOperation() {
        dataValue = getValueOrPostProcessedValue(recordToBackup, dataValue);
        return newBackupOperation(dataKey, recordToBackup, dataValue);
    }

    protected PutBackupOperation newBackupOperation(Data
                                                            dataKey, Record record, Data dataValue) {
        ExpiryMetadata metadata = recordStore.getExpirySystem().getExpiryMetadata(dataKey);
        return new PutBackupOperation(name, dataKey, record, dataValue, metadata);
    }

    @Override
    public final int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public final int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }
}
