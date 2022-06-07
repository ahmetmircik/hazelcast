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
import com.hazelcast.map.impl.operation.steps.State;
import com.hazelcast.map.impl.operation.steps.OpStep;
import com.hazelcast.map.impl.operation.steps.OpStepRunner;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import static com.hazelcast.map.impl.operation.steps.OpStepSupplier.newStepSupplier;

public abstract class BaseRemoveOperation extends LockAwareOperation
        implements BackupAwareOperation, MutatingOperation {

    protected transient Data dataOldValue;

    public BaseRemoveOperation() {
    }

    public BaseRemoveOperation(String name, Data dataKey) {
        super(name, dataKey);
    }

    @Override
    protected void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        if (recordStore != null) {
            recordStore.checkIfLoaded();
        }
    }

    @Override
    protected void afterRunInternal() {
        afterRunInternal0(dataKey, dataOldValue);
    }

    public void afterRunInternal0(Data dataKey, Object dataOldValue) {
        mapServiceContext.interceptAfterRemove(mapContainer.getInterceptorRegistry(), dataOldValue);
        mapEventPublisher.publishEvent(getCallerAddress(), name,
                EntryEventType.REMOVED, dataKey, dataOldValue, null);
        invalidateNearCache(dataKey);
        publishWanRemove(dataKey);
        evict(dataKey);
        super.afterRunInternal();
    }

    protected OpStepRunner newStepRunner() {
        return new OpStepRunner(getNodeEngine(), this,
                newStepSupplier(newState(), getOffloadStartingStep()), getName());
    }

    protected State newState() {
        return new State(recordStore, this)
                .setKey(dataKey)
                .setExpect(getExpect())
                .setCallerAddress(getCallerAddress())
                .setCallerProvenance(getCallerProvenance())
                .setPartitionId(getPartitionId());
    }

    protected Object getExpect() {
        return null;
    }

    abstract protected OpStep getOffloadStartingStep();

    @Override
    public Object getResponse() {
        return dataOldValue;
    }

    @Override
    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, dataKey, disableWanReplicationEvent());
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }
}
