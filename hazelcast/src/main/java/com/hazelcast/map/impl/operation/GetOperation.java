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

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.steps.GetOpStep;
import com.hazelcast.map.impl.operation.steps.State;
import com.hazelcast.map.impl.operation.steps.OpStepRunner;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import static com.hazelcast.map.impl.operation.steps.OpStepSupplier.newStepSupplier;

public final class GetOperation extends ReadonlyKeyBasedMapOperation implements BlockingOperation {

    private Data result;

    public GetOperation() {
    }

    public GetOperation(String name, Data dataKey) {
        super(name, dataKey);

        this.dataKey = dataKey;
    }

    @Override
    protected void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        recordStore.checkIfLoaded();
    }

    @Override
    protected void runInternal() {
        Object currentValue = recordStore.get(dataKey, false, getCallerAddress());
        result = extractResult(currentValue);
    }

    public Data extractResult(Object currentValue) {
        if (noCopyReadAllowed(currentValue)) {
            // in case of a 'remote' call (e.g a client call) we prevent making
            // an on-heap copy of the off-heap data
            return (Data) currentValue;
        } else {
            // in case of a local call, we do make a copy, so we can safely share
            // it with e.g. near cache invalidation
            return mapService.getMapServiceContext().toData(currentValue);
        }
    }

    @Override
    protected OpStepRunner newStepRunner() {
        State state = new State(recordStore, this);
        state.setKey(dataKey)
                .setCallerAddress(getCallerAddress())
                .setPartitionId(getPartitionId());

        return new OpStepRunner(getNodeEngine(), this,
                newStepSupplier(state, GetOpStep.INITIAL), getName());
    }

    private boolean noCopyReadAllowed(Object currentValue) {
        return currentValue instanceof Data
                && (!getNodeEngine().getLocalMember().getUuid().equals(getCallerUuid())
                || !super.executedLocally());
    }

    @Override
    protected void afterRunInternal() {
        afterRunInternal0(result);
    }

    public void afterRunInternal0(Data result) {
        mapServiceContext.interceptAfterGet(mapContainer.getInterceptorRegistry(), result);
        super.afterRunInternal();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(getServiceNamespace(), dataKey);
    }

    @Override
    public boolean shouldWait() {
        if (recordStore.isTransactionallyLocked(dataKey)) {
            return !recordStore.canAcquireLock(dataKey, getCallerUuid(), getThreadId());
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }

    @Override
    public Data getResponse() {
        return result;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.GET;
    }
}
