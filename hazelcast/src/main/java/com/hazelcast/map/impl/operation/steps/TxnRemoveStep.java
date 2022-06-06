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

package com.hazelcast.map.impl.operation.steps;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.UUID;

public enum TxnRemoveStep implements Step<State> {

    INITIAL() {
        @Override
        public Runnable createStep(State state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Step nextStep(State state) {
            return READ;
        }
    },

    READ() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    runStepDirect(state);
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public void runStepDirect(State state) {
            Step.log("==> reading old value"
                    + " (thread:" + Thread.currentThread().getName() + ")");

            RecordStore recordStore = state.getRecordStore();

            Data dataKey = state.getKey();
            long threadId = state.getThreadId();
            long version = state.getVersion();
            UUID ownerUuid = state.getOwnerUuid();
            long callId = state.getOperation().getCallId();
            UUID transactionId = state.getTxnId();
            MapOperation operation = state.getOperation();

            recordStore.unlock(dataKey, ownerUuid, threadId, callId);

            Record record = recordStore.getRecord(dataKey);
            if (record == null || version == record.getVersion()) {
                RemoveStep.READ.runStepDirect(state);
            }

            if (record == null) {
                operation.wbqCapacityCounter().decrement(transactionId);
            }
        }

        @Override
        public Step nextStep(State state) {
            return RemoveStep.READ.nextStep(state);
        }
    };

    TxnRemoveStep() {
    }
}
