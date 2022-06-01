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
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;

// TODO: what happens if partition is migrating and we offload map load.
public enum GetStep implements Step<State> {

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
                    System.out.println("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
                    RecordStore recordStore = state.getRecordStore();
                    Record record = recordStore.getRecordOrNull(state.getKey());
                    if (record != null) {
                        state.setOldValue(record.getValue());
                    }
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null ? GetStep.LOAD : GetStep.RESPONSE;
        }
    },

    LOAD() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                System.out.println("==> loading old value");
                state.setOldValue(state.getRecordStore().getMapDataStore().load(state.getKey()));
            };
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null ? GetStep.RESPONSE : GetStep.ON_LOAD;
        }
    },


    ON_LOAD() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    System.out.println("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
                    RecordStore recordStore = state.getRecordStore();
                    Record record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                            state.getOldValue(), false, state.getCallerAddress());
                    state.setOldValue(record == null ? null : record.getValue());
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return GetStep.RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                System.out.println("==> sending response");

                Operation operation = state.getOperation();
                Object oldValue = state.getOldValue();
                Data result = ((GetOperation) operation).extractResult(oldValue);
                state.setResult(result);

                operation.sendResponse(result);
            };
        }

        @Override
        public Step nextStep(State state) {
            return AFTER_RUN;
        }
    },

    AFTER_RUN() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    ThreadUtil.assertRunningOnPartitionThread();

                    ((GetOperation) state.getOperation()).afterRunInternal0((Data) state.getResult());
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    };

    GetStep() {
    }
}
