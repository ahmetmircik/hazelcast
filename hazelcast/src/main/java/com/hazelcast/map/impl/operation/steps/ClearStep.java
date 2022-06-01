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
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.operation.ClearOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

// TODO: what happens if partition is migrating and we offload map load.
public enum ClearStep implements Step<State> {

    INITIAL() {
        @Override
        public Runnable createStep(State state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Step nextStep(State state) {
            return CLEAR_MEMORY;
        }
    },

    CLEAR_MEMORY() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    System.out.println("==> clearing memory" + " (thread:" + Thread.currentThread().getName() + ")");
                    DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
                    recordStore.checkIfLoaded();

                    ArrayList<Data> keys = new ArrayList<>();
                    ArrayList<Record> records = new ArrayList<>();
                    // we don't remove locked keys. These are clearable records.
                    recordStore.forEach(new BiConsumer<Data, Record>() {
                        final Set<Data> lockedKeySet = recordStore.getLockStore().getLockedKeys();

                        @Override
                        public void accept(Data dataKey, Record record) {
                            if (lockedKeySet != null && !lockedKeySet.contains(dataKey)) {
                                keys.add(dataKey);
                                records.add(record);
                            }

                        }
                    }, false);

                    state.setKeys(keys);
                    state.setRecords(records);
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public ClearStep nextStep(State state) {
            return ClearStep.CLEAR_MAP_STORE;
        }
    },

    CLEAR_MAP_STORE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                System.out.println("==> clearing map store");
                DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
                List<Data> keys = state.getKeys();
                recordStore.getMapDataStore().removeAll(keys);
            };
        }

        @Override
        public ClearStep nextStep(State state) {
            return ClearStep.ON_CLEAR;
        }
    },


    ON_CLEAR() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    System.out.println("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
                    DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
                    int removedKeyCount = recordStore.removeBulk(state.getKeys(), state.getRecords());
                    if (removedKeyCount > 0) {
                        recordStore.updateStatsOnRemove(Clock.currentTimeMillis());
                    }
                    state.setResult(removedKeyCount);
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public ClearStep nextStep(State state) {
            return ClearStep.RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                System.out.println("==> clear all sending response");

                state.getOperation().sendResponse(state.getResult());
            };
        }

        @Override
        public ClearStep nextStep(State state) {
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

                    ((ClearOperation) state.getOperation()).afterRunInternal0(((int) state.getResult()));
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public ClearStep nextStep(State state) {
            return null;
        }
    };

    ClearStep() {
    }
}
