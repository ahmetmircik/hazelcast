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

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.BasePutOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

// TODO: re-run upon NOOME
public enum PutStep implements Step<State> {

    INITIAL() {
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
                    Step.log("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
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
            return state.getOldValue() == null ? PutStep.LOAD : PutStep.RUN_META_OPS;
        }
    },

    LOAD() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                Step.log("==> loading old value" + " (thread:" + Thread.currentThread().getName() + ")");

                StaticParams staticParams = state.getStaticParams();
                if (staticParams.isPutVanilla()) {
                    state.setOldValue(((DefaultRecordStore) state.getRecordStore()).loadValueOf(state.getKey()));
                } else if (staticParams.isPutIfAbsent() || staticParams.isPutIfExists()) {
                    state.setOldValue(state.getRecordStore().getMapDataStore().load(state.getKey()));
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return PutStep.RUN_META_OPS;
        }
    },

    RUN_META_OPS() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    ThreadUtil.assertRunningOnPartitionThread();

                    Step.log("==> meta processing" + " (thread:" + Thread.currentThread().getName() + ")");
                    StaticParams staticParams = state.getStaticParams();
                    Object newValue = state.getNewValue();

                    // For method replace, if current value is not expected one, return.
                    RecordStore recordStore = state.getRecordStore();
                    MapContainer mapContainer = recordStore.getMapContainer();
                    MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

                    if (staticParams.isPutIfAbsent()) {
                        Record record = recordStore.getRecord(state.getKey());
                        if (record == null && state.getOldValue() != null) {
                            record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                                    state.getOldValue(), false, state.getCallerAddress());
                        }

                        if (record != null) {
                            state.setOldValue(record.getValue());
                            state.setStopExecution(true);
                            return;
                        }
                    } else if (staticParams.isPutIfExists()) {
                        Record record = recordStore.getRecord(state.getKey());
                        if (record == null && state.getOldValue() != null) {
                            record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                                    state.getOldValue(), false, state.getCallerAddress());
                        }

                        if (record == null) {
                            state.setOldValue(null);
                            state.setStopExecution(true);
                            return;
                        }

                        newValue = staticParams.isSetTtl() ? record.getValue() : newValue;
                        state.setNewValue(newValue);
                        state.setOldValue(record.getValue());
                    }

                    ValueComparator valueComparator = ((DefaultRecordStore) recordStore).getValueComparator();
                    if (staticParams.isPutIfEqual()
                            && !valueComparator.isEqual(state.getExpect(), state.getOldValue(),
                            mapServiceContext.getNodeEngine().

                                    getSerializationService())) {
                        state.setStopExecution(true);
                        return;
                    }

                    // Intercept put on owner partition.
                    if (!staticParams.isBackup()) {
                        newValue = mapServiceContext.interceptPut(mapContainer.getInterceptorRegistry(),
                                state.getOldValue(), newValue);
                        state.setNewValue(newValue);
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
            // TODO: when isStopExecution what should be next state?
            return state.isStopExecution() ? PutStep.RESPONSE : STORE;
        }
    },

    STORE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
               runStepDirect(state);
            };
        }

        @Override
        public void runStepDirect(State state) {
            Step.log("==> store into map-store" + " (thread:" + Thread.currentThread().getName() + ")");
            Object newValue = ((DefaultRecordStore) state.getRecordStore()).putIntoMapStore2(state.getKey(),
                    state.getNewValue(), state.getTtl(), state.getMaxIdle(), state.getNow(), state.getTxnId());
            state.setNewValue(newValue);
        }

        @Override
        public Step nextStep(State state) {
            return ON_STORE;
        }
    },

    ON_STORE() {
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
            ThreadUtil.assertRunningOnPartitionThread();

            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();

            Step.log("==> write into memory" + " (thread:" + Thread.currentThread().getName() + ")");

            // TODO: read  record efficiently
            Record record = recordStore.getRecord(state.getKey());
            // TODO: fix event type
            if (record == null) {
                recordStore.putMemory(state.getKey(), state.getOldValue(),
                        state.getNewValue(), state.getTtl(), state.getMaxIdle(), state.getExpiryTime(),
                        state.getNow(), EntryEventType.ADDED, state.getStaticParams().isBackup(), null);

            } else {
                recordStore.updateMemory(record, state.getKey(), state.getOldValue(), state.getNewValue(),
                        state.getTtl(), state.getMaxIdle(), state.getExpiryTime(), state.getNow(),
                        state.getStaticParams().isBackup());
            }
        }

        @Override
        public Step nextStep(State state) {
            return RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                Step.log("==> sending response" + " (thread:" + Thread.currentThread().getName() + ")");
                RecordStore recordStore = state.getRecordStore();
                MapContainer mapContainer = recordStore.getMapContainer();
                MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

                StaticParams staticParams = state.getStaticParams();
                Object oldValue = state.getOldValue();

                if (staticParams.isTryPut()) {
                    state.getOperation().sendResponse(true);
                } else if (staticParams.isPutIfEqual() && staticParams.isPutIfExists()) {
                    state.getOperation().sendResponse(oldValue != null);
                } else if (staticParams.isPutVanilla() || staticParams.isPutIfAbsent() || staticParams.isPutIfExists()) {
                    state.getOperation().sendResponse(mapServiceContext.toData(oldValue));
                } else if (staticParams.isPutIfEqual()) {
                    state.getOperation().sendResponse(oldValue != null);
                }
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

                    ((BasePutOperation) state.getOperation()).afterRunInternal0(state.getKey(),
                            state.getNewValue(), state.getOldValue());
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


    PutStep() {
    }
}
