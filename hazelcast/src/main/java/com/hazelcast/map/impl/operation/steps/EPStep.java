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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;
import static com.hazelcast.map.impl.record.Record.UNSET;

// TODO: what happens if partition is migrating and we offload map load.
public enum EPStep implements Step<State> {

    INITIAL() {
        @Override
        public Step nextStep(State state) {
            return EP_START;
        }
    },

    EP_START() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    System.out.println("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");

                    EntryOperator operator = operator(state.getOperation(), state.getEntryProcessor());
                    operator.init(state.getKey(), null, null, null, null, null, UNSET);
                    state.setEntryOperator(operator);

                    if (operator.belongsAnotherPartition(state.getKey())) {
                        state.setStopExecution(true);
                        return;
                    }

                    GetStep.READ.runStepDirect(state);
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            if (state.isStopExecution()) {
                return EPStep.RESPONSE;
            }
            return state.getOldValue() == null ? EPStep.LOAD : EPStep.PROCESS;
        }
    },

    LOAD() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                GetStep.LOAD.runStepDirect(state);
            };
        }

        @Override
        public Step nextStep(State state) {
            return state.getOldValue() == null ? EPStep.PROCESS : EPStep.ON_LOAD;
        }
    },


    ON_LOAD() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    GetStep.ON_LOAD.runStepDirect(state);
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return EPStep.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    System.out.println("==> processing entry" + " (thread:" + Thread.currentThread().getName() + ")");
                    RecordStore recordStore = state.getRecordStore();
                    MapContainer mapContainer = recordStore.getMapContainer();
                    MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

                    EntryOperator entryOperator = state.getOperator();
                    entryOperator.init(state.getKey(), state.getOldValue(), null, null, null, null, UNSET);
                    entryOperator.operateOnKeyValueInternal0();

                    EntryEventType eventType = entryOperator.getEventType();
                    if (eventType == null) {
                        entryOperator.onTouched();
                        state.setStopExecution(true);
                    } else {
                        switch (eventType) {
                            case ADDED:
                            case UPDATED:
                                state.setStaticPutParams(StaticParams.SET_WTH_NO_ACCESS_PARAMS);
                                if (eventType == EntryEventType.UPDATED) {
                                    entryOperator.onTouched();
                                }
                                Object newValue = entryOperator.extractNewValue();
                                newValue = mapServiceContext.interceptPut(mapContainer.getInterceptorRegistry(),
                                        state.getOldValue(), newValue);
                                state.setNewValue(newValue);
                                state.setTtl(entryOperator.getEntry().getNewTtl());
                                break;
                            case REMOVED:
                                DeleteStep.READ.runStepDirect(state);
                                break;
                            default:
                                throw new IllegalArgumentException("Unexpected event found:" + eventType);
                        }
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
            EntryEventType eventType = state.getOperator().getEventType();
            if (eventType == null) {
                return RESPONSE;
            }

            switch (eventType) {
                case ADDED:
                case UPDATED:
                    return STORE;
                case REMOVED:
                    return DELETE;
                default:
                    throw new IllegalArgumentException("Unexpected event found:" + eventType);
            }
        }
    },

    STORE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                PutStep.STORE.runStepDirect(state);
            };
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
                    PutStep.ON_STORE.runStepDirect(state);

                    EntryOperator operator = state.getOperator();
                    operator.onAddedOrUpdated0(state.getNewValue());
                    operator.doPostOperateOps0();
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public Step nextStep(State state) {
            return RESPONSE;
        }
    },

    DELETE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                DeleteStep.DELETE.runStepDirect(state);
            };
        }

        @Override
        public Step nextStep(State state) {
            return ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    DeleteStep.ON_DELETE.runStepDirect(state);

                    EntryOperator operator = state.getOperator();
                    operator.onRemove0();
                    operator.doPostOperateOps0();
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
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
                System.out.println("==> sending response");

                Operation operation = state.getOperation();
                Data result = state.getOperator().getResult();
                operation.sendResponse(result);
            };
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    };

    EPStep() {
    }
}
