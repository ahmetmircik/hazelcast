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
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.spi.impl.operationservice.Operation;

import static com.hazelcast.map.impl.operation.EntryOperator.operator;
import static com.hazelcast.map.impl.record.Record.UNSET;

// TODO: UPDATE BACKUP!!!
// TODO: what happens if partition is migrating and we offload map load.
public enum EPSingleKeyOpStep implements OpStep {

    EP_START() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> EP_START reading old value" + " (thread:" + Thread.currentThread().getName() + ")");

            EntryOperator operator = operator(state.getOperation(), state.getEntryProcessor());
            operator.init(state.getKey(), null, null, null, null, null, UNSET);
            state.setEntryOperator(operator);

            if (operator.belongsAnotherPartition(state.getKey())) {
                state.setStopExecution(true);
                return;
            }

            GetOpStep.READ.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            if (state.isStopExecution()) {
                return EPSingleKeyOpStep.RESPONSE;
            }
            EPSingleKeyOpStep epStep = state.getOldValue() == null ? EPSingleKeyOpStep.LOAD : EPSingleKeyOpStep.PROCESS;
            OpStep.log(state.getOldValue() == null ? "EPStep.LOAD" : "EPStep.PROCESS");
            return epStep;
        }
    },

    LOAD() {
        @Override
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            assert !ThreadUtil.isRunningOnPartitionThread();

            GetOpStep.LOAD.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return state.getOldValue() == null ? EPSingleKeyOpStep.PROCESS : EPSingleKeyOpStep.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            GetOpStep.ON_LOAD.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return EPSingleKeyOpStep.PROCESS;
        }
    },

    PROCESS() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> processing entry" + " (thread:" + Thread.currentThread().getName() + ")");
            RecordStore recordStore = state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            EntryOperator entryOperator = state.getOperator();
            entryOperator.init(state.getKey(), state.getOldValue(),
                    null, null, null, recordStore.isLocked(state.getKey()), state.getTtl());
            if (!entryOperator.checkCanProceed()) {
                entryOperator.onTouched();
                state.setStopExecution(true);
                return;
            }
            entryOperator.operateOnKeyValueInternal();

            if (!entryOperator.isDidMatchPredicate()) {
                return;
            }

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
                        DeleteOpStep.READ.runStep(state);
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected event found:" + eventType);
                }
            }
        }

        @Override
        public OpStep nextStep(State state) {
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
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            PutOpStep.STORE.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return ON_STORE;
        }
    },

    ON_STORE() {
        @Override
        public void runStep(State state) {
            PutOpStep.ON_STORE.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return RESPONSE;
        }
    },

    DELETE() {
        @Override
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            DeleteOpStep.DELETE.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public void runStep(State state) {
            DeleteOpStep.ON_DELETE.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public void runStep(State state) {
            ThreadUtil.assertRunningOnPartitionThread();

            OpStep.log("==> sending response");

            Operation operation = state.getOperation();
            EntryOperator operator = state.getOperator();
            EntryEventType eventType = operator.getEventType();
            if (eventType != null) {
                switch (eventType) {
                    case ADDED:
                    case UPDATED:
                        operator.onAddedOrUpdated0(state.getNewValue());
                        operator.doPostOperateOps0();
                        break;
                    case REMOVED:
                        operator.onRemove0();
                        operator.doPostOperateOps0();
                        break;
                }
            }

            Data result = operator.getResult();
            operation.sendResponse(result);

            OpStepBackupHandler.handleBackup(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return null;
        }
    };

    EPSingleKeyOpStep() {
    }
}
