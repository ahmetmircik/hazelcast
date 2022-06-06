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

import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.BaseRemoveOperation;
import com.hazelcast.map.impl.operation.TryRemoveOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;

public enum RemoveStep implements Step<State> {

    INITIAL() {
        @Override
        public RemoveStep nextStep(State state) {
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
            Step.log("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            Record record = recordStore.getRecordOrNull(state.getKey());
            state.setRecordNull(record == null);

            if (record != null) {
                Object oldValue = record.getValue();
                oldValue = mapServiceContext.interceptRemove(mapContainer.getInterceptorRegistry(), oldValue);
                state.setOldValue(oldValue);
            }
        }

        @Override
        public RemoveStep nextStep(State state) {
            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            return state.isNullRecord() ? RemoveStep.LOAD
                    : (recordStore.persistenceEnabledFor(state.getCallerProvenance())
                    ? RemoveStep.DELETE : RemoveStep.RESPONSE);
        }
    },

    LOAD() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                Step.log("==> loading old value");
                DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
                Object oldValue = recordStore.loadValueOf(state.getKey());
                if (oldValue != null && recordStore.persistenceEnabledFor(state.getCallerProvenance())) {
                    recordStore.getMapDataStore().remove(state.getKey(), state.getNow(), state.getTxnId());
                    state.setOldValue(oldValue);
                }
            };
        }

        @Override
        public RemoveStep nextStep(State state) {
            return state.getOldValue() == null ? RemoveStep.RESPONSE : RemoveStep.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    Step.log("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
                    DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
                    recordStore.updateStatsOnRemove(state.getNow());
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public RemoveStep nextStep(State state) {
            return RemoveStep.RESPONSE;
        }
    },

    DELETE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                Step.log("==> deleting " + " (thread:" + Thread.currentThread().getName() + ")");
                MapDataStore mapDataStore = state.getRecordStore().getMapDataStore();
                mapDataStore.remove(state.getKey(), state.getNow(), state.getTxnId());
            };
        }

        @Override
        public RemoveStep nextStep(State state) {
            return RemoveStep.ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public Runnable createStep(State state) {
            return new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    ThreadUtil.assertRunningOnPartitionThread();

                    Step.log("==> on delete" + " (thread:" + Thread.currentThread().getName() + ")");
                    DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
                    Record record = recordStore.getRecord(state.getKey());
                    recordStore.removeRecord0(state.getKey(), record);
                    recordStore.onStore(record);
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public RemoveStep nextStep(State state) {
            return RemoveStep.RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public Runnable createStep(State state) {
            return () -> {
                Step.log("==> sending response" + " (thread:" + Thread.currentThread().getName() + ")");
                DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
                MapServiceContext mapServiceContext = recordStore.getMapContainer().getMapServiceContext();
                Object oldValue = state.getOldValue();
                Operation operation = state.getOperation();
                state.getOperation().sendResponse(operation instanceof TryRemoveOperation
                        ? oldValue != null : mapServiceContext.toData(oldValue));
            };
        }

        @Override
        public RemoveStep nextStep(State state) {
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

                    // TODO: prevent unneeded serialization for oldValue
                    Object oldValue = state.getOldValue();
                    if (oldValue != null) {
                        ((BaseRemoveOperation) state.getOperation()).afterRunInternal0(state.getKey(), oldValue);
                    }
                }

                @Override
                public int getPartitionId() {
                    return state.getPartitionId();
                }
            };
        }

        @Override
        public RemoveStep nextStep(State state) {
            return null;
        }
    };


    RemoveStep() {
    }
}
