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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.comparators.ValueComparator;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.BaseRemoveOperation;
import com.hazelcast.map.impl.operation.steps.engine.OpStep;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;

public enum RemoveIfSameOpStep implements OpStep {

    READ() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
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
        public RemoveIfSameOpStep nextStep(State state) {
            return state.isNullRecord() ? RemoveIfSameOpStep.LOAD : RemoveIfSameOpStep.ON_LOAD;
        }
    },

    LOAD() {
        @Override
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            OpStep.log("==> loading old value");
            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            Object oldValue = recordStore.loadValueOf(state.getKey());
            if (oldValue != null) {
                recordStore.getMapDataStore().remove(state.getKey(), state.getNow(), state.getTxnId());
                state.setOldValue(oldValue);
            } else {
                state.setResult(false);
            }
        }

        @Override
        public RemoveIfSameOpStep nextStep(State state) {
            return state.getOldValue() == null
                    ? RemoveIfSameOpStep.AFTER_RUN : RemoveIfSameOpStep.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> on load" + " (thread:" + Thread.currentThread().getName() + ")");

            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
            SerializationService serializationService = mapServiceContext
                    .getNodeEngine().getSerializationService();
            ValueComparator valueComparator = recordStore.getValueComparator();

            Object oldValue = state.getOldValue();
            if (valueComparator.isEqual(state.getExpect(), oldValue, serializationService)) {
                mapServiceContext.interceptRemove(mapContainer.getInterceptorRegistry(), oldValue);
            } else {
                state.setResult(false);
                state.setStopExecution(true);
            }
        }

        @Override
        public RemoveIfSameOpStep nextStep(State state) {
            return state.isStopExecution()
                    ? RemoveIfSameOpStep.AFTER_RUN : RemoveIfSameOpStep.DELETE;
        }
    },

    DELETE() {
        @Override
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            OpStep.log("==> deleting " + " (thread:" + Thread.currentThread().getName() + ")");
            MapDataStore mapDataStore = state.getRecordStore().getMapDataStore();
            mapDataStore.remove(state.getKey(), state.getNow(), state.getTxnId());
        }

        @Override
        public RemoveIfSameOpStep nextStep(State state) {
            return RemoveIfSameOpStep.ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public void runStep(State state) {
            ThreadUtil.assertRunningOnPartitionThread();

            OpStep.log("==> on delete" + " (thread:" + Thread.currentThread().getName() + ")");

            if (!state.isNullRecord()) {
                DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
                Record record = recordStore.getRecord(state.getKey());
                recordStore.removeRecord0(state.getKey(), record);
                recordStore.onStore(record);
                recordStore.updateStatsOnRemove(state.getNow());
            }

            state.setResult(true);
        }

        @Override
        public RemoveIfSameOpStep nextStep(State state) {
            return RemoveIfSameOpStep.AFTER_RUN;
        }
    },

    AFTER_RUN() {
        @Override
        public void runStep(State state) {
            ThreadUtil.assertRunningOnPartitionThread();

            // TODO: prevent unneeded serialization for oldValue
            if (((Boolean) state.getResult())) {
                ((BaseRemoveOperation) state.getOperation()).afterRunInternal0(state.getKey(),
                        state.getOldValue());
            }
        }

        @Override
        public RemoveIfSameOpStep nextStep(State state) {
            return null;
        }
    };


    RemoveIfSameOpStep() {
    }
}
