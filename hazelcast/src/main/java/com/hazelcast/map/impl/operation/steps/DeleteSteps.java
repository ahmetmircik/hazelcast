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

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.BaseRemoveOperation;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;

public enum DeleteSteps implements Step {

    READ() {
        @Override
        public void runStep(State state) {
            assertRunningOnPartitionThread();

            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            MapContainer mapContainer = recordStore.getMapContainer();
            MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

            Record record = recordStore.getRecordOrNull(state.getKey());
            state.setOldValue(record == null ? null : record.getValue());
            state.setRecordNull(record == null);

            if (record != null) {
                Object oldValue = record.getValue();
                oldValue = mapServiceContext.interceptRemove(mapContainer.getInterceptorRegistry(), oldValue);
                if (oldValue != null) {
                    state.setOldValue(oldValue);
                }
            }
        }

        @Override
        public Step nextStep(State state) {
            return DeleteSteps.DELETE;
        }
    },

    DELETE() {
        @Override
        public boolean isOffloadStep() {
            return true;
        }

        @Override
        public void runStep(State state) {
            assert !isRunningOnPartitionThread();

            DefaultRecordStore recordStore = ((DefaultRecordStore) state.getRecordStore());
            if (recordStore.persistenceEnabledFor(state.getCallerProvenance())) {
                MapDataStore mapDataStore = recordStore.getMapDataStore();
                mapDataStore.remove(state.getKey(), state.getNow(), state.getTxnId());
            }
        }

        @Override
        public Step nextStep(State state) {
            return state.isNullRecord() ? DeleteSteps.AFTER_RUN : DeleteSteps.ON_DELETE;
        }
    },

    ON_DELETE() {
        @Override
        public void runStep(State state) {
            assertRunningOnPartitionThread();

            DefaultRecordStore recordStore = (DefaultRecordStore) state.getRecordStore();
            Record record = recordStore.getRecord(state.getKey());
            recordStore.removeRecord0(state.getKey(), record);
            recordStore.onStore(record);
        }

        @Override
        public Step nextStep(State state) {
            return DeleteSteps.AFTER_RUN;
        }
    },

    AFTER_RUN() {
        @Override
        public void runStep(State state) {
            assertRunningOnPartitionThread();

            // TODO: prevent unneeded serialization for oldValue
            if (!state.isNullRecord()) {
                ((BaseRemoveOperation) state.getOperation()).afterRunInternal0(state.getKey(), state.getOldValue());
            }
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    };

    DeleteSteps() {
    }
}
