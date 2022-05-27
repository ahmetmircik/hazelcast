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

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

public enum GetStep implements Step<State> {

    INITIAL() {
        @Override
        public Runnable createStepWith(State state) {
            throw new UnsupportedOperationException();
        }

        @Override
        public GetStep getNextStep(State state) {
            return READ;
        }
    },

    READ() {
        @Override
        public Runnable createStepWith(State state) {
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
        public GetStep getNextStep(State state) {
            return state.getOldValue() != null ? GetStep.RESPONSE : GetStep.LOAD;
        }
    },

    LOAD() {
        @Override
        public Runnable createStepWith(State state) {
            return () -> {
                System.out.println("==> loading old value");
                state.setOldValue(state.getRecordStore().getMapDataStore().load(state.getKey()));
            };
        }

        @Override
        public GetStep getNextStep(State state) {
            return RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public Runnable createStepWith(State state) {
            return () -> {
                System.out.println("==> sending response");
                RecordStore recordStore = state.getRecordStore();
                MapContainer mapContainer = recordStore.getMapContainer();
                MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();

                state.getOperation().sendResponse(mapServiceContext.toData(state.oldValue));
            };
        }

        @Override
        public GetStep getNextStep(State state) {
            return null;
        }
    };

    GetStep() {
    }
}
