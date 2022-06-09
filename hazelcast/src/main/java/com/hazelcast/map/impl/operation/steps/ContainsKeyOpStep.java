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

import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.DefaultRecordStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.operationservice.Operation;

// TODO: what happens if partition is migrating and we offload map load.
public enum ContainsKeyOpStep implements OpStep {

    // TODO add touch record test?
    READ() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> reading old value" + " (thread:" + Thread.currentThread().getName() + ")");
            RecordStore recordStore = state.getRecordStore();
            Record record = recordStore.getRecordOrNull(state.getKey());
            if (record != null) {
                state.setOldValue(record.getValue());
                recordStore.accessRecord(state.getKey(), record, state.getNow());
            }
        }

        @Override
        public OpStep nextStep(State state) {
            return state.getOldValue() == null
                    ? ContainsKeyOpStep.LOAD : ContainsKeyOpStep.RESPONSE;
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

            GetOpStep.LOAD.runStep(state);
        }

        @Override
        public OpStep nextStep(State state) {
            return state.getOldValue() == null
                    ? ContainsKeyOpStep.RESPONSE : ContainsKeyOpStep.ON_LOAD;
        }
    },

    ON_LOAD() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> loading old value" + " (thread:" + Thread.currentThread().getName() + ")");
            RecordStore recordStore = state.getRecordStore();
            Record record = ((DefaultRecordStore) recordStore).onLoadRecord(state.getKey(),
                    state.getOldValue(), false, state.getCallerAddress());
            record = recordStore.evictIfExpired(state.getKey(), state.getNow(), false) ? null : record;
            state.setOldValue(record == null ? null : record.getValue());
        }

        @Override
        public OpStep nextStep(State state) {
            return ContainsKeyOpStep.RESPONSE;
        }
    },

    RESPONSE() {
        @Override
        public void runStep(State state) {
            OpStep.log("==> sending response");

            Operation operation = state.getOperation();
            operation.sendResponse(state.getOldValue() != null);
        }

        @Override
        public OpStep nextStep(State state) {
            return null;
        }
    };

    ContainsKeyOpStep() {
    }
}
