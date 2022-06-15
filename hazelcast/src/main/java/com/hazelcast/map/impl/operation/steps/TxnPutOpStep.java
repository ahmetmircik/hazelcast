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
import com.hazelcast.map.impl.operation.steps.engine.OpStep;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;

import java.util.UUID;

// TODO: re-run upon NOOME
public enum TxnPutOpStep implements OpStep {

    READ() {
        @Override
        public void runStep(State state) {
            OpStep.log("TxnPutOpStep ==> reading old value"
                    + " (thread:" + Thread.currentThread().getName() + ")");
            RecordStore recordStore = state.getRecordStore();

            Data dataKey = state.getKey();
            long threadId = state.getThreadId();
            long version = state.getVersion();
            UUID ownerUuid = state.getOwnerUuid();
            long callId = state.getOperation().getCallId();

            recordStore.unlock(dataKey, ownerUuid, threadId, callId);

            Record record = recordStore.getRecordOrNull(dataKey);
            if (record == null || version == record.getVersion()) {
                PutOpStep.READ.runStep(state);

                state.setEntryEventType(record == null
                        ? EntryEventType.ADDED : EntryEventType.UPDATED);
            }
        }

        @Override
        public OpStep nextStep(State state) {
            return PutOpStep.READ.nextStep(state);
        }
    };

    TxnPutOpStep() {
    }
}
