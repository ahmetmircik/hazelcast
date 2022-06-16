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

import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;
import com.hazelcast.map.impl.operation.steps.engine.StepResponseHandler;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;

public enum ResponseSteps implements Step {

    SEND_RESPONSE() {
        @Override
        public void runStep(State state) {
            assertRunningOnPartitionThread();

            StepResponseHandler.sendResponse(state);
        }

        @Override
        public Step nextStep(State state) {
            return AFTER_RUN;
        }
    },

    AFTER_RUN() {
        @Override
        public void runStep(State state) {
            assertRunningOnPartitionThread();

            MapOperation operation = state.getOperation();
            operation.afterRunInternal();
            operation.disposeDeferredBlocks();
        }

        @Override
        public Step nextStep(State state) {
            return null;
        }
    };
}
