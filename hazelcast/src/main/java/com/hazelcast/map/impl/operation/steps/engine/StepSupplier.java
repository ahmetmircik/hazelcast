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

package com.hazelcast.map.impl.operation.steps.engine;

import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.map.impl.operation.steps.ResponseSteps;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.function.Supplier;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.map.impl.operation.ForcedEviction.runStepWithForcedEvictionStrategies;

public class StepSupplier implements Supplier<Runnable> {

    private final State state;

    private volatile Step currentStep;

    public StepSupplier(Step startingStep, State state) {
        this.state = state;
        this.currentStep = startingStep;
    }

    @Override
    public Runnable get() {
        if (currentStep == null) {
            return null;
        }

        return createStep(state);
    }

    private Runnable createStep(State state) {
        if (currentStep.isOffloadStep()) {
            return () -> runStepWith(state);
        }

        return new PartitionSpecificRunnable() {
            @Override
            public void run() {
                runStepWith(state);
            }

            @Override
            public int getPartitionId() {
                return state.getPartitionId();
            }
        };
    }

    private void runStepWith(State state) {
        do {
            try {
                currentStep.runStep(state);
                currentStep = currentStep.nextStep(state);
            } catch (NativeOutOfMemoryError e) {
                assertRunningOnPartitionThread();

                rerunWithForcedEviction(() -> currentStep.runStep(state));
            } catch (Throwable throwable){
                state.setThrowable(throwable);
                currentStep = ResponseSteps.HANDLE_ERROR;
            }

            if (currentStep == null
                    || (currentStep.isOffloadStep() && ThreadUtil.isRunningOnPartitionThread())
                    || (!currentStep.isOffloadStep() && !ThreadUtil.isRunningOnPartitionThread())) {
                break;
            }
        } while (true);
    }

    private void rerunWithForcedEviction(Runnable step) {
        try {
            runStepWithForcedEvictionStrategies(state.getOperation(), step);
        } catch (NativeOutOfMemoryError e) {
            state.getOperation().disposeDeferredBlocks();
            throw e;
        }
    }
}
