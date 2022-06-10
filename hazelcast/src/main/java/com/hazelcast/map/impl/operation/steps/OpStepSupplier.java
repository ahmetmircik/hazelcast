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
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.function.Supplier;

public class OpStepSupplier implements Supplier<Runnable> {

    private final State state;

    private volatile OpStep currentStep;

    public OpStepSupplier(OpStep startingStep, State state) {
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
            currentStep.runStep(state);
            currentStep = currentStep.nextStep(state);
            if (currentStep == null
                    || (currentStep.isOffloadStep() && ThreadUtil.isRunningOnPartitionThread())
                    || (!currentStep.isOffloadStep() && !ThreadUtil.isRunningOnPartitionThread())) {
                break;
            }
        } while (true);
    }

    // TODO remove this method
    private void assertStepTypeSameWithInitial(OpStep nextOpStep) {
//                assert initial.getClass().getSuperclass() == nextStep.getClass().getSuperclass()
//                        : String.format("Expected %s but found %s ", initial.getClass().getSuperclass(),
//                        nextStep.getClass().getSuperclass());
    }

    public static Supplier<Runnable> newOpStepSupplier(OpStep startingStep, State state) {
        return new OpStepSupplier(startingStep, state);
    }


}
