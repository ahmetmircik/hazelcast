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

import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.function.Supplier;

public class StepSupplier implements Supplier<Runnable> {

    private final State state;

    private volatile Step current;

    public StepSupplier(State state, Step initialStep) {
        this.state = state;
        this.current = initialStep;
    }

    @Override
    public Runnable get() {
        if (current == null) {
            return null;
        }
        Step nextStep = current.nextStep(state);
        if (nextStep == null) {
            return null;
        }

        assertStepTypeSameWithInitial(nextStep);

        current = nextStep;
        return createStep(state, nextStep);
    }

    private Runnable createStep(State state, Step step) {
        if (step.offloadStep()) {
            return () -> step.runStep(state);
        }

        return new PartitionSpecificRunnable() {
            @Override
            public void run() {
                step.runStep(state);

                Step temp = step;
                while (true) {
                    Step nextStep = temp.nextStep(state);
                    if (nextStep == null
                            || nextStep.offloadStep()) {
                        current = temp;
                        break;
                    }

                    nextStep.runStep(state);
                    temp = nextStep;
                }
            }

            @Override
            public int getPartitionId() {
                return state.getPartitionId();
            }
        };
    }

    private void assertStepTypeSameWithInitial(Step nextStep) {
//                assert initial.getClass().getSuperclass() == nextStep.getClass().getSuperclass()
//                        : String.format("Expected %s but found %s ", initial.getClass().getSuperclass(),
//                        nextStep.getClass().getSuperclass());
    }

    public static Supplier<Runnable> newStepSupplier(State state, Step initial) {
        return new StepSupplier(state, initial);
    }


}
