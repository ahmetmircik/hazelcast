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

public class OpStepSupplier implements Supplier<Runnable> {

    private final State state;

    private volatile OpStep current;

    public OpStepSupplier(State state, OpStep initialOpStep) {
        this.state = state;
        this.current = initialOpStep;
    }

    @Override
    public Runnable get() {
        if (current == null) {
            return null;
        }
        OpStep nextOpStep = current.nextStep(state);
        if (nextOpStep == null) {
            return null;
        }

        assertStepTypeSameWithInitial(nextOpStep);

        current = nextOpStep;
        return createStep(state, nextOpStep);
    }

    private Runnable createStep(State state, OpStep opStep) {
        if (opStep.offloadStep()) {
            return () -> opStep.runStep(state);
        }

        return new PartitionSpecificRunnable() {
            @Override
            public void run() {
                opStep.runStep(state);

                OpStep temp = opStep;
                while (true) {
                    OpStep nextOpStep = temp.nextStep(state);
                    if (nextOpStep == null
                            || nextOpStep.offloadStep()) {
                        current = temp;
                        break;
                    }

                    nextOpStep.runStep(state);
                    temp = nextOpStep;
                }
            }

            @Override
            public int getPartitionId() {
                return state.getPartitionId();
            }
        };
    }

    private void assertStepTypeSameWithInitial(OpStep nextOpStep) {
//                assert initial.getClass().getSuperclass() == nextStep.getClass().getSuperclass()
//                        : String.format("Expected %s but found %s ", initial.getClass().getSuperclass(),
//                        nextStep.getClass().getSuperclass());
    }

    public static Supplier<Runnable> newStepSupplier(State state, OpStep initial) {
        return new OpStepSupplier(state, initial);
    }


}
