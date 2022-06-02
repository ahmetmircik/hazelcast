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

import java.util.function.Supplier;

public interface Step<State> {

    Runnable createStep(State state);

    Step nextStep(State state);

    default void runStepDirect(State state) {
        return;
    }


    static Supplier<Runnable> newStepSupplier(com.hazelcast.map.impl.operation.steps.State state,
                                              Step initial) {
        return new Supplier<Runnable>() {
            private volatile Step current = initial;

            @Override
            public Runnable get() {
                Step nextStep = current.nextStep(state);
                if (nextStep == null) {
                    return null;
                }

                assertStepTypeSameWithInitial(nextStep);

                current = nextStep;
                return nextStep.createStep(state);
            }

            private void assertStepTypeSameWithInitial(Step nextStep) {
                assert initial.getClass().getSuperclass() == nextStep.getClass().getSuperclass()
                        : String.format("Expected %s but found %s ", initial.getClass().getSuperclass(),
                        nextStep.getClass().getSuperclass());
            }
        };
    }
}
