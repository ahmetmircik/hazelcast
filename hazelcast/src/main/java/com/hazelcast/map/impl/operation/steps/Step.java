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

import javax.annotation.Nullable;

public interface Step {

    default void runStep(State state) {
        throw new UnsupportedOperationException();
    }

    /**
     * Idempotent next step finder.
     *
     * @param state state
     * @return next step or null if there is no next step.
     */
    @Nullable
    default Step nextStep(State state) {
        throw new UnsupportedOperationException();
    }

    default boolean offloadStep() {
        return false;
    }

    static void log(String msg) {
        //System.out.println("msg = " + msg);
    }
}
