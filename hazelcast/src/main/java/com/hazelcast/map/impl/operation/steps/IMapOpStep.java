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

import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.operation.steps.engine.State;
import com.hazelcast.map.impl.operation.steps.engine.Step;

import static com.hazelcast.spi.impl.executionservice.ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR;

/**
 * {@link Step} specialized for {@link
 * com.hazelcast.map.IMap} operations.
 */
public interface IMapOpStep extends Step<State> {

    default boolean isOffloadStep(State state) {
        if (isLoad()) {
            return true;
        }

        if (isStore()) {
            return !isWriteBehind(state);
        }

        return false;
    }

    default boolean isWriteBehind(State state) {
        return state.getRecordStore().getMapDataStore()
                instanceof WriteBehindStore;
    }

    default boolean isLoad() {
        return false;
    }

    default boolean isStore() {
        return false;
    }

    default String getExecutorName(State state) {
        return MAP_STORE_OFFLOADABLE_EXECUTOR;
    }
}
