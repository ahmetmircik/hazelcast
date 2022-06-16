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

import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.spi.impl.operationservice.Offload;

import java.util.Queue;

public class StepOffload extends Offload {

    private final StepRunner runner;
    private final Queue<StepRunner> offloadQueue;

    public StepOffload(MapOperation offloadedOperation,
                       String mapName, State state, Step startingStep) {
        super(offloadedOperation);
        this.runner = new StepRunner(offloadedOperation, state,
                startingStep, mapName);
        this.offloadQueue = offloadedOperation.getMapContainer().getMapServiceContext()
                .getPartitionContainer(state.getPartitionId())
                .getOffloadQueue(mapName);
    }

    @Override
    public void start() {
        offloadQueue.offer(runner);

        if (offloadQueue.peek() == runner) {
            runner.run();
        }
    }
}
