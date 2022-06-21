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

// TODO: what happens if partition is migrating and we offload map load.
public class StepOffload extends Offload {

    private final StepRunner stepRunner;

    public StepOffload(MapOperation mapOperation) {
        super(mapOperation);

        Queue<MapOperation> offloadQueue = getOffloadQueue(mapOperation);
        offloadQueue.offer(mapOperation);

        this.stepRunner = offloadQueue.peek() == mapOperation
                ? new StepRunner(offloadedOperation().getNodeEngine(),
                offloadQueue, mapOperation.getPartitionId()) : null;
    }

    @Override
    public void start() {
        if (stepRunner != null) {
            stepRunner.run();
        }
    }

    private static Queue<MapOperation> getOffloadQueue(MapOperation mapOperation) {
        return mapOperation.getMapContainer().getMapServiceContext()
                .getPartitionContainer(mapOperation.getPartitionId())
                .getOffloadQueue(mapOperation.getName());
    }
}
