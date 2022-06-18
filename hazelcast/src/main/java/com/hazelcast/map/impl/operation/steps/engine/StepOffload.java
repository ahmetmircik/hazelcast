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
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Queue;

public class StepOffload extends Offload {

    public StepOffload(MapOperation mapOperation) {
        super(mapOperation);

        getOffloadQueue().add(mapOperation);
    }

    @Override
    public void start() {
        Operation operation = offloadedOperation();
        int partitionId = operation.getPartitionId();
        Queue<MapOperation> offloadQueue = getOffloadQueue();
        if (offloadQueue.peek() == operation) {
            new StepRunner(offloadedOperation().getNodeEngine(),
                    offloadQueue, partitionId).run();
        }

    }

    private Queue<MapOperation> getOffloadQueue() {
        MapOperation mapOperation = (MapOperation) offloadedOperation();
        return mapOperation.getMapContainer().getMapServiceContext()
                .getPartitionContainer(mapOperation.getPartitionId())
                .getOffloadQueue(mapOperation.getName());
    }
}
