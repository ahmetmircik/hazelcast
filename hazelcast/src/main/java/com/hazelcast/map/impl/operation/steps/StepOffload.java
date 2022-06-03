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

import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.spi.impl.executionservice.impl.ExecutionServiceImpl;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.Executor;

public class StepOffload extends Offload {

    private final StepRunner runner;
    private final String executorName;

    public StepOffload(Operation offloadedOperation,
                       String mapName, StepRunner runner) {
        super(offloadedOperation);
        this.runner = runner;
        this.executorName = String.format("%s-%d-%s", mapName, Thread.currentThread().getId(),
                StepOffload.class.getName());
    }

    @Override
    public void start() {
        getOrCreateExecutor().execute(runner);
    }

    /**
     * @return always created by a single thread
     */
    private Executor getOrCreateExecutor() {
        ExecutionServiceImpl service = ((ExecutionServiceImpl) nodeEngine.getExecutionService());
        if (!service.hasExecutorWithName(executorName)) {
            service.register(executorName, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
            Step.log("---> New executorName = " + executorName);
        }
        return service.getExecutor(executorName);
    }
}
