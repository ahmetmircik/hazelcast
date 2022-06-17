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

import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.Queue;

import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR;

public class StepRunner implements PartitionSpecificRunnable {

    private final int partitionId;
    private final Queue<StepSupplier> offloadQueue;
    private final OperationExecutor operationExecutor;
    private final ManagedExecutorService executor;

    private volatile StepSupplier stepSupplier;

    public StepRunner(NodeEngine nodeEngine,
                      Queue<StepSupplier> offloadQueue, int partitionId) {
        this.offloadQueue = offloadQueue;
        this.partitionId = partitionId;

        this.operationExecutor = ((OperationServiceImpl) nodeEngine
                .getOperationService()).getOperationExecutor();
        this.executor = nodeEngine.getExecutionService()
                .getExecutor(MAP_STORE_OFFLOADABLE_EXECUTOR);

        initStepSupplier(offloadQueue.peek());
    }

    @Override
    public void run() {
        try {
            Runnable step;
            do {
                if (stepSupplier == null || (step = stepSupplier.get()) == null) {
                    if (isRunningOnPartitionThread()) {
                        pollCurrentAndPeekNextStepSupplier();
                        if (stepSupplier == null) {
                            return;
                        }
                        continue;
                    } else {
                        operationExecutor.execute(this);
                        return;
                    }
                }

                if (!runDirect(step)) {
                    offloadRun(step, this);
                    return;
                }
            } while (true);
        } catch (Throwable t) {
            stepSupplier.handleOperationError(t);
        }
    }

    private void pollCurrentAndPeekNextStepSupplier() {
        assert ThreadUtil.isRunningOnPartitionThread()
                : Thread.currentThread().getName();
        assert offloadQueue.peek() == stepSupplier;

        offloadQueue.poll();
        initStepSupplier(offloadQueue.peek());
    }

    private boolean runDirect(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            if (isRunningOnPartitionThread()) {
                step.run();
                return true;
            }
        } else {
            if (!isRunningOnPartitionThread()) {
                step.run();
                return true;
            }
        }
        return false;
    }

    private void offloadRun(Runnable step,
                            PartitionSpecificRunnable offload) {
        if (step instanceof PartitionSpecificRunnable) {
            operationExecutor.execute(offload);
        } else {
            executor.execute(offload);
        }
    }

    public StepRunner initStepSupplier(StepSupplier stepSupplier) {
        this.stepSupplier = stepSupplier;
        return this;

    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
