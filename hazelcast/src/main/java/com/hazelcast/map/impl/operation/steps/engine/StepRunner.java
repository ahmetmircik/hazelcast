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
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class StepRunner implements PartitionSpecificRunnable {

    private static final long DEFAULT_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = TimeUnit.MILLISECONDS.toNanos(10);
    private static final String PROP_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = "hazelcast.internal.map.mapstore.offloaded.max.successive.operation.run.nanos";
    private static final HazelcastProperty MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS
            = new HazelcastProperty(PROP_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS,
            DEFAULT_MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS, NANOSECONDS);

    private final int partitionId;
    private final long maxRunNanos;
    private final Queue<MapOperation> offloadQueue;
    private final OperationExecutor operationExecutor;
    private final ManagedExecutorService executor;

    private volatile StepSupplier stepSupplier;
    private volatile MapOperation currentOperation;

    public StepRunner(NodeEngine nodeEngine,
                      Queue<MapOperation> offloadQueue, int partitionId) {
        this.offloadQueue = offloadQueue;
        this.currentOperation = offloadQueue.peek();
        this.partitionId = partitionId;

        this.operationExecutor = ((OperationServiceImpl) nodeEngine
                .getOperationService()).getOperationExecutor();
        this.executor = nodeEngine.getExecutionService()
                .getExecutor(MAP_STORE_OFFLOADABLE_EXECUTOR);
        this.maxRunNanos = nodeEngine.getProperties()
                .getNanos(MAX_SUCCESSIVE_OFFLOADED_OP_RUN_NANOS);
    }

    @SuppressWarnings("checkstyle:innerassignment")
    @Override
    public void run() {
        long start = System.nanoTime();
        Runnable step;
        do {
            if (stepSupplier == null || (step = stepSupplier.get()) == null) {
                if (isRunningOnPartitionThread()) {
                    removeCurrentAndSetNextStepSupplier();
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

            // force free partition thread after a while
            // maxRunNanos=0 disables this.
            if (maxRunNanos > 0 && isRunningOnPartitionThread()
                    && System.nanoTime() - start >= maxRunNanos) {
                offloadRun(step, this);
                return;
            }

        } while (true);
    }

    private void removeCurrentAndSetNextStepSupplier() {
        assert ThreadUtil.isRunningOnPartitionThread()
                : Thread.currentThread().getName();
        assert offloadQueue.peek() == currentOperation;

        if (stepSupplier != null) {
            offloadQueue.poll();
        }
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

    public StepRunner initStepSupplier(MapOperation operation) {
        this.currentOperation = operation;
        this.stepSupplier = operation != null
                ? new StepSupplier(operation) : null;
        return this;

    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
