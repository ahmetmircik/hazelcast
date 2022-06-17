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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.map.impl.operation.ForcedEviction.runStepWithForcedEvictionStrategies;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.MAP_STORE_OFFLOADABLE_EXECUTOR;

public class StepRunner implements PartitionSpecificRunnable {

    private final int partitionId;
    private final Node node;
    private final State state;
    private final ILogger logger;
    private final Executor executor;
    private final Operation operation;
    private final Queue<StepRunner> offloadQueue;
    private final Supplier<Runnable> stepSupplier;
    private final OperationExecutor operationExecutor;

    public StepRunner(MapOperation operation,
                      State state,
                      Step startingStep,
                      String mapName) {
        assertRunningOnPartitionThread();
        this.state = state;
        this.operation = operation;
        NodeEngine nodeEngine = operation.getNodeEngine();
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        this.logger = nodeEngine.getLogger(getClass());
        this.operationExecutor = ((OperationServiceImpl) nodeEngine
                .getOperationService()).getOperationExecutor();
        this.stepSupplier = new StepSupplier(startingStep, state);
        this.executor = nodeEngine.getExecutionService()
                .getExecutor(MAP_STORE_OFFLOADABLE_EXECUTOR);
        this.partitionId = state.getPartitionId();
        this.offloadQueue = operation.getMapContainer().getMapServiceContext()
                .getPartitionContainer(state.getPartitionId())
                .getOffloadQueue(mapName);
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    // TODO: What happens in the event of exception?
    @Override
    public void run() {
        try {
            Runnable step;
            do {
                step = stepSupplier.get();
                if (step == null) {
                    startNextStepRunner();
                    return;
                }

                if (!runDirect(step)) {
                    offloadRun(step, this);
                    return;
                }
            } while (true);
        } catch (Throwable t) {
            handleErrorAndStartNexStepRunner(t);
        }
    }

    private void handleErrorAndStartNexStepRunner(Throwable t) {
        PartitionSpecificRunnable runnable = new PartitionSpecificRunnable() {
            @Override
            public void run() {
                handleOperationError(operation, t);
                nextRunner();
            }

            @Override
            public int getPartitionId() {
                return partitionId;
            }
        };

        if (!runDirect(runnable)) {
            offloadRun(runnable, runnable);
        }
    }

    private void startNextStepRunner() {
        PartitionSpecificRunnable runnable = new PartitionSpecificRunnable() {
            @Override
            public void run() {
                assertRunningOnPartitionThread();

                nextRunner();
            }

            @Override
            public int getPartitionId() {
                return partitionId;
            }
        };

        if (!runDirect(runnable)) {
            offloadRun(runnable, runnable);
        }
    }

    private void nextRunner() {
        assert ThreadUtil.isRunningOnPartitionThread()
                : Thread.currentThread().getName();
        assert offloadQueue.peek() == this;

        offloadQueue.poll();
        StepRunner runner = offloadQueue.peek();
        if (runner != null) {
            runner.run();
        }
    }

    private boolean runDirect(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            if (isRunningOnPartitionThread()) {
                try {
                    step.run();
                } catch (NativeOutOfMemoryError e) {
                    rerunWithForcedEviction(step);
                }
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

    private void rerunWithForcedEviction(Runnable step) {
        try {
            runStepWithForcedEvictionStrategies(state.getOperation(), step);
        } catch (NativeOutOfMemoryError e) {
            state.getOperation().disposeDeferredBlocks();
            throw e;
        }
    }

    // TODO handle this better
    private void offloadRun(Runnable step,
                            PartitionSpecificRunnable offload) {
        if (step instanceof PartitionSpecificRunnable) {
            operationExecutor.execute(offload);
        } else {
            executor.execute(offload);
        }
    }

    private void handleOperationError(Operation operation, Throwable e) {
        assertRunningOnPartitionThread();

        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        try {
            operation.onExecutionFailure(e);
        } catch (Throwable t) {
            logger.warning("While calling 'operation.onFailure(e)'... op: "
                    + operation + ", error: " + e, t);
        }

        operation.logError(e);

        // A response is sent regardless of the
        // Operation.returnsResponse method because some operations do
        // want to send back a response, but they didn't want to send it
        // yet but they ran into some kind of error. If on the receiving
        // side no invocation is waiting, the response is ignored.
        sendResponseAfterOperationError(operation, e);
    }

    private void sendResponseAfterOperationError(Operation operation, Throwable e) {
        try {
            if (node.getState() != NodeState.SHUT_DOWN) {
                operation.sendResponse(e);
            } else if (operation.executedLocally()) {
                operation.sendResponse(new HazelcastInstanceNotActiveException());
            }
        } catch (Throwable t) {
            logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
        }
    }
}
