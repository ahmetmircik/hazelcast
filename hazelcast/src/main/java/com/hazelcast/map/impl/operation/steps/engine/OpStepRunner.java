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
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;
import static com.hazelcast.map.impl.operation.steps.engine.OpStepSupplier.newOpStepSupplier;
import static java.lang.String.format;

// TODO: only one execution at a time, subsequent executions should wait previous ones finish.
public class OpStepRunner implements PartitionSpecificRunnable {

    // TODO is this too much? per-partition-per-map
    // TODO destroy this on map destroy
    private static final String OFFLOAD_EXECUTOR_NAME_FORMAT = "%d-%s-mapStoreOffloadingExecutor";

    private final Node node;
    private final ILogger logger;
    private final Executor executor;
    private final Supplier<Runnable> opStepSupplier;
    private final OperationExecutor operationExecutor;
    private final Operation operation;
    private final Queue<OpStepRunner> offloadQueue;
    private int partitionId;

    public OpStepRunner(MapOperation operation,
                        State state,
                        OpStep startingOpStep,
                        String mapName) {
        this.operation = operation;

        ThreadUtil.assertRunningOnPartitionThread();

        NodeEngine nodeEngine = operation.getNodeEngine();
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        this.logger = nodeEngine.getLogger(getClass());
        this.operationExecutor = ((OperationServiceImpl) nodeEngine
                .getOperationService()).getOperationExecutor();
        this.opStepSupplier = newOpStepSupplier(startingOpStep, state);
        this.executor = nodeEngine.getExecutionService()
                .getExecutor(format(OFFLOAD_EXECUTOR_NAME_FORMAT,
                        Thread.currentThread().getId(), mapName));
        this.partitionId = state.getPartitionId();
        this.offloadQueue = operation.getMapContainer().getMapServiceContext()
                .getPartitionContainer(state.getPartitionId())
                .getOffloadQueue(mapName);
    }

    // TODO: What happens in the event of exception?
    @Override
    public void run() {
        try {
            Runnable step;
            do {
                step = opStepSupplier.get();
                if (step == null) {
                    nextRunner();

                    return;
                }
            } while (doRun(step));
        } catch (Throwable t) {
            handleErrorStep(new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    handleOperationError(operation, t);
                    nextRunner();
                }

                @Override
                public int getPartitionId() {
                    return partitionId;
                }
            });
        }
    }

    private void nextRunner() {
        assert ThreadUtil.isRunningOnPartitionThread()
                : Thread.currentThread().getName();
        assert offloadQueue.peek() == this;

        offloadQueue.poll();
        OpStepRunner runner = offloadQueue.peek();
        if (runner != null) {
            runner.run();
        }
    }

    private boolean doRun(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            if (isRunningOnPartitionThread()) {
                step.run();
                return true;
            } else {
                operationExecutor.execute(this);
                return false;
            }
        } else {
            if (isRunningOnPartitionThread()) {
                executor.execute(this);
                return false;
            } else {
                step.run();
                return true;
            }
        }
    }

    // TODO refactor this step and unify it with `doRun` method.
    private boolean handleErrorStep(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            if (isRunningOnPartitionThread()) {
                step.run();
                return true;
            } else {
                operationExecutor.execute(((PartitionSpecificRunnable) step));
                return false;
            }
        } else {
            if (isRunningOnPartitionThread()) {
                executor.execute(step);
                return false;
            } else {
                step.run();
                return true;
            }
        }
    }

    private CompletableFuture executeAsync(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            CompletableFuture completableFuture = new CompletableFuture<>();
            PartitionSpecificRunnable runnable = wrapRunnable(step, completableFuture);
            if (isRunningOnPartitionThread()) {
                runnable.run();
            } else {
                operationExecutor.execute(runnable);
            }
            return completableFuture;
        }
        return CompletableFuture.runAsync(step, executor);
    }

    private PartitionSpecificRunnable wrapRunnable(Runnable step,
                                                   CompletableFuture completableFuture) {
        return new PartitionSpecificRunnable() {
            @Override
            public int getPartitionId() {
                return ((PartitionSpecificRunnable) step).getPartitionId();
            }

            @Override
            public void run() {
                try {
                    step.run();
                    completableFuture.complete(null);
                } catch (Throwable t) {
                    completableFuture.completeExceptionally(t);
                    throw ExceptionUtil.rethrow(t);
                }
            }
        };
    }

    // TODO handle this inside partition thread
    private void handleOperationError(Operation operation, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        try {
            operation.onExecutionFailure(e);
        } catch (Throwable t) {
            logger.warning("While calling 'operation.onFailure(e)'... op: " + operation + ", error: " + e, t);
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

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
