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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

// TODO: only one execution at a time, subsequent executions should wait previous ones finish.
public class StepRunner implements Runnable {

    private final Node node;
    private final ILogger logger;
    private final Executor executor;
    private final Supplier<Runnable> stepSupplier;
    private final OperationExecutor operationExecutor;
    private final Operation operation;

    public StepRunner(NodeEngine nodeEngine, Operation operation,
                      Supplier<Runnable> stepSupplier,
                      String mapName) {
        this.operation = operation;

        ThreadUtil.assertRunningOnPartitionThread();

        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        this.logger = nodeEngine.getLogger(getClass());
        this.operationExecutor = ((OperationServiceImpl) nodeEngine
                .getOperationService()).getOperationExecutor();
        this.stepSupplier = stepSupplier;
        this.executor = nodeEngine.getExecutionService()
                .getExecutor(String.format("%s-%d-mapStoreOffloadingExecutor",
                        mapName, Thread.currentThread().getId()));
    }

    // TODO: What happens in the event of exception?
    @Override
    public void run() {
        System.out.println("---> start running: " + getClass().getSimpleName() + "-" + System.identityHashCode(this));
        try {
            do {
                Runnable nextStep = stepSupplier.get();
                if (nextStep == null) {
                    System.out.println("---> end running: " + getClass().getSimpleName() + "-" + System.identityHashCode(this));
                    return;
                }

                executeAsync(nextStep).get();
            } while (true);
        } catch (Throwable t) {
            handleOperationError(operation, t);
        }
    }

    @Nonnull
    private CompletableFuture executeAsync(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            CompletableFuture completableFuture = new CompletableFuture<>();
            PartitionSpecificRunnable runnable = wrapRunnable(step, completableFuture);
            if (Thread.currentThread().getClass() == PartitionOperationThread.class) {
                runnable.run();
            } else {
                operationExecutor.execute(runnable);
            }
            return completableFuture;
        }
        return CompletableFuture.runAsync(step, executor);
    }

    @Nonnull
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
}
