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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

// TODO: only one execution at a time, subsequent executions should wait previous ones finish.
public class CascadingStepsMachine implements Runnable {

    private final Supplier<Runnable> stepSupplier;
    private final Executor executor;
    private final OperationExecutor operationExecutor;

    public CascadingStepsMachine(NodeEngine nodeEngine,
                                 Supplier<Runnable> stepSupplier,
                                 String mapName) {

        ThreadUtil.assertRunningOnPartitionThread();

        this.stepSupplier = stepSupplier;
        this.executor = nodeEngine.getExecutionService()
                .getExecutor(String.format("%s-%d-mapStoreOffloadingExecutor",
                        mapName, Thread.currentThread().getId()));
        this.operationExecutor = ((OperationServiceImpl) nodeEngine.getOperationService())
                .getOperationExecutor();
    }

    // TODO: What happens in the event of exception?
    @Override
    public void run() {
        System.out.println("---> start running: " + getClass().getSimpleName() + "-" + System.identityHashCode(this));
        do {
            Runnable nextStep = stepSupplier.get();
            if (nextStep == null) {
                System.out.println("---> end running: " + getClass().getSimpleName() + "-" + System.identityHashCode(this));
                return;
            }

            executeAsync(nextStep).join();
        } while (true);

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
                } finally {
                    completableFuture.complete(null);
                }
            }
        };
    }
}
