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
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

// TODO: only one execution at a time, subsequent executions should wait previous ones.
public class CascadingStepsMachine extends Offload {

    private final int threadId;
    private final Supplier<Runnable> stepSupplier;

    public CascadingStepsMachine(Operation offloadedOperation, Supplier<Runnable> stepSupplier) {
        super(offloadedOperation);

        ThreadUtil.assertRunningOnPartitionThread();

        this.stepSupplier = stepSupplier;
        this.threadId = ((PartitionOperationThread) Thread.currentThread()).getThreadId();
    }

    // TODO: What happens in the event of exception?
    @Override
    public void start() throws Exception {
        Runnable nextStep = stepSupplier.get();
        if (nextStep == null) {
            return;
        }

        executeAsync(nextStep).thenRunAsync(() -> {
            try {
                start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, getExecutor());
    }

    private Executor getExecutor() {
        return nodeEngine.getExecutionService()
                .getExecutor(String.format("MapStoreOffloadingExecutor::%d", threadId));
    }

    @Nonnull
    private CompletableFuture executeAsync(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            CompletableFuture completableFuture = new CompletableFuture<>();
            PartitionSpecificRunnable runnable = wrapRunnable(step, completableFuture);
            if (Thread.currentThread().getClass() == PartitionOperationThread.class) {
                runnable.run();
            } else {
                ((OperationServiceImpl) nodeEngine.getOperationService())
                        .getOperationExecutor().execute(runnable);
            }
            return completableFuture;
        }
        return CompletableFuture.runAsync(step, getExecutor());
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
