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
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import org.jetbrains.annotations.NotNull;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

public class CascadingRunMachine extends Offload {

    private final int threadId;
    private final Queue<Runnable> steps = new ConcurrentLinkedQueue<>();

    /**
     * Creates a new Offload instance.
     *
     * @param offloadedOperation the Operation that is offloaded
     */
    public CascadingRunMachine(Operation offloadedOperation) {
        super(offloadedOperation);
        ThreadUtil.assertRunningOnPartitionThread();
        this.threadId = ((PartitionOperationThread) Thread.currentThread()).getThreadId();
    }

    public void addStep(Runnable runnable) {
        steps.offer(runnable);
    }

    // TODO: What happens in the event of exception?
    @Override
    public void start() throws Exception {
        Runnable nextStep = steps.poll();
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
                .getExecutor("Offload::" + threadId);
    }

    @NotNull
    private CompletableFuture executeAsync(Runnable step) {
        if (step instanceof PartitionSpecificRunnable) {
            CompletableFuture completableFuture = new CompletableFuture<>();
            OperationExecutor operationExecutor = ((OperationServiceImpl) nodeEngine.getOperationService())
                    .getOperationExecutor();
            operationExecutor.execute(wrapRunnable(step, completableFuture));
            return completableFuture;
        }
        return CompletableFuture.runAsync(step, getExecutor());
    }

    @NotNull
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
