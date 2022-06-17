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
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.function.Supplier;

import static com.hazelcast.internal.util.ThreadUtil.assertRunningOnPartitionThread;
import static com.hazelcast.map.impl.operation.ForcedEviction.runStepWithForcedEvictionStrategies;

public class StepSupplier implements Supplier<Runnable> {

    private final State state;
    private final ILogger logger;
    private final Node node;

    private volatile Step currentStep;

    public StepSupplier(Step startingStep, State state) {
        this.state = state;
        this.currentStep = startingStep;
        NodeEngine nodeEngine = state.getOperation().getNodeEngine();
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public Runnable get() {
        if (currentStep == null) {
            return null;
        }

        return createStep(state);
    }

    private Runnable createStep(State state) {
        if (currentStep.isOffloadStep()) {
            return () -> runStepWith(state);
        }

        return new PartitionSpecificRunnable() {
            @Override
            public void run() {
                runStepWith(state);
            }

            @Override
            public int getPartitionId() {
                return state.getPartitionId();
            }
        };
    }

    private void runStepWith(State state) {
        do {
            try {
                currentStep.runStep(state);
            } catch (NativeOutOfMemoryError e) {
                rerunWithForcedEviction(() -> currentStep.runStep(state));
            }
            currentStep = currentStep.nextStep(state);
            if (currentStep == null
                    || (currentStep.isOffloadStep() && ThreadUtil.isRunningOnPartitionThread())
                    || (!currentStep.isOffloadStep() && !ThreadUtil.isRunningOnPartitionThread())) {
                break;
            }
        } while (true);
    }

    private void rerunWithForcedEviction(Runnable step) {
        try {
            runStepWithForcedEvictionStrategies(state.getOperation(), step);
        } catch (NativeOutOfMemoryError e) {
            state.getOperation().disposeDeferredBlocks();
            throw e;
        }
    }


    public void handleOperationError(Throwable e) {
        assertRunningOnPartitionThread();

        MapOperation operation = state.getOperation();

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
