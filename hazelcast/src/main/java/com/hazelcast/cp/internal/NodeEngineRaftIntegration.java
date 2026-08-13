/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.operation.integration.AppendFailureResponseOp;
import com.hazelcast.cp.internal.operation.integration.AppendRequestOp;
import com.hazelcast.cp.internal.operation.integration.AppendSuccessResponseOp;
import com.hazelcast.cp.internal.operation.integration.AsyncRaftOp;
import com.hazelcast.cp.internal.operation.integration.InstallSnapshotRequestOp;
import com.hazelcast.cp.internal.operation.integration.InstallSnapshotResponseOp;
import com.hazelcast.cp.internal.operation.integration.PreVoteRequestOp;
import com.hazelcast.cp.internal.operation.integration.PreVoteResponseOp;
import com.hazelcast.cp.internal.operation.integration.TriggerLeaderElectionOp;
import com.hazelcast.cp.internal.operation.integration.VoteRequestOp;
import com.hazelcast.cp.internal.operation.integration.VoteResponseOp;
import com.hazelcast.cp.internal.raft.ChunkedSnapshotAwareService;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotResponse;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.TriggerLeaderElection;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raftop.NotifyTermChangeOp;
import com.hazelcast.cp.internal.datastructures.snapshot.SnapshotPipelineConfig;
import com.hazelcast.cp.internal.raft.impl.SnapshotPipeline;
import com.hazelcast.cp.internal.raft.impl.SnapshotPipelineFactory;
import com.hazelcast.cp.internal.raft.impl.persistence.FileIncrementalSnapshotStore;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.cp.internal.util.PartitionSpecificRunnableAdaptor;
import com.hazelcast.internal.util.FrequentLogSuppressor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cp.internal.RaftService.CP_SUBSYSTEM_EXECUTOR;
import static com.hazelcast.cp.internal.datastructures.snapshot.ChunkUtil.extractChunksFrom;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.STEPPED_DOWN;
import static com.hazelcast.cp.internal.raft.impl.RaftNodeStatus.TERMINATED;

/**
 * The integration point of the Raft algorithm implementation and
 * Hazelcast system. Replicates Raft RPCs via Hazelcast operations and executes
 * committed Raft operations.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:ClassDataAbstractionCoupling"})
class NodeEngineRaftIntegration implements RaftIntegration {

    /**
     * !!! ONLY FOR INTERNAL USAGE AND TESTING !!!
     * Enables / disables the linearizable read optimization described in the Raft Dissertation Section 6.4.
     */
    public static final HazelcastProperty RAFT_LINEARIZABLE_READ_OPTIMIZATION_ENABLED
            = new HazelcastProperty("raft.linearizable.read.optimization.enabled", true);

    /**
     * How often operation failures should be logged, in seconds
     */
    public static final HazelcastProperty RAFT_OP_SUPPRESSED_FREQUENT_LOG_PERIOD
            = new HazelcastProperty("hazelcast.internal.raft.op.suppressed.frequent.log.period", 60);

    /**
     * How many times an operation failure is logged before it is suppressed
     */
    public static final HazelcastProperty RAFT_OP_SUPPRESSED_FREQUENT_LOG_THRESHOLD
            = new HazelcastProperty("hazelcast.internal.raft.op.suppress.frequent.log.threshold", 10);

    protected final NodeEngineImpl nodeEngine;
    protected final RaftInvocationManager invocationManager;

    private final CPGroupId groupId;
    private final RaftEndpoint localCPMember;
    private final Address localAddress;
    private final OperationServiceImpl operationService;
    private final TaskScheduler taskScheduler;
    private final int partitionId;
    private final int threadId;
    private final boolean linearizableReadOptimizationEnabled;

    private final FrequentLogSuppressor frequentLogSuppressor;

    NodeEngineRaftIntegration(NodeEngineImpl nodeEngine, CPGroupId groupId, RaftEndpoint localCPMember, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.groupId = groupId;
        this.localCPMember = localCPMember;
        this.localAddress = nodeEngine.getThisAddress();
        this.operationService = nodeEngine.getOperationService();
        this.invocationManager = ((RaftService) nodeEngine.getService(RaftService.SERVICE_NAME)).getInvocationManager();
        this.partitionId = partitionId;
        OperationExecutor operationExecutor = operationService.getOperationExecutor();
        this.threadId = operationExecutor.toPartitionThreadIndex(partitionId);
        this.taskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.linearizableReadOptimizationEnabled = nodeEngine.getProperties()
                .getBoolean(RAFT_LINEARIZABLE_READ_OPTIMIZATION_ENABLED);

        this.frequentLogSuppressor = new FrequentLogSuppressor(
                nodeEngine.getProperties().getInteger(RAFT_OP_SUPPRESSED_FREQUENT_LOG_PERIOD),
                nodeEngine.getProperties().getInteger(RAFT_OP_SUPPRESSED_FREQUENT_LOG_THRESHOLD),
                nodeEngine.getLogger(FrequentLogSuppressor.class)
        );
    }

    @Override
    public void execute(Runnable task) {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof PartitionOperationThread thread
                && thread.getThreadId() == threadId) {
            task.run();
        } else {
            operationService.execute(new PartitionSpecificRunnableAdaptor(task, partitionId));
        }
    }

    @Override
    public void submit(Runnable task) {
        operationService.execute(new PartitionSpecificRunnableAdaptor(task, partitionId));
    }

    @Override
    public void schedule(final Runnable task, long delay, TimeUnit timeUnit) {
        taskScheduler.schedule(() -> execute(task), delay, timeUnit);
    }

    @Override
    public InternalCompletableFuture newCompletableFuture() {
        return new InternalCompletableFuture();
    }

    @Override
    public Object getAppendedEntryOnLeaderElection() {
        return new NotifyTermChangeOp();
    }

    @Override
    public boolean isLinearizableReadOptimizationEnabled() {
        return linearizableReadOptimizationEnabled;
    }

    @Override
    public ILogger getLogger(String name) {
        return nodeEngine.getLogger(name);
    }

    @Override
    public boolean isReady() {
        return nodeEngine.getClusterService().isJoined();
    }

    @Override
    public boolean isReachable(RaftEndpoint target) {
        if (!isStartCompleted()) {
            return true;
        }

        CPMember targetMember = getCPMember(target);
        return targetMember != null && nodeEngine.getClusterService().getMember(targetMember.getAddress()) != null;
    }

    private boolean isStartCompleted() {
        return nodeEngine.getNode().getNodeExtension().isStartCompleted();
    }

    @Override
    public boolean send(PreVoteRequest request, RaftEndpoint target) {
        return send(new PreVoteRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(PreVoteResponse response, RaftEndpoint target) {
        return send(new PreVoteResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(VoteRequest request, RaftEndpoint target) {
        return send(new VoteRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(VoteResponse response, RaftEndpoint target) {
        return send(new VoteResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(AppendRequest request, RaftEndpoint target) {
        return send(new AppendRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(AppendSuccessResponse response, RaftEndpoint target) {
        return send(new AppendSuccessResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(AppendFailureResponse response, RaftEndpoint target) {
        return send(new AppendFailureResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(InstallSnapshotRequest request, RaftEndpoint target) {
        return send(new InstallSnapshotRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(InstallSnapshotResponse response, RaftEndpoint target) {
        return send(new InstallSnapshotResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(TriggerLeaderElection request, RaftEndpoint target) {
        return send(new TriggerLeaderElectionOp(groupId, request), target);
    }

    @Override
    public Object runOperation(Object op, long commitIndex) {
        RaftOp operation = (RaftOp) op;
        operation.setNodeEngine(nodeEngine);
        try {
            return operation.run(groupId, commitIndex);
        } catch (Throwable t) {
            frequentLogSuppressor.runSuppressed(t, () -> operation.logFailure(t));
            return t;
        }
    }

    @Nonnull
    @Override
    public Object takeServiceSnapshots(long commitIndex) {
        var snapshotPerServiceInfo = new HashMap<>();

        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(SnapshotAwareService.class)) {
            SnapshotAwareService<?> service = serviceInfo.getService();
            Object snapshot = service instanceof ChunkedSnapshotAwareService<?, ?> chunkedService
                    ? chunkedService.takeSnapshotChunks(groupId, commitIndex)
                    : service.takeSnapshot(groupId, commitIndex);
            if (snapshot == null) {
                continue;
            }

            snapshotPerServiceInfo.put(serviceInfo, snapshot);
        }

        return snapshotPerServiceInfo;
    }

    @Override
    public void resetForChunkedSnapshotRestore() {
        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(ChunkedSnapshotAwareService.class)) {
            ChunkedSnapshotAwareService<?, ?> service = serviceInfo.getService();
            service.prepareForSnapshotRestore(groupId);
        }
    }

    @Override
    public void restoreSnapshot(Object snapshotEntry, long commitIndex) {
        ILogger logger = nodeEngine.getLogger(getClass());
        List<SnapshotChunk> snapshotChunks = extractChunksFrom((SnapshotEntry) snapshotEntry);
        for (SnapshotChunk snapshotChunk : snapshotChunks) {
            List<Object> ops = (List<Object>) snapshotChunk.operation();
            for (Object snapshotOp : ops) {
                Object result = runOperation(snapshotOp, snapshotChunk.index());
                if (result instanceof Throwable throwable) {
                    logger.severe("Restore of " + snapshotOp + " failed...", throwable);
                }
            }
        }
    }

    private boolean send(AsyncRaftOp operation, RaftEndpoint target) {
        CPMember targetMember = getCPMember(target);
        if (targetMember == null || localAddress.equals(targetMember.getAddress())) {
            if (localCPMember.getUuid().equals(target.getUuid())) {
                throw new IllegalStateException("Cannot send " + operation + " to "
                        + target + " because it's same with the local CP member!");
            }

            return false;
        }

        operation.setTargetEndpoint(target).setPartitionId(partitionId);
        return operationService.send(operation, targetMember.getAddress());
    }

    @Override
    public CPMember getCPMember(RaftEndpoint target) {
        return invocationManager.getCPMember(target);
    }

    @Override
    public void onNodeStatusChange(RaftNodeStatus status) {
        if (status == TERMINATED) {
            Collection<RaftNodeLifecycleAwareService> services = nodeEngine.getServices(RaftNodeLifecycleAwareService.class);
            for (RaftNodeLifecycleAwareService service : services) {
                service.onRaftNodeTerminated(groupId);
            }
        } else if (status == STEPPED_DOWN) {
            Collection<RaftNodeLifecycleAwareService> services = nodeEngine.getServices(RaftNodeLifecycleAwareService.class);
            for (RaftNodeLifecycleAwareService service : services) {
                service.onRaftNodeSteppedDown(groupId);
            }
        }
    }

    @Override
    public void onGroupDestroyed(CPGroupId groupId) {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        nodeEngine.getExecutionService().execute(CP_SUBSYSTEM_EXECUTOR, () -> raftService.terminateRaftNode(groupId, true));

        Collection<RaftNodeLifecycleAwareService> services = nodeEngine.getServices(RaftNodeLifecycleAwareService.class);
        for (RaftNodeLifecycleAwareService service : services) {
            if (service == raftService) {
                continue;
            }
            service.onRaftNodeTerminated(groupId);
        }
    }

    @Override
    public void onNewLeaderElected(CPGroupId groupId) {
        // noop
    }

    @Override
    public Object newRestoreSnapshotOp(String serviceName, long commitIndex, Object chunk) {
        return new RestoreSnapshotOp(serviceName, chunk);
    }

    @Override
    public SnapshotPipelineFactory getSnapshotPipelineFactory(CPGroupId groupId) {
        if (!SnapshotPipelineConfig.isSnapshotEnabled()) {
            return null;
        }
        // per-group pipeline backed by the file incremental store under the
        // configured snapshot directory; the worker, completion and failure
        // callbacks are wired by the Raft node
        return (worker, completionNotifier, failureNotifier) -> new SnapshotPipeline(
                new FileIncrementalSnapshotStore(snapshotDir(groupId)),
                worker, completionNotifier, failureNotifier);
    }

    @Override
    public Executor getSnapshotExecutor() {
        if (!SnapshotPipelineConfig.isSnapshotEnabled()) {
            return null;
        }
        // the node-wide bounded global task scheduler is the snapshot worker
        // domain; snapshot drains run here, off the partition threads
        return nodeEngine.getExecutionService().getGlobalTaskScheduler();
    }

    private Path snapshotDir(CPGroupId groupId) {
        return Path.of(SnapshotPipelineConfig.SNAPSHOT_DIR)
                .resolve(localCPMember.getUuid().toString()) // per-member scope
                .resolve("delta")
                .resolve(SnapshotPipelineConfig.groupDirName(groupId));
    }

    @Override
    public Object captureSnapshot(long commitIndex) {
        if (!SnapshotPipelineConfig.isSnapshotEnabled()) {
            return null;
        }
        CaptureHandle handle = new CaptureHandle();
        for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(SnapshotAwareService.class)) {
            SnapshotAwareService<?> service = serviceInfo.getService();
            if (service instanceof ChunkedSnapshotAwareService<?, ?> chunkedService) {
                // O(1) freeze: mutations redirect to a transient overlay, the
                // real state stays frozen for the off-thread scan
                Object capture = chunkedService.captureSnapshot(groupId, commitIndex);
                if (capture == null) {
                    return null; // unsupported service -> fall back to the sync path
                }
                handle.chunkedCaptures.put(serviceInfo, capture);
            } else {
                // the small services are captured synchronously on the Raft
                // thread (bounded, not data-size-dependent)
                Object snapshot = service.takeSnapshot(groupId, commitIndex);
                if (snapshot != null) {
                    handle.smallSnapshots.put(serviceInfo, snapshot);
                }
            }
        }
        return handle;
    }

    @Override
    public Object takeCapturedServiceSnapshots(Object capture, long commitIndex) {
        CaptureHandle handle = (CaptureHandle) capture;
        Map<Object, Object> snapshotPerServiceInfo = new HashMap<>(handle.smallSnapshots);
        for (Map.Entry<ServiceInfo, Object> entry : handle.chunkedCaptures.entrySet()) {
            // the chunked scan runs on the worker over the FROZEN state
            ChunkedSnapshotAwareService<?, ?> chunkedService =
                    (ChunkedSnapshotAwareService<?, ?>) entry.getKey().getService();
            snapshotPerServiceInfo.put(entry.getKey(),
                    chunkedService.takeSnapshotChunks(groupId, commitIndex));
        }
        return snapshotPerServiceInfo;
    }

    @Override
    public void finishSnapshotCapture(Object capture) {
        CaptureHandle handle = (CaptureHandle) capture;
        for (Map.Entry<ServiceInfo, Object> entry : handle.chunkedCaptures.entrySet()) {
            // on the Raft thread: apply the overlay back to the real state
            ((ChunkedSnapshotAwareService<?, ?>) entry.getKey().getService())
                    .finishSnapshotCapture(groupId);
        }
    }

    private static final class CaptureHandle {
        final Map<ServiceInfo, Object> chunkedCaptures = new HashMap<>();
        final Map<ServiceInfo, Object> smallSnapshots = new HashMap<>();
    }
}
