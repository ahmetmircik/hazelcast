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
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapService;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapCompareAndSetOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutIfAbsentOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapRaftOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapRemoveOp;
import com.hazelcast.cp.internal.datastructures.cpmap.store.CPMapStore;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.codecs.CPMapSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.GroupSnapshotCoordinator;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.engine.PreparedGroupState;
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
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.cp.internal.util.PartitionSpecificRunnableAdaptor;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.FrequentLogSuppressor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private static final int DEFAULT_DELTA_BUFFER_CAPACITY = 1000;
    private static final long DEFAULT_INITIAL_INCARNATION = 1L;

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
    private final boolean cpConstantTimeSnapshotEnabled;
    private final GroupSnapshotCoordinator groupSnapshotCoordinator;
    private final java.util.concurrent.ConcurrentMap<String, Long> mapGenerations
            = new java.util.concurrent.ConcurrentHashMap<>();
    private long lastCommitIndex = -1L;
    private int nextOrdinal;


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

        this.cpConstantTimeSnapshotEnabled = nodeEngine.getProperties()
                .getBoolean(ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED);
        if (this.cpConstantTimeSnapshotEnabled) {
            CPMapSnapshotCodec cpMapCodec = new CPMapSnapshotCodec();
            long incarnation = (groupId instanceof RaftGroupId rId) ? Math.max(DEFAULT_INITIAL_INCARNATION, rId.getId())
                    : DEFAULT_INITIAL_INCARNATION;
            this.groupSnapshotCoordinator = new GroupSnapshotCoordinator(
                    groupId,
                    incarnation,
                    DEFAULT_DELTA_BUFFER_CAPACITY,
                    Map.of(cpMapCodec.serviceId(), cpMapCodec)
            );
        } else {
            this.groupSnapshotCoordinator = null;
        }
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
            Object result = operation.run(groupId, commitIndex);
            if (cpConstantTimeSnapshotEnabled && groupSnapshotCoordinator != null) {
                recordCPMapMutationIfApplicable(operation, result, commitIndex);
            }
            return result;
        } catch (Throwable t) {
            frequentLogSuppressor.runSuppressed(t, () -> operation.logFailure(t));
            return t;
        }
    }

    private int nextOrdinalFor(long commitIndex) {

        if (commitIndex == lastCommitIndex) {
            return nextOrdinal++;
        }
        lastCommitIndex = commitIndex;
        nextOrdinal = 1;
        return 0;
    }

    private void recordCPMapMutationIfApplicable(RaftOp operation, Object result, long commitIndex) {
        if (operation instanceof CPMapRaftOp cpMapOp) {
            String mapName = cpMapOp.getObjectName();
            Data mapNameData = nodeEngine.toData(mapName);
            long generation = mapGenerations.computeIfAbsent(mapName, k -> 1L);
            int ordinal = nextOrdinalFor(commitIndex);
            if (operation instanceof CPMapPutOp putOp) {
                recordPut(mapNameData, generation, putOp.getKey(), putOp.getValue(), commitIndex, ordinal);
            } else if (operation instanceof CPMapPutIfAbsentOp putIfAbsentOp) {
                if (result == null) {
                    recordPut(mapNameData, generation, putIfAbsentOp.getKey(), putIfAbsentOp.getValue(), commitIndex, ordinal);
                }
            } else if (operation instanceof CPMapCompareAndSetOp casOp) {
                if (Boolean.TRUE.equals(result)) {
                    recordCas(mapNameData, generation, casOp.getKey(), casOp.getNewValue(), commitIndex, ordinal);
                }
            } else if (operation instanceof CPMapRemoveOp removeOp) {
                recordRemove(mapNameData, generation, removeOp.getKey(), commitIndex, ordinal);
            }
        }
    }

    private void recordPut(Data mapNameData, long generation, Data key, Data val, long commitIndex, int ordinal) {
        if (key != null && val != null) {
            groupSnapshotCoordinator.activeDeltaBuffer().append(new DeltaEntry(
                    CPMapSnapshotCodec.SERVICE_ID,
                    mapNameData,
                    generation,
                    DeltaEntry.OP_PUT,
                    key,
                    val,
                    commitIndex,
                    ordinal
            ));
        }
    }

    private void recordCas(Data mapNameData, long generation, Data key, Data val, long commitIndex, int ordinal) {
        byte opCode = (val != null) ? DeltaEntry.OP_PUT : DeltaEntry.OP_REMOVE;
        groupSnapshotCoordinator.activeDeltaBuffer().append(new DeltaEntry(
                CPMapSnapshotCodec.SERVICE_ID,
                mapNameData,
                generation,
                opCode,
                key,
                val,
                commitIndex,
                ordinal
        ));
    }

    private void recordRemove(Data mapNameData, long generation, Data key, long commitIndex, int ordinal) {
        if (key != null) {
            groupSnapshotCoordinator.activeDeltaBuffer().append(new DeltaEntry(
                    CPMapSnapshotCodec.SERVICE_ID,
                    mapNameData,
                    generation,
                    DeltaEntry.OP_REMOVE,
                    key,
                    null,
                    commitIndex,
                    ordinal
            ));
        }
    }


    @Override
    public boolean isConstantTimeSnapshotEnabled() {
        return cpConstantTimeSnapshotEnabled;
    }

    public GroupSnapshotCoordinator getGroupSnapshotCoordinator() {
        return groupSnapshotCoordinator;
    }

    @Override
    public Object takeConstantTimeSnapshot(
            long commitIndex, int snapshotTerm, long membershipLogIndex, Collection<RaftEndpoint> members) {
        if (cpConstantTimeSnapshotEnabled && groupSnapshotCoordinator != null) {
            List<RaftEndpoint> memberList = members instanceof List<RaftEndpoint> list
                    ? list : new java.util.ArrayList<>(members);
            return groupSnapshotCoordinator.performPhase1SnapshotCut(
                    commitIndex, snapshotTerm, membershipLogIndex, memberList);
        }
        return takeServiceSnapshots(commitIndex);
    }

    @Nonnull
    @Override
    public Object takeServiceSnapshots(long commitIndex) {
        if (cpConstantTimeSnapshotEnabled && groupSnapshotCoordinator != null) {
            throw new UnsupportedOperationException(
                    "Constant-time snapshotting (v2) requires dynamic snapshotTerm; "
                    + "use takeConstantTimeSnapshot instead of takeServiceSnapshots.");
        }

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
        if (cpConstantTimeSnapshotEnabled && groupSnapshotCoordinator != null) {
            if (snapshotEntry instanceof SnapshotEntry entry && entry.operation() instanceof PreparedGroupState prep) {
                groupSnapshotCoordinator.applyFollowerRestore(prep);
                restoreCPMapStoresFromPreparedState(prep);
                return;
            }
        }

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

    private void restoreCPMapStoresFromPreparedState(PreparedGroupState prep) {
        CPMapService mapService = nodeEngine.getService(CPMapService.SERVICE_NAME);
        if (mapService != null) {
            Object container = prep.liveContainers().get(CPMapSnapshotCodec.SERVICE_ID);
            if (container instanceof Map<?, ?> mapData) {
                for (Map.Entry<?, ?> mapEntry : mapData.entrySet()) {
                    String mapName = (String) mapEntry.getKey();
                    CPMapStore mapStore = mapService.getOrInitMapStore(groupId, mapName);
                    if (mapEntry.getValue() instanceof Map<?, ?> kvPairs) {
                        for (Map.Entry<?, ?> kv : kvPairs.entrySet()) {
                            mapStore.put((Data) kv.getKey(), (Data) kv.getValue(), CPMapStore.NO_TIMESTAMP);
                        }
                    }
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
            if (groupSnapshotCoordinator != null) {
                groupSnapshotCoordinator.shutdown();
            }
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
        if (groupSnapshotCoordinator != null) {
            groupSnapshotCoordinator.shutdown();
        }
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
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        raftService.getGroupViewTracker().publishGroupLeadershipStates();
    }

    @Override
    public Object newRestoreSnapshotOp(String serviceName, long commitIndex, Object chunk) {
        return new RestoreSnapshotOp(serviceName, chunk);
    }
}
