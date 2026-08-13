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

package com.hazelcast.cp.internal.raft.impl;

import com.hazelcast.cp.internal.RaftEndpointImpl;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshotRequest;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.persistence.RaftStateStore;
import com.hazelcast.cp.internal.raft.impl.state.RaftState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ChunkedSnapshotInstallerTest {

    private ChunkedSnapshotInstaller installer;
    private RaftNodeImpl raftNode;
    private RaftState raftState;

    @Before
    public void setup() {
        raftNode = mock(RaftNodeImpl.class, RETURNS_MOCKS);
        ILogger logger = mock(ILogger.class);
        RaftStateStore stateStore = mock(RaftStateStore.class);
        raftState = mock(RaftState.class);
        RaftIntegration raftIntegration = mock(RaftIntegration.class);

        when(raftNode.state()).thenReturn(raftState);
        when(raftNode.getLogger(any())).thenReturn(logger);
        when(raftNode.getRaftIntegration()).thenReturn(raftIntegration);
        when(raftState.stateStore()).thenReturn(stateStore);
        when(raftState.commitIndex()).thenReturn(10L);
        when(raftNode.getLocalMember()).thenReturn(new RaftEndpointImpl(UUID.randomUUID()));
        when(raftNode.getLeader()).thenReturn(new RaftEndpointImpl(UUID.randomUUID()));

        installer = new ChunkedSnapshotInstaller(raftNode);
    }

    private static SnapshotChunk newSnapshotChunk(long index, int chunkCount, int chunkNumber, int term) {
        SnapshotChunk snapshotChunk = new SnapshotChunk();
        snapshotChunk.setGroupMembers(singletonList(new RaftEndpointImpl(UUID.randomUUID())));
        return (SnapshotChunk) snapshotChunk
                .setChunkCount(chunkCount)
                .setChunkNumber(chunkNumber)
                .setSnapshotTerm(term)
                .setIndex(index)
                .setOperation(new Object());
    }

    private static SnapshotChunk newMetadataChunk(long index, int chunkCount, int chunkNumber, int term) {
        return (SnapshotChunk) newSnapshotChunk(index, chunkCount, chunkNumber, term).setOperation(null);
    }

    @Test
    public void testRepeatedChunkInstallRequestIsIgnored() {
        SnapshotChunk chunk1 = newMetadataChunk(12L, 10, 0, 1);
        InstallSnapshotRequest req1 = new InstallSnapshotRequest(
                new RaftEndpointImpl(UUID.randomUUID()), 1, chunk1, 0, 123L);
        installer.processChunk(req1);

        SnapshotChunk chunk2 = newMetadataChunk(13L, 10, 0, 1);
        InstallSnapshotRequest req2 = new InstallSnapshotRequest(
                new RaftEndpointImpl(UUID.randomUUID()), 1, chunk2, 0, 123L);
        installer.processChunk(req2);

        assertEquals(0, installer.getReceivedChunks().size());
    }

    @Test
    public void ignoresStaleChunk() {
        when(raftState.commitIndex()).thenReturn(15L);
        SnapshotChunk chunk = newMetadataChunk(12L, 1, 0, 1);

        InstallSnapshotRequest req = new InstallSnapshotRequest(
                raftNode.getLeader(), 1, chunk, 0, 123L);

        installer.processChunk(req);

        assertEquals(0, installer.getReceivedChunks().size());
    }

    @Test
    public void skipsDuplicateChunkNumber() {
        SnapshotChunk chunk = newMetadataChunk(13L, 2, 0, 1);
        InstallSnapshotRequest req = new InstallSnapshotRequest(
                raftNode.getLeader(), 1, chunk, 0, 123L);

        installer.processChunk(req);
        // duplicate
        installer.processChunk(req);

        assertEquals(0, installer.getReceivedChunks().size());
    }

    @Test
    public void installsSnapshotWhenAllChunksReceived() {
        for (int i = 0; i < 10; i++) {
            SnapshotChunk chunk = newSnapshotChunk(13L, 11, i, 1);
            InstallSnapshotRequest req = new InstallSnapshotRequest(
                    raftNode.getLeader(), 1, chunk, 0, 123L);
            installer.processChunk(req);
        }

        assertEquals(10, installer.getReceivedChunks().size());


        // send final chunk
        SnapshotChunk chunk = newSnapshotChunk(13L, 11, 10, 1);
        InstallSnapshotRequest req = new InstallSnapshotRequest(
                raftNode.getLeader(), 1, chunk, 0, 123L);
        installer.processChunk(req);

        // after final chunk installer's state is reset
        assertEquals(0, installer.getReceivedChunks().size());
    }
}
