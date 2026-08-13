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

import java.io.IOException;
import java.util.concurrent.Executor;

/**
 * Factory for the per-group {@link SnapshotPipeline}. The platform's
 * {@link RaftIntegration} implementation provides one (e.g.
 * {@code NodeEngineRaftIntegration}) when the snapshot redesign is enabled;
 * the Raft node builds the pipeline itself so it can wire its own
 * completion/failure callbacks, which are routed back to the Raft thread via
 * {@code raftIntegration.submit(...)}.
 */
@FunctionalInterface
public interface SnapshotPipelineFactory {

    /**
     * Creates the pipeline for one CP group.
     *
     * @param worker             the node-wide bounded worker executor (snapshot
     *                           work runs here, off the Raft thread)
     * @param completionNotifier invoked after a drain durably advanced S
     * @param failureNotifier    invoked when a drain failed (deltas are
     *                           re-queued and retried)
     */
    SnapshotPipeline newPipeline(Executor worker, Runnable completionNotifier,
                                 Runnable failureNotifier) throws IOException;
}
