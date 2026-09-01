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

package com.hazelcast.cp.internal.datastructures.snapshot.v2;

import com.hazelcast.config.Config;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SnapshotV2FlagIntegrationTest {

    @Test
    public void testFlagDisabledByDefault() {
        Config config = new Config();
        boolean enabled = config.getProperties().getProperty(ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED.getName()) != null
                && Boolean.parseBoolean(
                        config.getProperties().getProperty(ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED.getName()));

        assertFalse("Default must have snapshot v2 flag disabled for backward compatibility", enabled);
    }

    @Test
    public void testFlagEnabledExplicitly() {
        Config config = new Config();
        config.setProperty(ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED.getName(), "true");

        boolean enabled = Boolean.parseBoolean(
                config.getProperties().getProperty(ClusterProperty.CP_CONSTANT_TIME_SNAPSHOT_ENABLED.getName()));
        assertTrue("Flag should be enabled when set to true", enabled);
    }
}
