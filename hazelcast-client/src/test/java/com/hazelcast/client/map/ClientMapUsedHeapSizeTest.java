/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapUsedHeapSizeTest extends HazelcastTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();


    @Test(timeout = 60 * 60 * 1000)
    public void test() {
        Config config = createConfig();
        HazelcastInstance server = factory.newHazelcastInstance(config);

        HazelcastInstance client = factory.newHazelcastClient();

        IMap<Integer, byte[]> clientMap = client.getMap("test");
        // 10 MBytes - This is
        // just so that we know the cached values are individually
        // far smaller than the eviction policy limit.
        // Test case fails before hitting this size anyway.
        int maxSizeBytes = 10 * 1024 * 1024;
        for (int i = 0; i < maxSizeBytes; i++) {
            clientMap.set(i, new byte[i]);
            if (i % 1000 == 0) {
                System.out.println("Done " + i + " set operations, map-size is " + clientMap.size());
            }
        }
    }

    Config createConfig() {
        Config config = getConfig();
        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE);
        msc.setSize(200);

        MapConfig mapConfig = config.getMapConfig("test");
        mapConfig.setEvictionPolicy(EvictionPolicy.LRU);
        mapConfig.setMaxSizeConfig(msc);

        return config;
    }

}
