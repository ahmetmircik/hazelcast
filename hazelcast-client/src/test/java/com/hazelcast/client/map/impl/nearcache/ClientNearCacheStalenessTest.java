/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.util.RandomPicker.getInt;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientNearCacheStalenessTest extends HazelcastTestSupport {

    private final int ENTRY_COUNT = 10;
    private final int NEAR_CACHE_INVALIDATOR_THREAD_COUNT = 1;
    private final int NEAR_CACHE_PUTTER_THREAD_COUNT = 10;
    private final String MAP_NAME = randomMapName();
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    private IMap clientMap;
    private IMap memberMap;

    @Before
    public void setUp() throws Exception {
        Config config = getConfig();
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "false");

        NearCacheConfig nearCacheConfig = new NearCacheConfig(MAP_NAME);
        nearCacheConfig.setInvalidateOnChange(true);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HazelcastInstance member = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        clientMap = client.getMap(MAP_NAME);
        memberMap = member.getMap(MAP_NAME);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void testNearCache_notContainsStaleValue_whenUpdatedByMultipleThreads() throws Exception {
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < NEAR_CACHE_INVALIDATOR_THREAD_COUNT; i++) {
            Thread putter = new NearCacheInvalidator();
            threads.add(putter);
        }

        for (int i = 0; i < NEAR_CACHE_PUTTER_THREAD_COUNT; i++) {
            Thread getter = new NearCachePutter();
            threads.add(getter);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(5);

        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        // give some-time to receive possible latest invalidations.
        sleepSeconds(2);

        assertNoStaleDataExistInNearCache(clientMap);

    }

    protected void assertNoStaleDataExistInNearCache(IMap map) {
        // 1. Get all entries when near-cache is full, so some values will come from near-cache.
        Map fromNearCache = getAllEntries(map);

        // 2. Clear near-cache
        ((NearCachedClientMapProxy) map).getNearCache().clear();

        // 3. Get all values when near-cache is empty, these requests
        // will go to underlying IMap directly because near-cache is empty.
        Map fromIMap = getAllEntries(map);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals(fromIMap.get(i), fromNearCache.get(i));
        }
    }

    protected HashMap getAllEntries(IMap iMap) {
        HashMap localMap = new HashMap(ENTRY_COUNT);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            localMap.put(i, iMap.get(i));
        }
        return localMap;
    }

    private class NearCacheInvalidator extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    memberMap.put(i, getInt(ENTRY_COUNT));
                }
            }
        }
    }


    private class NearCachePutter extends Thread {

        @Override
        public void run() {
            while (!stop.get()) {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    clientMap.get(i);
                }
            }
        }
    }
}
