/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.ExpiryPolicy;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class CachePutAllTest extends CacheTestSupport {

    private static final int INSTANCE_COUNT = 2;

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
    private HazelcastInstance[] hazelcastInstances;
    private HazelcastInstance hazelcastInstance;

    @Override
    protected void onSetup() {
    }

    @Override
    protected void onTearDown() {
        factory.terminateAll();
        hazelcastInstances = null;
        hazelcastInstance = null;
    }

    @Override
    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig cacheConfig = super.createCacheConfig();
        cacheConfig.setBackupCount(INSTANCE_COUNT - 1);
        return cacheConfig;
    }

    @Override
    protected HazelcastInstance getHazelcastInstance() {
        Config config = createConfig();
        hazelcastInstances = new HazelcastInstance[INSTANCE_COUNT];
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            hazelcastInstances[i] = factory.newHazelcastInstance(config);
        }
        warmUpPartitions(hazelcastInstances);
        waitAllForSafeState(hazelcastInstances);
        hazelcastInstance = hazelcastInstances[0];
        return hazelcastInstance;
    }

    private Map<String, String> createAndFillEntries() {
        final int ENTRY_COUNT_PER_PARTITION = 3;
        Node node = getNode(hazelcastInstance);
        int partitionCount = node.getPartitionService().getPartitionCount();
        Map<String, String> entries = new HashMap<String, String>(partitionCount * ENTRY_COUNT_PER_PARTITION);

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int i = 0; i < ENTRY_COUNT_PER_PARTITION; i++) {
                String key = generateKeyForPartition(hazelcastInstance, partitionId);
                String value = generateRandomString(16);
                entries.put(key, value);
            }
        }

        return entries;
    }

    @Test
    public void testPutAll() {
        ICache<String, String> cache = createCache();
        String cacheName = cache.getName();
        Map<String, String> entries = createAndFillEntries();

        cache.putAll(entries);

        // Verify that put-all works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            String actualValue = cache.get(key);
            assertEquals(expectedValue, actualValue);
        }

        Node node = getNode(hazelcastInstance);
        InternalPartitionService partitionService = node.getPartitionService();
        SerializationService serializationService = node.getSerializationService();

        // Verify that backup of put-all works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entries.get(key);
            Data keyData = serializationService.toData(key);
            int keyPartitionId = partitionService.getPartitionId(keyData);
            for (int i = 0; i < INSTANCE_COUNT; i++) {
                Node n = getNode(hazelcastInstances[i]);
                ICacheService cacheService = n.getNodeEngine().getService(ICacheService.SERVICE_NAME);
                ICacheRecordStore recordStore = cacheService.getRecordStore("/hz/" + cacheName, keyPartitionId);
                assertNotNull(recordStore);
                String actualValue = serializationService.toObject(recordStore.get(keyData, null));
                assertEquals(expectedValue, actualValue);
            }
        }
    }

    @Test
    public void testPutAllWithExpiration() {
        final long EXPIRATION_TIME_IN_MILLISECONDS = 5000;

        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(EXPIRATION_TIME_IN_MILLISECONDS, 0, 0);
        ICache<String, String> cache = createCache();
        Map<String, String> entries = createAndFillEntries();

        cache.putAll(entries, expiryPolicy);

        sleepAtLeastMillis(EXPIRATION_TIME_IN_MILLISECONDS + 1000);

        // Verify that expiration of put-all works
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            String key = entry.getKey();
            String actualValue = cache.get(key);
            assertNull(actualValue);
        }
    }

}
