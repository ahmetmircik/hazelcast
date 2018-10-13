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

package com.hazelcast.internal.eviction;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_CLEANUP_OPERATION_COUNT;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_CLEANUP_PERCENTAGE;
import static com.hazelcast.cache.impl.eviction.CacheClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheExpirationBouncingMemberTest extends HazelcastTestSupport {

    private String cacheName = "test";
    private int backupCount = 3;
    private int keySpace = 1000;

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule = set(PROP_TASK_PERIOD_SECONDS, "1");

    @Rule
    public final OverridePropertyRule overrideTaskPercentage = set(PROP_CLEANUP_PERCENTAGE, "100");

    @Rule
    public final OverridePropertyRule overrideOpCount = set(PROP_CLEANUP_OPERATION_COUNT, "1000");

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .driverType(BounceTestConfiguration.DriverType.ALWAYS_UP_MEMBER)
            .build();

    protected CacheConfig getCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.setName(cacheName);
        cacheConfig.setBackupCount(backupCount);
        cacheConfig.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new HazelcastExpiryPolicy(1, 1, 1)));
        return cacheConfig;
    }

    @After
    public void tearDown() throws Exception {
        bounceMemberRule.getFactory().shutdownAll();
    }

    @Test(timeout = 1000 * 60 * 10)
    public void backups_should_be_empty_after_expiration() {
        Runnable[] methods = new Runnable[2];
        final HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        final CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(testDriver);
        provider.getCacheManager().createCache(cacheName, getCacheConfig());
        final HazelcastCacheManager cacheManager = (HazelcastServerCacheManager) provider.getCacheManager();

        methods[0] = new Get(provider);
        methods[1] = new Set(provider);

        bounceMemberRule.testRepeatedly(methods, 20);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final AtomicReference msg = new AtomicReference(Collections.emptyList());
                final AtomicInteger sum = new AtomicInteger();
                final CountDownLatch latch = new CountDownLatch(1);
                HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
                steadyMember.getExecutorService("test").submitToAllMembers(new Count(), new MultiExecutionCallback() {
                    @Override
                    public void onResponse(Member member, Object value) {

                    }

                    @Override
                    public void onComplete(Map<Member, Object> results) {
                        try {
                            List all = new ArrayList();
                            int total = 0;
                            for (Map.Entry<Member, Object> entry : results.entrySet()) {
                                Member member = entry.getKey();

                                List list = (List) entry.getValue();
                                for (int i = 0; i < list.size(); i += 5) {
                                    Integer partitionId = (Integer) list.get(i);
                                    Boolean expirable = (Boolean) list.get(i + 1);
                                    Integer size = (Integer) list.get(i + 2);
                                    Boolean local = (Boolean) list.get(i + 3);
                                    total += size;
                                }
                                all.add(list);
                            }
                            sum.set(total);
                            msg.set(all);
                        } finally {
                            latch.countDown();
                        }
                    }
                });
                assertOpenEventually(latch, 20);

                assertEquals(msg.get().toString(), 0, sum.get());
            }
        }, 120);
    }


    private static class Count implements Callable, HazelcastInstanceAware, Serializable {

        HazelcastInstance hazelcastInstance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public Object call() throws Exception {
            ArrayList<Object> objects = new ArrayList<Object>();

            NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hazelcastInstance);
            CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
            InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
            for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                CachePartitionSegment container = service.getSegment(partitionId);
                boolean local = partitionService.getPartition(partitionId).isLocal();
                Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
                while (iterator.hasNext()) {
                    ICacheRecordStore recordStore = iterator.next();
                    boolean expirable = recordStore.isExpirable();

                    if (recordStore.size() > 0) {
                        objects.add(recordStore.getPartitionId());
                        objects.add(expirable);
                        objects.add(recordStore.size());
                        objects.add(local);
                        objects.add(nodeEngineImpl.getClusterService().getLocalMember().getAddress());
                    }
                }
            }

            return objects;
        }
    }

    private class Get implements Runnable {

        private final Cache<Integer, Integer> cache;

        public Get(CachingProvider hz) {
            this.cache = hz.getCacheManager().getCache(cacheName);
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                cache.get(i);
            }
        }
    }

    private class Set implements Runnable {

        private final Cache<Integer, Integer> cache;

        public Set(CachingProvider hz) {
            this.cache = hz.getCacheManager().getCache(cacheName);
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                cache.put(i, i);
            }
        }
    }
}
