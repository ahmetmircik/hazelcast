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

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
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

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CacheExpirationBouncingMemberTest extends HazelcastTestSupport {

    private static final long FIVE_MINUTES = 5 * 60 * 1000;

    private int backupCount = 3;
    private int keySpace = 1000;
    private String cacheName = "test";

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

    @Test(timeout = FIVE_MINUTES)
    public void backups_should_be_empty_after_expiration() {
        Cache cache = createCache();

        Runnable[] methods = new Runnable[2];
        methods[0] = new Get(cache);
        methods[1] = new Set(cache);

        bounceMemberRule.testRepeatedly(methods, 20);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RemainingCacheSize remainingCacheSize = findRemainingCacheSize();
                assertEquals(remainingCacheSize.getInfoUnexpired(), 0, remainingCacheSize.getTotalUnexpired());
            }

            private RemainingCacheSize findRemainingCacheSize() {
                CountDownLatch latch = new CountDownLatch(1);
                HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
                IExecutorService executorService = steadyMember.getExecutorService("test");
                RemainingCacheSize remainingCacheSize = new RemainingCacheSize(latch);
                executorService.submitToAllMembers(new UnexpiredRecordStoreCollector(), remainingCacheSize);
                assertOpenEventually(latch);
                return remainingCacheSize;
            }
        }, 240);
    }

    private Cache createCache() {
        HazelcastInstance testDriver = bounceMemberRule.getNextTestDriver();
        CachingProvider provider = HazelcastServerCachingProvider.createCachingProvider(testDriver);
        return provider.getCacheManager().createCache(cacheName, getCacheConfig());
    }

    private static final class RemainingCacheSize implements MultiExecutionCallback {
        private final CountDownLatch latch;
        private final AtomicInteger totalUnexpired = new AtomicInteger();
        private final AtomicReference<List> infoUnexpired = new AtomicReference(Collections.emptyList());

        public RemainingCacheSize(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onResponse(Member member, Object value) {
            //Intentionally empty method body.
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            try {
                List infoList = new ArrayList();
                int sumUnexpired = 0;
                for (Map.Entry<Member, Object> entry : values.entrySet()) {
                    List info = (List) entry.getValue();
                    String msg = null;
                    for (int i = 0; i < info.size(); i += 5) {
                        sumUnexpired += (Integer) info.get(i);
                        String format = "partition[remaining: %d, id: %d, expirable: %b, primary: %b, address: %s]%n";
                        msg = String.format(format, info.get(i), info.get(i + 1), info.get(i + 2), info.get(i + 3), info.get(i + 4));
                    }
                    infoList.add(msg);
                }
                totalUnexpired.set(sumUnexpired);
                infoUnexpired.set(infoList);
            } finally {
                latch.countDown();
            }
        }

        public int getTotalUnexpired() {
            return totalUnexpired.get();
        }

        public String getInfoUnexpired() {
            return infoUnexpired.get().toString();
        }
    }

    private static class UnexpiredRecordStoreCollector
            implements Callable, HazelcastInstanceAware, Serializable {

        HazelcastInstance hazelcastInstance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public Object call() throws Exception {
            List<Object> objects = new ArrayList<Object>();

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
                        objects.add(recordStore.size());
                        objects.add(recordStore.getPartitionId());
                        objects.add(expirable);
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

        public Get(Cache cache) {
            this.cache = cache;
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

        public Set(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void run() {
            for (int i = 0; i < keySpace; i++) {
                cache.put(i, i);
            }
        }
    }
}
