/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static com.hazelcast.test.Accessors.getNode;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class})
public class ShutdownCorrectnessTest extends HazelcastTestSupport {

    @Test
    @Repeat(20)
    public void name() {
        Config config = new Config();
        config.getMetricsConfig().setEnabled(false);
        config.getMapConfig("default").setBackupCount(0);
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        HazelcastInstance three = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance one = instanceFactory.newHazelcastInstance(config);

        HazelcastInstance two = instanceFactory.newHazelcastInstance(config);

        IMap<Object, Object> map = one.getMap("test");
        for (int i = 0; i < 10_000; i++) {
            map.set(i, i);
        }

        System.err.println(two.getCluster().getLocalMember().getUuid());
        Address address = getNode(two).getThisAddress();
        two.shutdown();
        HazelcastInstance instance = instanceFactory.newHazelcastInstance(address);

        System.err.println(instance.getCluster().getLocalMember().getUuid());

//        //suspectMember(one, instance);
//
//        CountDownLatch latch = new CountDownLatch(2);
//        Future first = spawn(new Runnable() {
//            @Override
//            public void run() {
//                Address address = getNode(two).getThisAddress();
//                instanceFactory.newHazelcastInstance(address);
//                two.shutdown();
//            }
//        });
//
//        Future second = spawn(new Runnable() {
//            @Override
//            public void run() {
//                Address address = getNode(three).getThisAddress();
//                three.shutdown();
//                instanceFactory.newHazelcastInstance(address);
//            }
//        });
//
//        FutureUtil.waitForever(Arrays.asList(first, second));
//
//        int size = instanceFactory.getAllHazelcastInstances().iterator().next().getMap("test").size();
//        assertEquals(size, 10_000);
    }

    @Test
    public void name2() {
        ExecutorService es = Executors.newCachedThreadPool();

        CompletableFuture<?> future = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                System.err.println("main");
            }
        }, es);


        CompletableFuture<?> completeAsync = future.whenCompleteAsync(new BiConsumer<Object, Throwable>() {
            @Override
            public void accept(Object o, Throwable throwable) {
                sleepSeconds(3);
                System.err.println("whenCompleteAsync");
            }
        });

        completeAsync.thenRun(new Runnable() {
            @Override
            public void run() {
                System.err.println("thenRun");
            }
        });

        sleepSeconds(100);


    }
}
