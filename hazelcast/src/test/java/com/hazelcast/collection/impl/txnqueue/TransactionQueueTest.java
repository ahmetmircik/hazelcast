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

package com.hazelcast.collection.impl.txnqueue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.collection.impl.queue.model.VersionedObject;
import com.hazelcast.collection.impl.queue.model.VersionedObjectComparator;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.transaction.TransactionContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TransactionQueueTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "comparatorClassName: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{null, VersionedObjectComparator.class.getName()});
    }

    @Parameterized.Parameter
    public String comparatorClassName;

    @Test @Repeat(30)
    public void testIssue859And863_WhenQueuesAreOnFirstInstance() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        String inQueueName = generateKeyOwnedBy(instance1);
        String outQueueName = generateKeyOwnedBy(instance1);

        testIssue859And863(instance1, instance2, inQueueName, outQueueName);
    }


    // https://github.com/hazelcast/hazelcast/issues/3796
    private void testIssue859And863(HazelcastInstance instance1,
                                    HazelcastInstance instance2,
                                    String inQueueName,
                                    String outQueueName) {
        int numberOfMessages = 3000;
        AtomicInteger counter = new AtomicInteger();

        IQueue<VersionedObject<Integer>> inQueue = instance1.getQueue(inQueueName);
        for (int i = 0; i < numberOfMessages; i++) {
            if (!inQueue.offer(new VersionedObject<>(i, i))) {
                throw new RuntimeException("initial put did not work");
            }
        }
        Thread[] instance1Threads = createThreads(instance1, 3, inQueueName, outQueueName, counter);
        Thread[] instance2Threads = createThreads(instance2, 3, inQueueName, outQueueName, counter);

        try {
            startThreads(instance1Threads);
            startThreads(instance2Threads);

            while (counter.get() < numberOfMessages / 2) {
                LockSupport.parkNanos(1000);
            }
            instance2.getLifecycleService().shutdown();

            interruptThreads(instance2Threads);

            assertJoinable(instance2Threads);

            // When a node goes down, backup of the transaction commits all prepared stated transactions
            // Since it relies on 'memberRemoved' event, it is async. That's why we should assert eventually
            assertTrueEventually(() -> {
                assertEquals(numberOfMessages, instance1.getQueue(outQueueName).size());
                assertTrue(instance1.getQueue(inQueueName).isEmpty());
            });
        } finally {
            interruptThreads(instance1Threads);
            interruptThreads(instance2Threads);
            assertJoinable(instance1Threads);
        }
    }

    private Thread[] createThreads(HazelcastInstance instance,
                                   int threadCount, String in, String out,
                                   AtomicInteger counter) {
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            MoveMessage moveMessage = new MoveMessage(instance, in, out, counter);
            threads[i] = new Thread(moveMessage);
        }
        return threads;
    }

    private void interruptThreads(Thread[] threads) {
        for (Thread thread : threads) {
            thread.interrupt();
        }
    }

    private void startThreads(Thread[] threads) {
        for (Thread thread : threads) {
            thread.start();
        }
    }


    private void assertTransactionMapSize(HazelcastInstance instance, String name, int size) {
        QueueService queueService = getNode(instance).nodeEngine.getService(QueueService.SERVICE_NAME);
        assertEquals(size, queueService.getOrCreateContainer(name, true).txMapSize());
    }

    private <E> IQueue<E> getQueue(HazelcastInstance[] instances, String name) {
        Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getQueue(name);
    }

    static class MoveMessage implements Runnable {
        private final String inQueueName;
        private final String outQueueName;
        private final AtomicInteger counter;

        private final HazelcastInstance hazelcastInstance;

        MoveMessage(HazelcastInstance hazelcastInstance,
                    String inQueueName, String outQueueName,
                    AtomicInteger counter) {
            this.hazelcastInstance = hazelcastInstance;
            this.inQueueName = inQueueName;
            this.outQueueName = outQueueName;
            this.counter = counter;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TransactionContext transactionContext = hazelcastInstance.newTransactionContext();
                    transactionContext.beginTransaction();
                    try {
                        Object item = transactionContext.getQueue(inQueueName).poll();
                        if (item != null && !transactionContext.getQueue(outQueueName).offer(item)) {
                            throw new RuntimeException("Out Queue wouldn't accept item");
                        }
                        transactionContext.commitTransaction();
                        if (item != null) {
                            counter.incrementAndGet();
                        }
                    } catch (HazelcastInstanceNotActiveException e) {
                        throw e;
                    } catch (Exception e) {
                        try {
                            transactionContext.rollbackTransaction();
                        } catch (HazelcastInstanceNotActiveException ignored) {
                        }
                    }
                } catch (HazelcastInstanceNotActiveException e) {
                    break;
                }
            }
        }
    }


    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getQueueConfig("default")
              .setPriorityComparatorClassName(comparatorClassName);
        return config;
    }
}
