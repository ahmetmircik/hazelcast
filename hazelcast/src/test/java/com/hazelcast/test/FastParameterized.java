/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import org.junit.runners.Parameterized;
import org.junit.runners.model.RunnerScheduler;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;

public class FastParameterized extends Parameterized {

    private static final int PARALLELISM = Math.max(2, RuntimeAvailableProcessors.get());

    private static final ForkJoinPool POOL = new ForkJoinPool(PARALLELISM, p -> {
        ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
        thread.setName("hz.test.fast.parameterized." + thread.getName());
        return thread;
    }, null, true);

    private final List<ForkJoinTask<?>> tests = new LinkedList<>();

    public FastParameterized(Class<?> klass) throws Throwable {
        super(klass);

        setScheduler(new RunnerScheduler() {

            @Override
            public void schedule(Runnable test) {
                tests.add(POOL.submit(test));
            }

            @Override
            public void finished() {
                List<Throwable> throwables = new ArrayList<>();

                for (ForkJoinTask<?> test : tests) {
                    try {
                        test.join();
                    } catch (Throwable t) {
                        throwables.add(t);
                    }
                }
            }
        });
    }
}
