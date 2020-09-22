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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FastParameterized2 extends Parameterized {

    private static final int PARALLELISM = Math.max(2, RuntimeAvailableProcessors.get());

    private final ExecutorService executor = Executors.newFixedThreadPool(PARALLELISM);

    public FastParameterized2(Class<?> klass) throws Throwable {
        super(klass);

        setScheduler(new RunnerScheduler() {

            @Override
            public void schedule(Runnable test) {
                executor.execute(test);
            }

            @Override
            public void finished() {
                try {
                    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
