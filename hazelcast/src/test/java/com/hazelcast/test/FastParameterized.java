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

public class FastParameterized extends Parameterized {

    /**
     * Only called reflectively. Do not use programmatically.
     *
     * @param klass
     */
    public FastParameterized(Class<?> klass) throws Throwable {
        super(klass);
        setScheduler(new RunnerScheduler() {
            private final ExecutorService fService = Executors.newFixedThreadPool(RuntimeAvailableProcessors.get());

            public void schedule(Runnable childStatement) {
                this.fService.submit(childStatement);
            }

            public void finished() {
                try {
                    this.fService.shutdown();
                    this.fService.awaitTermination(9223372036854775807L, TimeUnit.NANOSECONDS);
                } catch (InterruptedException var2) {
                    var2.printStackTrace(System.err);
                }

            }
        });
    }
}
