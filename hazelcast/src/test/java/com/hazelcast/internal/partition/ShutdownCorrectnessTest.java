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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({SlowTest.class})
public class ShutdownCorrectnessTest extends HazelcastTestSupport {

    @Test
    public void name() {
        Config config = new Config();
        config.getMetricsConfig().setEnabled(false);
        config.getMapConfig("default").setBackupCount(0);
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory();
        HazelcastInstance one = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance = instanceFactory.newHazelcastInstance(config);

//        IMap<Object, Object> map = instance.getMap("test");
//        for (int i = 0; i < 10_000; i++) {
//            map.set(i, i);
//        }
//
//        //suspectMember(one, instance);
//        instance.shutdown();
//
//
//        int size = one.getMap("test").size();
//        assertEquals(size, 10_000);

        sleepSeconds(10);
    }
}
