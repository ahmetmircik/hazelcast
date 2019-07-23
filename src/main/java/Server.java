/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.eviction.ExpirationManager;

import java.util.concurrent.TimeUnit;

public class Server {

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setProperty(ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_PERCENTAGE,"100");
        config.setProperty(ExpirationManager.SYS_PROP_EXPIRATION_TASK_PERIOD_SECONDS,"2");
        config.setProperty(ExpirationManager.SYS_PROP_EXPIRATION_CLEANUP_OPERATION_COUNT,"271");

        HazelcastInstance node = Hazelcast.newHazelcastInstance(config);
        IMap<Object, Object> test = node.getMap("test");

        while (true) {
            for (int i = 0; i < 1000; i++) {
                test.set(i, i, 100, TimeUnit.MILLISECONDS);
            }
            TimeUnit.SECONDS.sleep(5);
        }
    }
}
