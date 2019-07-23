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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.config.ClientProperties.PROP_HEARTBEAT_TIMEOUT;

public class Client {
    public static void main(String[] args) throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(PROP_HEARTBEAT_TIMEOUT,"5000");
        clientConfig.getNetworkConfig()
                .setConnectionAttemptPeriod(2)
                .setConnectionTimeout(2000)
                .setConnectionAttemptLimit(100);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        IMap<Object, Object> test = client.getMap("test");

        test.addEntryListener(new EntryAdapter(), true);


        while (true) {
            for (int i = 0; i < 1000; i++) {
                test.get(i);
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
