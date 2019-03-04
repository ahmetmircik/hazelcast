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

package com.hazelcast.map.impl.tx;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TxnMapWriteBehindStoreTest extends HazelcastTestSupport {

    @Test
    public void prepare_fails_when_no_space_in_write_behind_queue() {
        final String mapName = "map";

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteCoalescing(false);
        mapStoreConfig.setWriteDelaySeconds(100);
        mapStoreConfig.setImplementation(new MapStoreAdapter());

        Config config = getConfig();
        config.setProperty(GroupProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName(), "1");
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
//        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        instance.getMap(mapName).put(1, 1);

        TransactionOptions transactionOptions = new TransactionOptions();
        transactionOptions.setTransactionType(TransactionOptions.TransactionType.TWO_PHASE);
        TransactionContext context = instance.newTransactionContext(transactionOptions);
        context.beginTransaction();

        TransactionalMap map = context.getMap(mapName);

        map.put(2, 2);
        map.put(3, 3);

        context.commitTransaction();

        assertEquals(2, map.size());
    }

    @Test
    public void prepare_fails_rollback_succeeds_when_no_space_in_write_behind_queue() {

    }
}
