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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.query.DefaultIndexProvider;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SampleTestObjects.Value;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.TestUtil.toData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class IndexesTest {

    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Parameterized.Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameterized.Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {IndexCopyBehavior.NEVER}
        });
    }

    @Test
    public void testAndWithSingleEntry() throws Exception {
        Indexes indexes = new Indexes(serializationService, new DefaultIndexProvider(), Extractors.empty(), true, copyBehavior);
        indexes.addOrGetIndex("name", false);
        indexes.addOrGetIndex("age", true);
        indexes.addOrGetIndex("salary", true);

        int count = 10;
        Set<String> ages = new HashSet<String>(count);
        for (int i = 0; i < count; i++) {
            ages.add(String.valueOf(i));
        }

        EntryObject entryObject = new PredicateBuilder().getEntryObject();
        PredicateBuilder predicate = entryObject.get("name").equal("0Name")
                .and(entryObject.get("age").in(ages.toArray(new String[count])));
        indexes.addOrGetPartialIndex(predicate.getPredicate());

        for (int i = 0; i < 100000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 1000));
            indexes.saveEntryIndex(new QueryEntry(serializationService, toData(i), employee, Extractors.empty()), null);
        }

        Set<QueryableEntry> results = null;
        long start = System.nanoTime();
        for (int i = 0; i < 200000; i++) {
            results = indexes.query(predicate);
        }
        long elapsed = System.nanoTime() - start;

        System.err.println(TimeUnit.NANOSECONDS.toMillis(elapsed));

        assertEquals(1, results.size());
    }

    @Test
    public void testIndex() throws Exception {
        Indexes indexes = new Indexes(serializationService, new DefaultIndexProvider(), Extractors.empty(), true, copyBehavior);
        indexes.addOrGetIndex("name", false);
        indexes.addOrGetIndex("age", true);
        indexes.addOrGetIndex("salary", true);
        for (int i = 0; i < 2000; i++) {
            Employee employee = new Employee(i + "Name", i % 80, (i % 2 == 0), 100 + (i % 100));
            indexes.saveEntryIndex(new QueryEntry(serializationService, toData(i), employee, Extractors.empty()), null);
        }

        for (int i = 0; i < 10; i++) {
            SqlPredicate predicate = new SqlPredicate("salary=161 and age >20 and age <23");
            Set<QueryableEntry> results = new HashSet<QueryableEntry>(indexes.query(predicate));
            assertEquals(5, results.size());
        }
    }

    @Test
    public void testIndex2() throws Exception {
        Indexes indexes = new Indexes(serializationService, new DefaultIndexProvider(), Extractors.empty(), true, copyBehavior);
        indexes.addOrGetIndex("name", false);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(1), new Value("abc"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(2), new Value("xyz"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(3), new Value("aaa"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(4), new Value("zzz"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(5), new Value("klm"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(6), new Value("prs"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(7), new Value("prs"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(8), new Value("def"), Extractors.empty()), null);
        indexes.saveEntryIndex(new QueryEntry(serializationService, toData(9), new Value("qwx"), Extractors.empty()), null);
        assertEquals(8, new HashSet<QueryableEntry>(indexes.query(new SqlPredicate("name > 'aac'"))).size());
    }

    /**
     * Imagine we have only keys and nullable values. And we add index for a field of that nullable object.
     * When we execute a query on keys, there should be no returned value from indexing service and it does not
     * throw exception.
     */
    @Test
    public void shouldNotThrowException_withNullValues_whenIndexAddedForValueField() throws Exception {
        Indexes indexes = new Indexes(serializationService, new DefaultIndexProvider(), Extractors.empty(), true, copyBehavior);
        indexes.addOrGetIndex("name", false);

        shouldReturnNull_whenQueryingOnKeys(indexes);
    }

    @Test
    public void shouldNotThrowException_withNullValues_whenNoIndexAdded() throws Exception {
        Indexes indexes = new Indexes(serializationService, new DefaultIndexProvider(), Extractors.empty(), true, copyBehavior);

        shouldReturnNull_whenQueryingOnKeys(indexes);
    }

    private void shouldReturnNull_whenQueryingOnKeys(Indexes indexes) {
        for (int i = 0; i < 50; i++) {
            // passing null value to QueryEntry
            indexes.saveEntryIndex(new QueryEntry(serializationService, toData(i), null, Extractors.empty()), null);
        }

        Set<QueryableEntry> query = indexes.query(new SqlPredicate("__key > 10 "));

        assertNull("There should be no result", query);
    }

    @Test
    public void shouldNotThrowException_withNullValue_whenIndexAddedForKeyField() throws Exception {
        Indexes indexes = new Indexes(serializationService, new DefaultIndexProvider(), Extractors.empty(), true, copyBehavior);
        indexes.addOrGetIndex("__key", false);

        for (int i = 0; i < 100; i++) {
            // passing null value to QueryEntry
            indexes.saveEntryIndex(new QueryEntry(serializationService, toData(i), null, Extractors.empty()), null);
        }

        Set<QueryableEntry> query = indexes.query(new SqlPredicate("__key > 10 "));

        assertEquals(89, query.size());
    }
}
