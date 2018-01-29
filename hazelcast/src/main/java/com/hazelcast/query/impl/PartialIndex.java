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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PartialIndex implements Index {

    // a = 1 and b = 4 and c = 5
    private final Predicate mainPredicate;
    private final Set<QueryableEntry> filteredResultSet
            = Collections.newSetFromMap(new ConcurrentHashMap<QueryableEntry, Boolean>());

    public PartialIndex(Predicate mainPredicate) {
        this.mainPredicate = mainPredicate;
    }

    @Override
    public void addToIndex(QueryableEntry e, Object oldValue) throws QueryException {
        if (!mainPredicate.apply(e)) {
            return;
        }

        filteredResultSet.add(e);
    }

    @Override
    public void removeFromIndex(Data keyData, Object value) {
        filteredResultSet.remove(new QueryEntry(null, keyData, value, null));
    }

    @Override
    public void clear() {
        filteredResultSet.clear();
    }

    public Set<QueryableEntry> getEntries() {
        return filteredResultSet;
    }

    @Override
    public void destroy() {
        // Intentionally empty method body.
    }

    @Override
    public String getAttributeName() {
        throw new UnsupportedOperationException("getAttributeName() cannot be used");
    }

    @Nullable
    @Override
    public TypeConverter getConverter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getRecords(Comparable[] values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOrdered() {
        throw new UnsupportedOperationException();
    }
}
