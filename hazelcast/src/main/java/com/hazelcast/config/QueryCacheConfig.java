/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;

import java.util.List;

import static com.hazelcast.util.ValidationUtil.checkNotNull;
import static com.hazelcast.util.ValidationUtil.hasText;
import static com.hazelcast.util.ValidationUtil.isNotInstanceOf;
import static com.hazelcast.util.ValidationUtil.isNotNegative;
import static com.hazelcast.util.ValidationUtil.shouldBePositive;

/**
 * // todo toString, hashCode, Equals
 * // todo listenerConfigs, stats, mapIndexConfigs
 *
 * @since 3.5
 */
public class QueryCacheConfig {

    private static final int DEFAULT_BATCH_SIZE = 1;
    private static final long DEFAULT_DELAY_TIME_MILLIS = 0;
    private static final boolean DEFAULT_INCLUDE_VALUE = true;
    private static final boolean DEFAULT_POPULATE_INITIALLY = true;
    private static final boolean DEFAULT_STATISTICS_ENABLED = true;
    private static final boolean DEFAULT_COALESCE_KEYS = false;
    private static final int DEFAULT_TTL_SECONDS = 0;
    private static final int DEFAULT_MAX_IDLE_SECONDS = 0;
    private static final EvictionPolicy DEFAULT_EVICTION_POLICY = EvictionPolicy.NONE;

    private int batchSize = DEFAULT_BATCH_SIZE;
    private long delayTime = DEFAULT_DELAY_TIME_MILLIS;
    private boolean includeValue = DEFAULT_INCLUDE_VALUE;
    private boolean populateInitially = DEFAULT_POPULATE_INITIALLY;
    private boolean coalesceKeys = DEFAULT_COALESCE_KEYS;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;
    private int maxIdleSeconds = DEFAULT_MAX_IDLE_SECONDS;
    private EvictionPolicy evictionPolicy = DEFAULT_EVICTION_POLICY;
    private List<ListenerConfig> listenerConfigs;
    private List<MapIndexConfig> mapIndexConfigs;
    private boolean statisticsEnabled = DEFAULT_STATISTICS_ENABLED;
    private String name;

    private Predicate predicate;

    public QueryCacheConfig(String name) {
        setName(name);
    }

    public String getName() {
        return name;
    }

    public QueryCacheConfig setName(String name) {
        hasText(name, "name");

        this.name = name;
        return this;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public QueryCacheConfig setPredicate(Predicate predicate) {
        checkNotNull(predicate, "predicate can not be null");
        isNotInstanceOf(PagingPredicate.class, predicate, "predicate");

        this.predicate = predicate;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public QueryCacheConfig setBatchSize(int batchSize) {
        shouldBePositive(batchSize, "batchSize");

        this.batchSize = batchSize;
        return this;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public QueryCacheConfig setDelayTime(long delayTime) {
        isNotNegative(delayTime, "delayTime");

        this.delayTime = delayTime;
        return this;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public QueryCacheConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        checkNotNull(evictionPolicy, "evictionPolicy can not be null");

        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public boolean isIncludeValue() {
        return includeValue;
    }

    public QueryCacheConfig setIncludeValue(boolean includeValue) {
        this.includeValue = includeValue;
        return this;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public QueryCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        isNotNegative(maxIdleSeconds, "maxIdleSeconds");

        this.maxIdleSeconds = maxIdleSeconds;
        return this;
    }

    public boolean isPopulateInitially() {
        return populateInitially;
    }

    public QueryCacheConfig setPopulateInitially(boolean populateInitially) {
        this.populateInitially = populateInitially;
        return this;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public QueryCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        isNotNegative(timeToLiveSeconds, "timeToLiveSeconds");

        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public boolean isCoalesceKeys() {
        return coalesceKeys;
    }

    public QueryCacheConfig setCoalesceKeys(boolean coalesceKeys) {
        this.coalesceKeys = coalesceKeys;
        return this;
    }

    public QueryCacheConfig addListenerConfig(ListenerConfig listenerConfig) {
        getListenerConfigs().add(listenerConfig);
        return this;
    }

    public List<ListenerConfig> getListenerConfigs() {
        return listenerConfigs;
    }
}
