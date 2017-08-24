/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.internal.config.ConfigUtils;
import com.hazelcast.map.impl.querycache.QueryCacheConfigurator;
import com.hazelcast.map.impl.querycache.QueryCacheEventService;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Node side implementation of {@link QueryCacheConfigurator}.
 *
 * @see QueryCacheConfigurator
 */
public class NodeQueryCacheConfigurator extends AbstractQueryCacheConfigurator {

    private final Config config;

    public NodeQueryCacheConfigurator(Config config, ClassLoader configClassLoader,
                                      QueryCacheEventService eventService) {
        super(configClassLoader, eventService);
        this.config = config;
    }

    @Override
    public QueryCacheConfig getOrCreateConfiguration(String mapName, String userGivenCacheName) {
        MapConfig mapConfig = config.getMapConfig(mapName);

        QueryCacheConfig queryCacheConfig = findQueryCacheConfigFromMapConfig(mapConfig, userGivenCacheName);

        if (null != queryCacheConfig) {
            setPredicateImpl(queryCacheConfig);
            setEntryListener(mapName, userGivenCacheName, queryCacheConfig);
            return queryCacheConfig;
        }

        QueryCacheConfig newConfig = new QueryCacheConfig(userGivenCacheName);
        mapConfig.getQueryCacheConfigs().add(newConfig);
        return newConfig;
    }

    @Override
    public QueryCacheConfig getOrNull(String mapName, String userGivenCacheName) {
        MapConfig mapConfig = config.getMapConfigOrNull(mapName);
        if (null == mapConfig) {
            return null;
        }

        return findQueryCacheConfigFromMapConfig(mapConfig, userGivenCacheName);
    }

    private QueryCacheConfig findQueryCacheConfigFromMapConfig(MapConfig mapConfig, String userGivenCacheName) {
        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        Map<String, QueryCacheConfig> allQueryCacheConfigs = new HashMap<String, QueryCacheConfig>(queryCacheConfigs.size());
        for (QueryCacheConfig queryCacheConfig : queryCacheConfigs) {
            allQueryCacheConfigs.put(queryCacheConfig.getName(), queryCacheConfig);
        }

        return ConfigUtils.lookupByPattern(config.getConfigPatternMatcher(), allQueryCacheConfigs, userGivenCacheName);
    }

    @Override
    public void removeConfiguration(String mapName, String userGivenCacheName) {
        MapConfig mapConfig = config.getMapConfig(mapName);
        List<QueryCacheConfig> queryCacheConfigs = mapConfig.getQueryCacheConfigs();
        if (queryCacheConfigs == null || queryCacheConfigs.isEmpty()) {
            return;
        }
        Iterator<QueryCacheConfig> iterator = queryCacheConfigs.iterator();
        while (iterator.hasNext()) {
            QueryCacheConfig config = iterator.next();
            if (config.getName().equals(userGivenCacheName)) {
                iterator.remove();
            }
        }
    }
}
