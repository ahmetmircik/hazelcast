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

package com.hazelcast.map.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.impl.eviction.EvictionOperator;
import com.hazelcast.map.impl.eviction.EvictionOperatorFactory;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.nearcache.NearCacheSizeEstimator;
import com.hazelcast.map.impl.query.QueryEntryFactory;
import com.hazelcast.map.impl.record.DataRecordFactory;
import com.hazelcast.map.impl.record.ObjectRecordFactory;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationPublisher;
import com.hazelcast.wan.WanReplicationService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.map.impl.mapstore.MapStoreContextFactory.createMapStoreContext;

/**
 * Map container.
 */
public class MapContainer {

    protected final String name;
    protected final String quorumName;
    protected final MapServiceContext mapServiceContext;
    protected final Map<String, MapInterceptor> interceptorMap;
    protected final Indexes indexes;
    protected final SizeEstimator nearCacheSizeEstimator;
    protected final PartitioningStrategy partitioningStrategy;
    protected final MapStoreContext mapStoreContext;
    protected final SerializationService serializationService;
    protected final QueryEntryFactory queryEntryFactory;
    protected final IFunction<Object, Data> toDataFunction = new IFunction<Object, Data>() {
        @Override
        public Data apply(Object input) {
            SerializationService ss = mapStoreContext.getSerializationService();
            return ss.toData(input, partitioningStrategy);
        }
    };
    protected final List<MapInterceptor> interceptors;

    protected EvictionOperator evictionOperator;
    protected WanReplicationPublisher wanReplicationPublisher;
    protected MapMergePolicy wanMergePolicy;
    protected ConstructorFunction<Void, RecordFactory> recordFactoryConstructor;
    protected volatile MapConfig mapConfig;

    /**
     * Operations which are done in this constructor should obey the rules defined
     * in the method comment {@link com.hazelcast.spi.PostJoinAwareService#getPostJoinOperation()}
     * Otherwise undesired situations, like deadlocks, may appear.
     */
    public MapContainer(final String name, final MapConfig mapConfig, final MapServiceContext mapServiceContext) {
        this.name = name;
        this.mapConfig = mapConfig;
        this.mapServiceContext = mapServiceContext;
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.partitioningStrategy = createPartitioningStrategy();
        this.quorumName = mapConfig.getQuorumName();
        this.serializationService = nodeEngine.getSerializationService();
        this.recordFactoryConstructor = createRecordFactoryConstructor(serializationService);
        initWanReplication(nodeEngine);
        this.queryEntryFactory = new QueryEntryFactory(mapConfig.isOptimizeQueries());
        initWanReplication(nodeEngine);
        this.interceptors = new CopyOnWriteArrayList<MapInterceptor>();
        this.interceptorMap = new ConcurrentHashMap<String, MapInterceptor>();
        this.nearCacheSizeEstimator = new NearCacheSizeEstimator();
        this.evictionOperator = createEvictionOperator(mapServiceContext);
        this.mapStoreContext = createMapStoreContext(this);
        this.mapStoreContext.start();
        this.indexes = new Indexes(serializationService);
    }

    // this method is overridden.
    EvictionOperator createEvictionOperator(MapServiceContext mapServiceContext) {
        return EvictionOperatorFactory.create(mapServiceContext);
    }

    // overridden in different context.
    ConstructorFunction<Void, RecordFactory> createRecordFactoryConstructor(final SerializationService serializationService) {
        return new ConstructorFunction<Void, RecordFactory>() {
            @Override
            public RecordFactory createNew(Void notUsedArg) {
                RecordFactory recordFactory;
                switch (mapConfig.getInMemoryFormat()) {
                    case BINARY:
                        recordFactory = new DataRecordFactory(mapConfig, serializationService, partitioningStrategy);
                        break;
                    case OBJECT:
                        recordFactory = new ObjectRecordFactory(mapConfig, serializationService);
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid storage format: " + mapConfig.getInMemoryFormat());
                }

                return recordFactory;
            }
        };
    }


    public void initWanReplication(NodeEngine nodeEngine) {
        WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
        if (wanReplicationRef == null) {
            return;
        }
        String wanReplicationRefName = wanReplicationRef.getName();
        WanReplicationService wanReplicationService = nodeEngine.getWanReplicationService();
        wanReplicationPublisher = wanReplicationService.getWanReplicationPublisher(wanReplicationRefName);
        wanMergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(wanReplicationRef.getMergePolicy());
    }

    private PartitioningStrategy createPartitioningStrategy() {
        PartitioningStrategy strategy = null;
        PartitioningStrategyConfig partitioningStrategyConfig = mapConfig.getPartitioningStrategyConfig();
        if (partitioningStrategyConfig != null) {
            strategy = partitioningStrategyConfig.getPartitioningStrategy();
            if (strategy == null && partitioningStrategyConfig.getPartitioningStrategyClass() != null) {
                try {
                    strategy = ClassLoaderUtil.newInstance(mapServiceContext.getNodeEngine().getConfigClassLoader(),
                            partitioningStrategyConfig.getPartitioningStrategyClass());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
        }
        return strategy;
    }

    public Indexes getIndexes() {
        return indexes;
    }

    public WanReplicationPublisher getWanReplicationPublisher() {
        return wanReplicationPublisher;
    }

    public MapMergePolicy getWanMergePolicy() {
        return wanMergePolicy;
    }

    public void addInterceptor(String id, MapInterceptor interceptor) {

        removeInterceptor(id);

        interceptorMap.put(id, interceptor);
        interceptors.add(interceptor);
    }

    public List<MapInterceptor> getInterceptors() {
        return interceptors;
    }

    public Map<String, MapInterceptor> getInterceptorMap() {
        return interceptorMap;
    }

    public void removeInterceptor(String id) {
        MapInterceptor interceptor = interceptorMap.remove(id);
        interceptors.remove(interceptor);
    }

    public boolean isNearCacheEnabled() {
        return mapConfig.isNearCacheEnabled();
    }

    public int getTotalBackupCount() {
        return getBackupCount() + getAsyncBackupCount();
    }

    public int getBackupCount() {
        return mapConfig.getBackupCount();
    }

    public int getAsyncBackupCount() {
        return mapConfig.getAsyncBackupCount();
    }

    public PartitioningStrategy getPartitioningStrategy() {
        return partitioningStrategy;
    }

    public SizeEstimator getNearCacheSizeEstimator() {
        return nearCacheSizeEstimator;
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    public MapStoreContext getMapStoreContext() {
        return mapStoreContext;
    }

    public MapConfig getMapConfig() {
        return mapConfig;
    }

    public void setMapConfig(MapConfig mapConfig) {
        this.mapConfig = mapConfig;
    }

    public String getName() {
        return name;
    }

    public String getQuorumName() {
        return quorumName;
    }

    public IFunction<Object, Data> toData() {
        return toDataFunction;
    }

    public ConstructorFunction<Void, RecordFactory> getRecordFactoryConstructor() {
        return recordFactoryConstructor;
    }

    public EvictionOperator getEvictionOperator() {
        return evictionOperator;
    }

    // only used for testing purposes.
    public void setEvictionOperator(EvictionOperator evictionOperator) {
        this.evictionOperator = evictionOperator;
    }

    public QueryableEntry newQueryEntry(Data key, Object value) {
        return queryEntryFactory.newEntry(serializationService, key, value);
    }
}


