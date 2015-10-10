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

package com.hazelcast.map.impl.tx;

import com.hazelcast.core.Member;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapKeySet;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.ContainsKeyOperation;
import com.hazelcast.map.impl.operation.GetOperation;
import com.hazelcast.map.impl.operation.MapEntrySetOperation;
import com.hazelcast.map.impl.operation.MapKeySetOperation;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.SizeOperationFactory;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.map.impl.query.QueryPartitionOperation;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultSet;
import com.hazelcast.map.impl.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.ThreadUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.nearcache.NearCache.NULL_OBJECT;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

/**
 * Base class contains proxy helper methods for {@link com.hazelcast.map.impl.tx.TransactionalMapProxy}
 */
public abstract class TransactionalMapProxySupport extends AbstractDistributedObject<MapService>
        implements TransactionalObject {

    protected final String name;
    protected final Transaction tx;
    protected final PartitioningStrategy partitionStrategy;
    protected final Map<Data, VersionedValue> valueMap = new HashMap<Data, VersionedValue>();
    protected final RecordFactory recordFactory;
    protected final MapOperationProvider operationProvider;

    public TransactionalMapProxySupport(String name, MapService mapService, NodeEngine nodeEngine,
                                        Transaction transaction) {
        super(nodeEngine, mapService);
        this.name = name;
        this.tx = transaction;
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        this.operationProvider = mapServiceContext.getMapOperationProvider(name);
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        this.recordFactory = mapContainer.getRecordFactoryConstructor().createNew(null);
        this.partitionStrategy = mapContainer.getPartitioningStrategy();
    }

    protected boolean isEquals(Object value1, Object value2) {
        return recordFactory.isEquals(value1, value2);
    }

    protected void checkTransactionState() {
        if (!tx.getState().equals(Transaction.State.ACTIVE)) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    public boolean containsKeyInternal(Data key) {
        ContainsKeyOperation operation = new ContainsKeyOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Future f = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return (Boolean) f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getInternal(Data key) {
        final MapService mapService = getService();
        final boolean nearCacheEnabled = mapService.getMapServiceContext().getMapContainer(name).isNearCacheEnabled();
        if (nearCacheEnabled) {
            Object cached = mapService.getMapServiceContext().getNearCacheProvider().getFromNearCache(name, key);
            if (cached != null) {
                if (cached.equals(NULL_OBJECT)) {
                    cached = null;
                }
                return cached;
            }
        }
        GetOperation operation = new GetOperation(name, key);
        operation.setThreadId(ThreadUtil.getThreadId());
        final NodeEngine nodeEngine = getNodeEngine();
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        try {
            Future f = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            return f.get();
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Object getForUpdateInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        addUnlockTransactionRecord(key, versionedValue.version);
        return versionedValue.value;
    }

    public int sizeInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(SERVICE_NAME, new SizeOperationFactory(name));
            int total = 0;
            for (Object result : results.values()) {
                Integer size = (Integer) getService().getMapServiceContext().toObject(result);
                total += size;
            }
            return total;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Data putInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data putInternal(Data key, Data value, long ttl, TimeUnit timeUnit) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        final long timeInMillis = getTimeInMillis(ttl, timeUnit);
        MapOperation operation
                = operationProvider.createTxnSetOperation(name, key, value, versionedValue.version, timeInMillis);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operation, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data putIfAbsentInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue.value != null) {
            addUnlockTransactionRecord(key, versionedValue.version);
            return versionedValue.value;
        }

        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public Data replaceInternal(Data key, Data value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (versionedValue.value == null) {
            addUnlockTransactionRecord(key, versionedValue.version);
            return null;
        }
        final TxnSetOperation op = new TxnSetOperation(name, key, value, versionedValue.version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), op, versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean replaceIfSameInternal(Data key, Object oldValue, Data newValue) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!isEquals(oldValue, versionedValue.value)) {
            addUnlockTransactionRecord(key, versionedValue.version);
            return false;
        }
        final TxnSetOperation op = new TxnSetOperation(name, key, newValue, versionedValue.version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), op, versionedValue.version, tx.getOwnerUuid()));
        return true;
    }

    public Data removeInternal(Data key) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());

        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operationProvider.createTxnDeleteOperation(name, key, versionedValue.version),
                versionedValue.version, tx.getOwnerUuid()));
        return versionedValue.value;
    }

    public boolean removeIfSameInternal(Data key, Object value) {
        VersionedValue versionedValue = lockAndGet(key, tx.getTimeoutMillis());
        if (!isEquals(versionedValue.value, value)) {
            addUnlockTransactionRecord(key, versionedValue.version);
            return false;
        }
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key),
                operationProvider.createTxnDeleteOperation(name, key, versionedValue.version),
                versionedValue.version, tx.getOwnerUuid()));
        return true;
    }

    private void addUnlockTransactionRecord(Data key, long version) {
        TxnUnlockOperation operation = new TxnUnlockOperation(name, key, version);
        tx.add(new MapTransactionLogRecord(name, key, getPartitionId(key), operation, version, tx.getOwnerUuid()));
    }

    private VersionedValue lockAndGet(Data key, long timeout) {
        VersionedValue versionedValue = valueMap.get(key);
        if (versionedValue != null) {
            return versionedValue;
        }
        final NodeEngine nodeEngine = getNodeEngine();
        MapOperation operation
                = operationProvider.createTxnLockAndGetOperation(name, key, timeout, timeout, tx.getOwnerUuid());
        operation.setThreadId(ThreadUtil.getThreadId());
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            Future<VersionedValue> f = nodeEngine.getOperationService()
                    .invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
            versionedValue = f.get();
            if (versionedValue == null) {
                throw new TransactionException("Transaction couldn't obtain lock for the key:"
                        + getService().getMapServiceContext().toObject(key));
            }
            valueMap.put(key, versionedValue);
            return versionedValue;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Set<Data> keySetInternal() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            SERVICE_NAME,
                            new BinaryOperationFactory(new MapKeySetOperation(name), nodeEngine)
                    );
            Set<Data> keySet = new HashSet<Data>();
            for (Object result : results.values()) {
                Set<Data> keys = ((MapKeySet) getService().getMapServiceContext().toObject(result)).getKeySet();
                keySet.addAll(keys);
            }
            return keySet;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected List<Map.Entry<Data, Data>> getEntries() {
        final NodeEngine nodeEngine = getNodeEngine();
        try {
            Map<Integer, Object> results = nodeEngine.getOperationService()
                    .invokeOnAllPartitions(
                            SERVICE_NAME,
                            new BinaryOperationFactory(new MapEntrySetOperation(name), nodeEngine)
                    );
            List<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>();
            for (Object result : results.values()) {
                MapEntries mapEntries = ((MapEntries) getService().getMapServiceContext().toObject(result));
                entries.addAll(mapEntries.entries());
            }
            return entries;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    protected Set queryInternal(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        NodeEngine nodeEngine = getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> partitions = new HashSet<Integer>(partitionCount);
        QueryResultSet result = new QueryResultSet(nodeEngine.getSerializationService(), iterationType, dataResult);

        try {
            List futures = invokeQueryOperation(predicate, operationService, members, iterationType);
            collectResults(partitions, result, futures);
            if (partitions.size() == partitionCount) {
                return result;
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw ExceptionUtil.rethrow(t);
            }
            EmptyStatement.ignore(t);
        }

        List<Integer> missingList = new ArrayList<Integer>();
        findMissingPartitions(partitionCount, partitions, missingList);
        try {
            List<Future> missingFutures = new ArrayList<Future>(missingList.size());
            invokeOperationOnMissingPartitions(predicate, operationService, missingList, missingFutures);
            collectResultsFromMissingPartitions(result, missingFutures);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    private List<Future> invokeQueryOperation(Predicate predicate, OperationService operationService,
                                              Collection<Member> members, IterationType iterationType) {
        String mapName = this.name;
        List<Future> futures = new ArrayList<Future>();
        for (Member member : members) {
            Operation operation = new QueryOperation(mapName, predicate, iterationType);
            Future future = operationService
                    .invokeOnTarget(SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }

        return futures;
    }

    private MapOperationProvider getMapOperationProvider(String mapName) {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapName);
    }

    @SuppressWarnings("unchecked")
    private void collectResults(Set<Integer> plist, QueryResultSet result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {
                Collection<Integer> partitionIds = queryResult.getPartitionIds();
                if (partitionIds != null) {
                    plist.addAll(partitionIds);
                    result.addAll(queryResult.getRows());
                }
            }
        }
    }

    private void findMissingPartitions(int partitionCount, Set<Integer> plist, List<Integer> missingList) {
        for (int i = 0; i < partitionCount; i++) {
            if (!plist.contains(i)) {
                missingList.add(i);
            }
        }
    }

    private void invokeOperationOnMissingPartitions(Predicate predicate, OperationService operationService,
                                                    List<Integer> missingList, List<Future> futures) {
        for (Integer pid : missingList) {
            //todo: potential performance problem since both key/value are retrieved.
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate, IterationType.ENTRY);
            queryPartitionOperation.setPartitionId(pid);
            try {
                Future f = operationService.invokeOnPartition(SERVICE_NAME, queryPartitionOperation, pid);
                futures.add(f);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void collectResultsFromMissingPartitions(QueryResultSet result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            result.addAll(queryResult.getRows());
        }
    }

    protected long getTimeInMillis(final long time, final TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
    }

    public String getName() {
        return name;
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }
}
