package com.hazelcast.map.proxy;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.QueryResult;
import com.hazelcast.map.operation.QueryOperation;
import com.hazelcast.map.operation.QueryPartitionOperation;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;
import com.hazelcast.util.SortedQueryResultSet;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.map.MapService.SERVICE_NAME;

/**
 * Support methods for queryOnMembers operations in {@link com.hazelcast.map.proxy.MapProxySupport}.
 */
public class MapProxyQuerySupport {

    private String name;
    private NodeEngine nodeEngine;

    public MapProxyQuerySupport(String name, NodeEngine nodeEngine) {
        this.name = name;
        this.nodeEngine = nodeEngine;
    }


    /**
     * Used for paging predicate queries on node local entries.
     *
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    public Set queryLocalWithPagingPredicate(final PagingPredicate pagingPredicate,
                                             final IterationType iterationType) {
        final NodeEngine nodeEngine = this.nodeEngine;
        List<Integer> partitionIds = nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
        pagingPredicate.setIterationType(iterationType);
        if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
            pagingPredicate.previousPage();
            queryLocalWithPagingPredicate(pagingPredicate, iterationType);
            pagingPredicate.nextPage();
        }
        final Set result = new SortedQueryResultSet(pagingPredicate.getComparator(),
                iterationType, pagingPredicate.getPageSize());
        try {
            final Future future = queryLocal(pagingPredicate, nodeEngine);
            final List<Future> singletonList = Collections.singletonList(future);
            addPagingPredicateResults(singletonList, result, partitionIds);
            if (partitionIds.isEmpty()) {
                PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate,
                        ((SortedQueryResultSet) result).last());
                return result;
            }

            final List<Future> futures = queryOnPartitions(pagingPredicate, partitionIds, nodeEngine);
            addPagingPredicateResults(futures, result, partitionIds);

        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    /**
     * Used for predicates which queries on node local entries, except paging predicate.
     *
     * @param predicate     except paging predicate.
     * @param iterationType type of {@link IterationType}
     * @param dataResult    <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return {@link QueryResultSet}
     */
    public Set queryLocal(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final List<Integer> partitionIds = nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
        final SerializationService serializationService = nodeEngine.getSerializationService();
        final Set result = new QueryResultSet(serializationService, iterationType, dataResult);
        try {
            final Future future = queryLocal(predicate, nodeEngine);
            final List<Future> singletonList = Collections.singletonList(future);
            addPredicateResults(singletonList, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
            List<Future> futures = queryOnPartitions(predicate, partitionIds, nodeEngine);
            addPredicateResults(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    /**
     * Used for paging predicate queries on all members.
     *
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    public Set queryWithPagingPredicate(PagingPredicate pagingPredicate, final IterationType iterationType) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> partitionIds = newSetPopulatedWithPartitionIds(partitionCount);
        pagingPredicate.setIterationType(iterationType);
        if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
            pagingPredicate.previousPage();
            queryWithPagingPredicate(pagingPredicate, iterationType);
            pagingPredicate.nextPage();
        }
        final Set result = new SortedQueryResultSet(pagingPredicate.getComparator(),
                iterationType, pagingPredicate.getPageSize());
        try {
            final List<Future> futures = queryOnMembers(pagingPredicate, members, nodeEngine);
            addPagingPredicateResults(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
                return result;
            }
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Exception while querying ", t);
        }

        try {
            final List<Future> futures = queryOnPartitions(pagingPredicate, partitionIds, nodeEngine);
            addPagingPredicateResults(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
        return result;
    }

    /**
     * Used for predicates which queries on all members, except paging predicate.
     *
     * @param predicate     except paging predicate.
     * @param iterationType type of {@link IterationType}
     * @param dataResult    <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return {@link QueryResultSet}
     */
    public Set queryx(final Predicate predicate,
                     final IterationType iterationType, final boolean dataResult) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final SerializationService serializationService = nodeEngine.getSerializationService();
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> partitionIds = newSetPopulatedWithPartitionIds(partitionCount);
        final Set result = new QueryResultSet(serializationService, iterationType, dataResult);
        try {
            final List<Future> futures = queryOnMembers(predicate, members, nodeEngine);
            addPredicateResults(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Exception while querying ", t);
        }

        try {
            final List<Future> futures = queryOnPartitions(predicate, partitionIds, nodeEngine);
            addPredicateResults(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    private Future queryLocal(Predicate predicate, NodeEngine nodeEngine) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Future future = operationService
                .invokeOnTarget(SERVICE_NAME,
                        new QueryOperation(name, predicate),
                        nodeEngine.getThisAddress());
        return future;
    }

    private List<Future> queryOnMembers(Predicate predicate,
                                        Collection<MemberImpl> members, NodeEngine nodeEngine) {
        final List<Future> futures = new ArrayList<Future>(members.size());
        final OperationService operationService = nodeEngine.getOperationService();
        for (MemberImpl member : members) {
            Future future = operationService
                    .invokeOnTarget(SERVICE_NAME, new QueryOperation(name, predicate), member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    private List<Future> queryOnPartitions(Predicate predicate, Collection<Integer> partitionIds,
                                           NodeEngine nodeEngine) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return Collections.emptyList();
        }
        final OperationService operationService = nodeEngine.getOperationService();
        final List<Future> futures = new ArrayList<Future>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future f =
                        operationService.invokeOnPartition(SERVICE_NAME, queryPartitionOperation, partitionId);
                futures.add(f);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return futures;
    }


    /**
     * For paging predicates.
     * Adds results to result set and removes returned partition ids.
     */
    private void addPagingPredicateResults(List<Future> futures, Set result, Collection<Integer> partitionIds)
            throws ExecutionException, InterruptedException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {
                List<Integer> tmpPartitionIds = queryResult.getPartitionIds();
                if (tmpPartitionIds != null) {
                    partitionIds.removeAll(tmpPartitionIds);
                    for (QueryResultEntry queryResultEntry : queryResult.getResult()) {
                        Object key = toObject(queryResultEntry.getKeyData());
                        Object value = toObject(queryResultEntry.getValueData());
                        result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                    }
                }
            }
        }
    }

    /**
     * For predicates except paging predicates.
     * Adds results to result set and removes returned partition ids.
     */
    private void addPredicateResults(List<Future> futures, Set result, Collection<Integer> partitionIds)
            throws ExecutionException, InterruptedException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult == null) {
                continue;
            }
            final List<Integer> tmpPartitionIdList = queryResult.getPartitionIds();
            if (tmpPartitionIdList != null) {
                partitionIds.removeAll(tmpPartitionIdList);
                result.addAll(queryResult.getResult());
            }
        }
    }

    private Set<Integer> newSetPopulatedWithPartitionIds(int partitionCount) {
        final Set<Integer> partitionIds = new HashSet<Integer>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionIds.add(i);
        }
        return partitionIds;
    }


    private Object toObject(Object obj) {
        return nodeEngine.getSerializationService().toObject(obj);
    }

    protected Set query(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {

        final NodeEngine nodeEngine = this.nodeEngine;
        OperationService operationService = nodeEngine.getOperationService();
        final SerializationService ss = nodeEngine.getSerializationService();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> plist = new HashSet<Integer>(partitionCount);
        PagingPredicate pagingPredicate = null;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
            pagingPredicate.setIterationType(iterationType);
            if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
                pagingPredicate.previousPage();
                query(pagingPredicate, iterationType, dataResult);
                pagingPredicate.nextPage();
            }
        }
        Set result;
        if (pagingPredicate == null) {
            result = new QueryResultSet(ss, iterationType, dataResult);
        } else {
            result = new SortedQueryResultSet(pagingPredicate.getComparator(), iterationType, pagingPredicate.getPageSize());
        }
        List<Integer> missingList = new ArrayList<Integer>();
        try {
            List<Future> flist = new ArrayList<Future>();
            for (MemberImpl member : members) {
                Future future = operationService
                        .invokeOnTarget(SERVICE_NAME, new QueryOperation(name, predicate), member.getAddress());
                flist.add(future);
            }
            for (Future future : flist) {
                QueryResult queryResult = (QueryResult) future.get();
                if (queryResult != null) {
                    final List<Integer> partitionIds = queryResult.getPartitionIds();
                    if (partitionIds != null) {
                        plist.addAll(partitionIds);
                        if (pagingPredicate == null) {
                            result.addAll(queryResult.getResult());
                        } else {
                            for (QueryResultEntry queryResultEntry : queryResult.getResult()) {
                                Object key = ss.toObject(queryResultEntry.getKeyData());
                                Object value = ss.toObject(queryResultEntry.getValueData());
                                result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                            }
                        }
                    }
                }
            }
            if (plist.size() == partitionCount) {
                if (pagingPredicate != null) {
                    PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
                }
                return result;
            }
            for (int i = 0; i < partitionCount; i++) {
                if (!plist.contains(i)) {
                    missingList.add(i);
                }
            }
        } catch (Throwable t) {
            missingList.clear();
            for (int i = 0; i < partitionCount; i++) {
                if (!plist.contains(i)) {
                    missingList.add(i);
                }
            }
        }

        try {
            List<Future> futures = new ArrayList<Future>(missingList.size());
            for (Integer pid : missingList) {
                QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
                queryPartitionOperation.setPartitionId(pid);
                try {
                    Future f =
                            operationService.invokeOnPartition(SERVICE_NAME, queryPartitionOperation, pid);
                    futures.add(f);
                } catch (Throwable t) {
                    throw ExceptionUtil.rethrow(t);
                }
            }
            for (Future future : futures) {
                QueryResult queryResult = (QueryResult) future.get();
                if (pagingPredicate == null) {
                    result.addAll(queryResult.getResult());
                } else {
                    for (QueryResultEntry queryResultEntry : queryResult.getResult()) {
                        Object key = ss.toObject(queryResultEntry.getKeyData());
                        Object value = ss.toObject(queryResultEntry.getValueData());
                        result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        if (pagingPredicate != null) {
            PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
        }
        return result;
    }


}
