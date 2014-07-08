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
            addResultsOfPagingPredicate(singletonList, result, partitionIds);
            if (partitionIds.isEmpty()) {
                PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate,
                        ((SortedQueryResultSet) result).last());
                return result;
            }

            final List<Future> futures = queryOnPartitions(pagingPredicate, partitionIds, nodeEngine);
            addResultsOfPagingPredicate(futures, result, partitionIds);

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
            addResultsOfPredicate(singletonList, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
            List<Future> futures = queryOnPartitions(predicate, partitionIds, nodeEngine);
            addResultsOfPredicate(futures, result, partitionIds);
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
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> partitionIds = createSetPopulatedWithPartitionIds(partitionCount);
        pagingPredicate.setIterationType(iterationType);
        if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
            pagingPredicate.previousPage();
            queryWithPagingPredicate(pagingPredicate, iterationType);
            pagingPredicate.nextPage();
        }
        final Set result = new SortedQueryResultSet(pagingPredicate.getComparator(),
                iterationType, pagingPredicate.getPageSize());
        try {
            final List<Future> futures = queryOnMembers(pagingPredicate, nodeEngine);
            addResultsOfPagingPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
                return result;
            }
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Exception while querying ", t);
        }

        try {
            final List<Future> futures = queryOnPartitions(pagingPredicate, partitionIds, nodeEngine);
            addResultsOfPagingPredicate(futures, result, partitionIds);
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
    public Set query(final Predicate predicate,
                      final IterationType iterationType, final boolean dataResult) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final SerializationService serializationService = nodeEngine.getSerializationService();
        final int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        final Set<Integer> partitionIds = createSetPopulatedWithPartitionIds(partitionCount);
        final Set result = new QueryResultSet(serializationService, iterationType, dataResult);
        List<Future> futures = queryOnMembers(predicate, nodeEngine);
        addResultsOfPredicate(futures, result, partitionIds);
        if (partitionIds.isEmpty()) {
            return result;
        }

        futures = queryOnPartitions(predicate, partitionIds, nodeEngine);
        addResultsOfPredicate(futures, result, partitionIds);
        return result;
    }

    protected Set query2(final Predicate predicate, final IterationType iterationType, final boolean dataResult) {
        final NodeEngine nodeEngine = this.nodeEngine;
        final SerializationService ss = nodeEngine.getSerializationService();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> plist = createSetPopulatedWithPartitionIds(partitionCount);
        Set result = new QueryResultSet(ss, iterationType, dataResult);
        try {
            List<Future> futures = queryOnMembers(predicate, nodeEngine);
            addResultsOfPredicate(futures, result, plist);
            if (plist.isEmpty()) {
                return result;
            }

        } catch (Throwable t) {
        }

        final List<Future> futures = new ArrayList<Future>(plist.size());
        try {
            queryOnPartitions(predicate, plist, nodeEngine);
            addResultsOfPredicate(futures, result, plist);
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

    private List<Future> queryOnMembers(Predicate predicate, NodeEngine nodeEngine) {
        final Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
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
     * Adds results to result set and removes queried partition ids.
     */
    private void addResultsOfPagingPredicate(List<Future> futures, Set result, Collection<Integer> partitionIds) {
        for (Future future : futures) {
            final QueryResult queryResult = getQueryResultOrNull(future);
            if (queryResult == null) {
                continue;
            }
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

    /**
     * For predicates except paging predicates.
     * Adds results to result set and removes queried partition ids.
     */
    private void addResultsOfPredicate(List<Future> futures, Set result, Collection<Integer> partitionIds) {
        for (Future future : futures) {
            final QueryResult queryResult = getQueryResultOrNull(future);
            if (queryResult == null) {
                continue;
            }
            final List<Integer> queriedPartitionIds = queryResult.getPartitionIds();
            if (queriedPartitionIds != null) {
                partitionIds.removeAll(queriedPartitionIds);
                result.addAll(queryResult.getResult());
            }
        }
    }

    private List<Integer> addResultsOfPredicate2(List<Future> futures, Set result) {
        final List<Integer> partitionIds = new ArrayList<Integer>();
        for (Future future : futures) {
            final QueryResult queryResult = getQueryResultOrNull(future);
            if (queryResult == null) {
                continue;
            }
            final List<Integer> tmpPartitionIds = queryResult.getPartitionIds();
            if (tmpPartitionIds != null) {
                partitionIds.addAll(tmpPartitionIds);
                result.addAll(queryResult.getResult());
            }
        }
        return partitionIds;
    }

    private QueryResult getQueryResultOrNull(Future future) {
        QueryResult queryResult = null;
        try {
            queryResult = (QueryResult) future.get();
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Could not get result", t);
        }
        return queryResult;
    }

    private Set<Integer> createSetPopulatedWithPartitionIds(int partitionCount) {
        final Set<Integer> partitionIds = new HashSet<Integer>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionIds.add(i);
        }
        return partitionIds;
    }


    private Object toObject(Object obj) {
        return nodeEngine.getSerializationService().toObject(obj);
    }


}
