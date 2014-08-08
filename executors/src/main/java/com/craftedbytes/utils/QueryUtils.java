package com.craftedbytes.utils;

import com.craftedbytes.domain.QuickSearch;
import com.craftedbytes.tasks.CollectTask;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.query.Predicate;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class QueryUtils {

    private static Logger logger = Logger.getLogger(QueryUtils.class);

    public static double collectTaskWithPredicateTesting(IExecutorService executor, IMap<String, QuickSearch> qs_map,
                                                         Predicate predicate)
            throws InterruptedException, ExecutionException {

        StopWatch clock = new StopWatch();
        clock.reset();
        clock.start();
        CollectTask ct3 = new CollectTask(predicate);

        Map<Member, Future<List<Long>>> result3 = executor.submitToAllMembers(ct3);

        Set<Long> result = new HashSet<Long>();
        for (Future<List<Long>> future : result3.values()) {
            List<Long> longs = future.get();
            logger.info("resultset size = " + longs.size());
            result.addAll(longs);
        }

        clock.stop();
        return clock.getTime();
    }

    public static double collectTaskWithSQL(IMap<String, QuickSearch> qs_map, Predicate predicate)
            throws InterruptedException, ExecutionException {

        StopWatch clock = new StopWatch();
        clock.reset();
        clock.start();

        Set<String> values = qs_map.keySet(predicate);

        logger.info("resultset size = " + values.size());

        clock.stop();
        return clock.getTime();

    }
}

