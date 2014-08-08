package com.craftedbytes.client;

import com.craftedbytes.domain.QuickSearch;
import com.craftedbytes.utils.PredicateUtil;
import com.craftedbytes.utils.QueryUtils;
import com.craftedbytes.utils.QuickSearchFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Client
{

    private static IMap<String, Set<Long>> accessMap;
    private static IMap<String, QuickSearch> qs_map;

    public static void main( String[] args )
            throws IOException, ExecutionException, InterruptedException {

        ClientConfig clientConfig = new XmlClientConfigBuilder("hazelcast-client.xml").build();
        HazelcastInstance hazelcastClient = HazelcastClient.newHazelcastClient(clientConfig);
        initMaps(hazelcastClient);
        populateWithFactory(60000);
        runExecutorBasedQuery(hazelcastClient.getExecutorService("exec1"),10);
        runIMapSQLBasedQuery(10);

    }

    private static void runIMapSQLBasedQuery(int queries) throws InterruptedException, ExecutionException {
        double[] total = new double[10];
        for(int i =  0; i < queries; i++) {
            double res = QueryUtils.collectTaskWithSQL(qs_map, PredicateUtil.mediumComplexQuery());
            total[i] = res;
        }
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for(int i = 0; i < total.length; i++) {
            stats.addValue(total[i]);
        }
        System.out.println("Query avg time of 10 queries was : " + stats.getMean() + " ms\n");
        System.out.println("Max time of 10 queries was : " + stats.getMax() + " ms\n");
        System.out.println("Query variance of 10 queries was : " + stats.getVariance() + "\n");
        System.out.println("Query STD of 10 queries was: " + stats.getStandardDeviation() +  "\n");
    }

    private static void runExecutorBasedQuery(IExecutorService ex,
                                              int queries) throws InterruptedException, ExecutionException {
        double[] total = new double[10];
        for(int i =  0; i < queries; i++) {
            double res = QueryUtils.collectTaskWithPredicateTesting(ex, qs_map, PredicateUtil.mediumComplexQuery());
            total[i] = res;
        }
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for(int i = 0; i < total.length; i++) {
            stats.addValue(total[i]);
        }
        System.out.println("Query avg time of 10 queries was : " + stats.getMean() + " ms\n");
        System.out.println("Max time of 10 queries was : " + stats.getMax() + " ms\n");
        System.out.println("Query variance of 10 queries was : " + stats.getVariance() + "\n");
        System.out.println("Query STD of 10 queries was: " + stats.getStandardDeviation() +  "\n");
    }

    private static void initMaps(HazelcastInstance client) {
        qs_map = client.getMap("quicksearch");
        accessMap = client.getMap("access");
    }

    private static void populateWithFactory(int limit) {
        QuickSearchFactory qs = new QuickSearchFactory();
        Collection<QuickSearch> q = qs.createQuickSearchList(limit);
        int count = 0;
        for(QuickSearch o : q) {
            if(count % 1000 == 0){
                System.out.println(count);
            }
            qs_map.put(o.getQuickSearchKey(), o);
            count++;
        }
        System.out.println("Map has " + qs_map.size());
    }
}
