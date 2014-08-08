package com.craftedbytes.tasks;

import com.craftedbytes.domain.QuickSearch;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import org.apache.commons.lang3.time.StopWatch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class CollectTask
        implements Callable<List<Long>>, Serializable, HazelcastInstanceAware {

    private static final long serialVersionUID = 1L;
    private transient HazelcastInstance hz;
    private Predicate predicate;
    private PagingPredicate paging;
    private boolean isSql = false;
    private String sql = "";
    private boolean isPaging = false;

    private CollectTask(){}

    public CollectTask(Predicate predicate){
        this.predicate = predicate;
    }

    public CollectTask(PagingPredicate page){
        this.paging = page;
        isPaging = true;
    }

    public CollectTask(String sql){
        this.sql = sql;
        isSql = true;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }

    @Override
    public List<Long> call() throws Exception {
        List<Long> copy_codes = new ArrayList<Long>();
        IMap<String, QuickSearch> map = hz.getMap("quicksearch");

        if(isSql){
            SqlPredicate sqlpredicate = new SqlPredicate();
            for(String key : map.localKeySet(sqlpredicate)) {
                copy_codes.add(map.get(key).getCopy_code_id());
            }
        } else if(isPaging){
            System.out.println("Paging!" + paging.getPageSize());
            paging.nextPage();
            for(String key : map.localKeySet(paging)) {
                copy_codes.add(map.get(key).getCopy_code_id());
            }
        } else {
            for(String key : map.localKeySet(predicate)) {
                copy_codes.add(map.get(key).getCopy_code_id());
            }
        }
        return copy_codes;
    }
}

