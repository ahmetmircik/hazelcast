package com.craftedbytes.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by dbrimley on 06/08/2014.
 */
public class Server {

    public static void main(String args[]) {
        final Config config = new Config();
        config.getGroupConfig().setName("ahmet");
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
    }

}
