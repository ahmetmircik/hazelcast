package com.craftedbytes.client;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * Created by dbrimley on 06/08/2014.
 */
public class Server {

    public static void main(String args[]){
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
    }

}
