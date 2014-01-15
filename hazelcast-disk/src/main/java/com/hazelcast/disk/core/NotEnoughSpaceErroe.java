package com.hazelcast.disk.core;

/**
 * @author: ahmetmircik
 * Date: 1/14/14
 */
public class NotEnoughSpaceErroe extends Error {

    public NotEnoughSpaceErroe() {
    }

    public NotEnoughSpaceErroe(String message) {
        super(message);
    }
}
