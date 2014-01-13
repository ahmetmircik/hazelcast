package com.hazelcast.disk.core;

/**
 * @author: ahmetmircik
 * Date: 1/14/14
 */
public class NotEnoughSpaceException extends Error {

    public NotEnoughSpaceException() {
    }

    public NotEnoughSpaceException(String message) {
        super(message);
    }
}
