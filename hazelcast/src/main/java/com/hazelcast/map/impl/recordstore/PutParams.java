/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;

import java.util.UUID;

import static com.hazelcast.map.impl.record.Record.UNSET;

public class PutParams {

    public static final PutParams PUT_PARAMS = new PutParams()
            .setExpiryTime(UNSET)
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setExpectedValue(null)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setTransactionId(null)
            .setCallerAddress(null)
            .setCountAsAccess(true)
            .setBackup(false);

    public static final PutParams PUT_IF_ABSENT_PARAMS = new PutParams()
            .setExpiryTime(UNSET)
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(true)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setExpectedValue(null)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(false);


    public static final PutParams SET_PARAMS = new PutParams()
            .setExpiryTime(UNSET)
            .setLoad(false)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setExpectedValue(null)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setTransactionId(null)
            .setCallerAddress(null)
            .setCountAsAccess(true)
            .setBackup(false);

    public static final PutParams PUT_TRANSIENT_PARAMS = new PutParams()
            .setExpiryTime(UNSET)
            .setLoad(false)
            .setStore(false)
            .setPutIfAbsent(false)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setExpectedValue(null)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setTransactionId(null)
            .setCallerAddress(null)
            .setCountAsAccess(true)
            .setBackup(false);


    public static final PutParams REPLACE_PARAMS = new PutParams()
            .setExpiryTime(UNSET)
            .setTtl(UNSET)
            .setMaxIdle(UNSET)
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(true)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setExpectedValue(null)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setTransactionId(null)
            .setCallerAddress(null)
            .setCountAsAccess(true)
            .setBackup(true);

    public static final PutParams REPLACE_IF_SAME_PARAMS = new PutParams()
            .setExpiryTime(UNSET)
            .setTtl(UNSET)
            .setMaxIdle(UNSET)
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(true)
            .setPutIfEqual(true)
            .setPutFromLoad(false)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setTransactionId(null)
            .setCallerAddress(null)
            .setCountAsAccess(true)
            .setBackup(false);



    // params
    private Data key;
    private Object newValue;
    private long ttl;
    private long maxIdle;
    private long expiryTime;
    private long now;
    private boolean load;
    private boolean store;
    private boolean putIfAbsent;
    private boolean putIfExists;
    private boolean putIfEqual;
    private boolean putFromLoad;
    private Object expectedValue;
    private boolean setTtl;
    private boolean checkIfLoaded;
    private UUID transactionId;
    private Address callerAddress;
    private boolean countAsAccess;
    private boolean backup;

    public PutParams setKey(Data key) {
        this.key = key;
        return this;
    }

    public PutParams setNewValue(Object newValue) {
        this.newValue = newValue;
        return this;
    }

    public PutParams setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public PutParams setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    public PutParams setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
        return this;
    }

    public PutParams setNow(long now) {
        this.now = now;
        return this;
    }

    public PutParams setLoad(boolean load) {
        this.load = load;
        return this;
    }

    public PutParams setStore(boolean store) {
        this.store = store;
        return this;
    }

    public PutParams setPutIfAbsent(boolean putIfAbsent) {
        this.putIfAbsent = putIfAbsent;
        return this;
    }

    public PutParams setPutIfExists(boolean putIfExists) {
        this.putIfExists = putIfExists;
        return this;
    }

    public PutParams setPutIfEqual(boolean putIfEqual) {
        this.putIfEqual = putIfEqual;
        return this;
    }

    public PutParams setPutFromLoad(boolean putFromLoad) {
        this.putFromLoad = putFromLoad;
        return this;
    }

    public PutParams setExpectedValue(Object expectedValue) {
        this.expectedValue = expectedValue;
        return this;
    }

    public PutParams setSetTtl(boolean setTtl) {
        this.setTtl = setTtl;
        return this;
    }

    public PutParams setCheckIfLoaded(boolean checkIfLoaded) {
        this.checkIfLoaded = checkIfLoaded;
        return this;
    }

    public PutParams setTransactionId(UUID transactionId) {
        this.transactionId = transactionId;
        return this;
    }

    public PutParams setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public PutParams setCountAsAccess(boolean countAsAccess) {
        this.countAsAccess = countAsAccess;
        return this;
    }

    public PutParams setBackup(boolean backup) {
        this.backup = backup;
        return this;
    }

    public Data getKey() {
        return key;
    }

    public Object getNewValue() {
        return newValue;
    }

    public long getTtl() {
        return ttl;
    }

    public long getMaxIdle() {
        return maxIdle;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    public long getNow() {
        return now;
    }

    public boolean isLoad() {
        return load;
    }

    public boolean isStore() {
        return store;
    }

    public boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    public boolean isPutIfExists() {
        return putIfExists;
    }

    public boolean isPutIfEqual() {
        return putIfEqual;
    }

    public boolean isPutFromLoad() {
        return putFromLoad;
    }

    public Object getExpectedValue() {
        return expectedValue;
    }

    public boolean isSetTtl() {
        return setTtl;
    }

    public boolean isCheckIfLoaded() {
        return checkIfLoaded;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public Address getCallerAddress() {
        return callerAddress;
    }

    public boolean isCountAsAccess() {
        return countAsAccess;
    }

    public boolean isBackup() {
        return backup;
    }
}
