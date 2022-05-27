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

package com.hazelcast.map.impl.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.UUID;

import static com.hazelcast.map.impl.record.Record.UNSET;

public class State {

    protected volatile Object oldValue;
    protected volatile Object newValue;

    private final RecordStore recordStore;
    private final Operation operation;

    // params
    boolean putTransient;
    long ttl = UNSET;
    long maxIdle = UNSET;
    long version;
    long expiryTime;
    long now = Clock.currentTimeMillis();
    Data key;
    Address callerAddress;
    Object expect;
    StaticParams staticParams;
    UUID txnId;
    UUID ownerUuid;

    int partitionId;
    private volatile boolean stopExecution;


    public State(RecordStore recordStore, Operation operation) {
        this.recordStore = recordStore;
        this.operation = operation;
    }

    public RecordStore getRecordStore() {
        return recordStore;
    }

    public Operation getOperation() {
        return operation;
    }

    public State setPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public State setKey(Data key) {
        this.key = key;
        return this;
    }

    public State setNewValue(Object newValue) {
        this.newValue = newValue;
        return this;
    }

    public State setOldValue(Object oldValue) {
        this.oldValue = oldValue;
        return this;
    }

    public State setExpect(Object expect) {
        this.expect = expect;
        return this;
    }

    public State setTxnId(UUID uuid) {
        this.txnId = uuid;
        return this;
    }

    public State ownerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
        return this;
    }

    public State version(long version) {
        this.version = version;
        return this;
    }

    public State setTtl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    public State setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
        return this;
    }

    public void setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
    }

    public State setPutTransient(boolean putTransient) {
        this.putTransient = putTransient;
        return this;
    }

    public State setStaticPutParams(StaticParams staticStaticParams) {
        this.staticParams = staticStaticParams;
        return this;
    }

    public State setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public Object getOldValue() {
        return oldValue;
    }

    public Object getNewValue() {
        return newValue;
    }

    public boolean isPutTransient() {
        return putTransient;
    }

    public long getTtl() {
        return ttl;
    }

    public long getMaxIdle() {
        return maxIdle;
    }

    public long getVersion() {
        return version;
    }

    public long getExpiryTime() {
        return expiryTime;
    }

    public long getNow() {
        return now;
    }

    public Data getKey() {
        return key;
    }

    public Address getCallerAddress() {
        return callerAddress;
    }

    public Object getExpect() {
        return expect;
    }

    public StaticParams getStaticParams() {
        return staticParams;
    }

    public UUID getTxnId() {
        return txnId;
    }

    public UUID getOwnerUuid() {
        return ownerUuid;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isStopExecution() {
        return stopExecution;
    }

    public void setStopExecution(boolean stopExecution) {
        this.stopExecution = stopExecution;
    }
}
