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

package com.hazelcast.map.impl.operation.steps;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.operation.EntryOperator;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.StaticParams;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.map.impl.record.Record.UNSET;

public class State {

    private final RecordStore recordStore;
    private final MapOperation operation;

    // fields coming from operation
    boolean putTransient;
    int partitionId;
    long threadId;
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
    CallerProvenance callerProvenance;

    // fields used in inter thread communication
    private volatile Object oldValue;
    private volatile Object newValue;
    private volatile boolean stopExecution;
    private volatile Object result;
    private volatile boolean nullRecord;
    private volatile Collection<Data> keysToLoad = Collections.emptyList();
    private volatile Map loadedKeyValuePairs = Collections.emptyMap();
    private volatile Collection<Data> keys;
    private volatile ArrayList<Record> records;
    private volatile EntryProcessor entryProcessor;
    private volatile EntryOperator operator;
    private volatile List<State> toStore;
    private volatile List<State> toRemove;
    private volatile Predicate predicate;
    private volatile List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> mergingEntries;
    private volatile SplitBrainMergePolicy<Object,
            SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object> mergePolicy;

    public State(RecordStore recordStore, MapOperation operation) {
        this.recordStore = recordStore;
        this.operation = operation;
    }

    public State(State state) {
        this.recordStore = state.getRecordStore();
        this.operation = state.getOperation();

        setTtl(state.getTtl())
                .setPutTransient(state.isPutTransient())
                .setMaxIdle(state.getMaxIdle())
                .setVersion(state.getVersion())
                .setExpiryTime(state.getExpiryTime())
                .setNow(state.getNow())
                .setStaticPutParams(state.getStaticParams())
                .setOwnerUuid(state.getOwnerUuid())
                .setTxnId(state.getTxnId())
                .setCallerProvenance(state.getCallerProvenance())
                .setEntryProcessor(state.getEntryProcessor())
                .setCallerAddress(state.getCallerAddress())
                .setPartitionId(state.getPartitionId());
    }

    public RecordStore getRecordStore() {
        return recordStore;
    }

    public MapOperation getOperation() {
        return operation;
    }

    public State setVersion(long version) {
        this.version = version;
        return this;
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

    public State setRecordNull(boolean nullRecord) {
        this.nullRecord = nullRecord;
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

    public State setExpiryTime(long expiryTime) {
        this.expiryTime = expiryTime;
        return this;
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

    public State setCallerProvenance(CallerProvenance callerProvenance) {
        this.callerProvenance = callerProvenance;
        return this;
    }

    public State setEntryProcessor(EntryProcessor entryProcessor) {
        this.entryProcessor = entryProcessor;
        return this;
    }

    public State setThreadId(long threadId) {
        this.threadId = threadId;
        return this;
    }

    public long getThreadId() {
        return threadId;
    }

    public EntryProcessor getEntryProcessor() {
        return entryProcessor;
    }

    public boolean isNullRecord() {
        return nullRecord;
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

    public State setNow(long now) {
        this.now = now;
        return this;
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

    public State setOwnerUuid(UUID ownerUuid) {
        this.ownerUuid = ownerUuid;
        return this;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public CallerProvenance getCallerProvenance() {
        return callerProvenance;
    }

    public boolean isStopExecution() {
        return stopExecution;
    }

    public void setStopExecution(boolean stopExecution) {
        this.stopExecution = stopExecution;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Object getResult() {
        return result;
    }

    public State setKeys(Collection<Data> keys) {
        this.keys = keys;
        return this;
    }

    public Collection<Data> getKeys() {
        return keys;
    }

    public void setRecords(ArrayList<Record> records) {
        this.records = records;
    }

    public Collection<Data> getKeysToLoad() {
        return keysToLoad;
    }

    public State setKeysToLoad(Collection<Data> keysToLoad) {
        this.keysToLoad = keysToLoad;
        return this;
    }

    public State setLoadedKeyValuePairs(Map loadedKeyValuePairs) {
        this.loadedKeyValuePairs = loadedKeyValuePairs;
        return this;
    }

    public Map getLoadedKeyValuePairs() {
        return loadedKeyValuePairs;
    }

    public ArrayList<Record> getRecords() {
        return records;
    }

    public State setEntryOperator(EntryOperator operator) {
        this.operator = operator;
        return this;
    }

    public EntryOperator getOperator() {
        return operator;
    }

    public void setToStore(List<State> toStore) {
        this.toStore = toStore;
    }

    public void setToRemove(List<State> toRemove) {
        this.toRemove = toRemove;
    }

    public List<State> getToStore() {
        return toStore;
    }

    public List<State> getToRemove() {
        return toRemove;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public State setPredicate(Predicate predicate) {
        this.predicate = predicate;
        return this;
    }

    public State setMergingEntries(List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> mergingEntries) {
        this.mergingEntries = mergingEntries;
        return this;
    }

    public List<SplitBrainMergeTypes.MapMergeTypes<Object, Object>> getMergingEntries() {
        return mergingEntries;
    }

    public State setMergePolicy(SplitBrainMergePolicy<Object,
            SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object> mergePolicy) {
        this.mergePolicy = mergePolicy;
        return this;
    }

    public SplitBrainMergePolicy<Object,
            SplitBrainMergeTypes.MapMergeTypes<Object, Object>, Object> getMergePolicy() {
        return mergePolicy;
    }
}
