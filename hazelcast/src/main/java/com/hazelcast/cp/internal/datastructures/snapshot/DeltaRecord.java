/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.snapshot;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * An immutable, key-level mutation record produced while applying a committed
 * Raft entry. The record is the unit that the durable incremental snapshot
 * store consumes (Option B/F of the snapshot redesign): it carries the
 * service/object scope, the affected key, the new value (or a tombstone), the
 * purge timestamp when relevant, and the Raft op index the mutation was
 * applied at.
 * <p>
 * The durable snapshot index {@code S} of the incremental store advances to
 * the maximum {@link #opIndex()} of the records of a durably committed batch.
 *
 * @see DeltaSink
 * @see DeltaContext
 */
public final class DeltaRecord {

    /**
     * The kind of mutation a record represents.
     */
    public enum Kind {
        /** A key was associated with a value. */
        PUT,
        /** A key was removed. */
        DELETE,
        /** An entire object (e.g. a destroyed CP map) was removed. */
        REMOVE_ALL
    }

    private final Kind kind;
    private final String serviceName;
    private final String objectName;
    private final Data key;
    private final Data value;
    private final long timestamp;
    private final long opIndex;

    public DeltaRecord(Kind kind, String serviceName, String objectName, Data key,
                       Data value, long timestamp, long opIndex) {
        this.kind = Objects.requireNonNull(kind);
        this.serviceName = Objects.requireNonNull(serviceName);
        this.objectName = Objects.requireNonNull(objectName);
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.opIndex = opIndex;
    }

    public Kind kind() {
        return kind;
    }

    public String serviceName() {
        return serviceName;
    }

    public String objectName() {
        return objectName;
    }

    public Data key() {
        return key;
    }

    public Data value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }

    public long opIndex() {
        return opIndex;
    }

    /**
     * Writes this record to a binary stream (the on-disk format of the
     * incremental snapshot store). Format: kind ordinal, service/object
     * names, then nullable length-prefixed key and value bytes, timestamp,
     * op index.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(kind.ordinal());
        out.writeUTF(serviceName);
        out.writeUTF(objectName);
        writeData(out, key);
        writeData(out, value);
        out.writeLong(timestamp);
        out.writeLong(opIndex);
    }

    /**
     * Reads a record from a binary stream; the mirror of {@link #writeTo}.
     */
    public static DeltaRecord readFrom(DataInput in) throws IOException {
        Kind kind = Kind.values()[in.readInt()];
        String serviceName = in.readUTF();
        String objectName = in.readUTF();
        Data key = readData(in);
        Data value = readData(in);
        long timestamp = in.readLong();
        long opIndex = in.readLong();
        return new DeltaRecord(kind, serviceName, objectName, key, value, timestamp, opIndex);
    }

    private static void writeData(DataOutput out, Data data) throws IOException {
        out.writeBoolean(data != null);
        if (data != null) {
            byte[] bytes = data.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private static Data readData(DataInput in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new HeapData(bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DeltaRecord that)) {
            return false;
        }
        return timestamp == that.timestamp
                && opIndex == that.opIndex
                && kind == that.kind
                && serviceName.equals(that.serviceName)
                && objectName.equals(that.objectName)
                && Objects.equals(key, that.key)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, serviceName, objectName, key, value, timestamp, opIndex);
    }

    @Override
    public String toString() {
        return "DeltaRecord{" + kind + ", service=" + serviceName
                + ", object=" + objectName
                + ", key=" + key
                + ", value=" + (value == null ? "null" : "not null")
                + ", timestamp=" + timestamp
                + ", opIndex=" + opIndex + '}';
    }
}
