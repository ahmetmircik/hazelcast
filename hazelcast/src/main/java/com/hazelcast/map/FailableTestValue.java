package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * A test value which can be configured to fail during serialize or deserialize.
 */
public class FailableTestValue implements DataSerializable {

    private String value;

    private boolean failInSerialize;

    private boolean failInDeserialize;

    /*
     * protected constructor for deserialization
     */
    FailableTestValue() {
    }

    public FailableTestValue(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setFailInSerialize(boolean failInSerialize) {
        this.failInSerialize = failInSerialize;
    }

    public void setFailInDeserialize(boolean failInDeserialize) {
        this.failInDeserialize = failInDeserialize;
    }

    // ---------------------------------------------------------- serialization

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (failInSerialize) {
            throw new IOException("Intended failure during serialize for '" + value + "'.");
        }
        out.writeUTF(value);
        out.writeBoolean(failInSerialize);
        out.writeBoolean(failInDeserialize);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readUTF();
        failInSerialize = in.readBoolean();
        failInDeserialize = in.readBoolean();
        if (failInDeserialize) {
            throw new IOException("Intended failure during deserialize for '" + value + "'.");
        }
    }

    // ------------------------------------------------------- Object overrides

    @Override
    public String toString() {
        return "[" + (failInSerialize ? "-" : "+") + "," + (failInDeserialize ? "-" : "+") + "] " + value;
    }

}