package com.hazelcast.disk;

import com.hazelcast.nio.serialization.Data;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * @author: ahmetmircik
 * Date: 12/20/13
 */
public abstract class PersistencyUnit implements Closeable {

    public abstract Data put(Data key, Data value);

    public abstract Data get(Data key);

    public abstract Data remove(Data key);

    public abstract void flush();

    public abstract long size();

    //todo make generic
    public abstract List loadAll() throws IOException;
}
