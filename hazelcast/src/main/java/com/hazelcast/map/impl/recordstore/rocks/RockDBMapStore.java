package com.hazelcast.map.impl.recordstore.rocks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.AccessUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class RockDBMapStore implements MapStore<Object, Object>, MapLoaderLifecycleSupport {

    protected static final ILogger LOGGER = Logger.getLogger(RockDBMapStore.class);
    private static final String NAME = "experiment";

    private Supplier<RocksDB> dbSupplier = () -> createDB();
    private ConcurrentMemoizingSupplier<RocksDB> concurrentMemoizingSupplier
            = new ConcurrentMemoizingSupplier<>(dbSupplier);

    private volatile InternalSerializationService ss;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        this.ss = AccessUtil.getHazelcastInstanceImpl(hazelcastInstance)
                .getSerializationService();
    }

    private RocksDB getRocksDB() {
        return concurrentMemoizingSupplier.get();
    }

    @Override
    public void destroy() {

    }

    private RocksDB createDB() {
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);

        File parent = new File("ahmet/rocks-db");
        File dbDir = new File(parent, NAME);
        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
            Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            RocksDB rocksDB = RocksDB.open(options, dbDir.getAbsolutePath());
            LOGGER.info("RocksDB initialized and ready to use");
            return rocksDB;
        } catch (IOException | RocksDBException ex) {
            LOGGER.severe(String.format("Error initializing RocksDB, check configurations and permissions, "
                            + "exception: %s, message: %s, stackTrace: %s",
                    ex.getCause(), ex.getMessage(), ex.getStackTrace()));
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Object load(Object key) {
        try {
            byte[] bytes = ss.toBytes(key);
            return getRocksDB().get(bytes);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map loadAll(Collection<Object> keys) {
        HashMap result = new HashMap();

        ArrayList<byte[]> byteKeys = new ArrayList<>();
        for (Object key : keys) {
            byteKeys.add(ss.toBytes(key));
        }
        try {
            List<byte[]> valueList = getRocksDB().multiGetAsList(byteKeys);

            for (int i = 0; i < byteKeys.size(); i++) {
                byte[] key = byteKeys.get(i);
                Object value = valueList.get(i);

                if (value != null) {
                    result.put(ss.toObject(key), ss.toObject(value));
                }
            }

        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    @Override
    public Iterable loadAllKeys() {
        return new Iterable() {
            RocksIterator rocksIterator = getRocksDB().newIterator();
            {
                rocksIterator.seekToFirst();
            }

            @Override
            public Iterator iterator() {
                return new Iterator() {
                    @Override
                    public boolean hasNext() {
                        return rocksIterator.isValid();
                    }

                    @Override
                    public Object next() {
                        byte[] key = rocksIterator.key();
                        rocksIterator.next();
                        return ss.toObject(key);
                    }
                };
            }
        };
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        WriteBatch batch = new WriteBatch();
        try {
            for (Object key : keys) {
                batch.delete(ss.toBytes(key));
            }

            WriteOptions options = new WriteOptions();
            getRocksDB().write(options, batch);

        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void store(Object key, Object value) {
        try {
            getRocksDB().put(ss.toBytes(key), ss.toBytes(value));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        WriteBatch batch = new WriteBatch();
        try {
            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                batch.put(ss.toBytes(entry.getKey()), ss.toBytes(entry.getValue()));
            }

            WriteOptions options = new WriteOptions();
            getRocksDB().write(options, batch);

        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Object key) {
        try {
            getRocksDB().delete(ss.toBytes(key));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}
