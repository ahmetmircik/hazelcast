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

package com.hazelcast.cp.internal.datastructures.snapshot.v2.codecs;

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.CPServiceSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ServiceWorkspace;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSink;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSource;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Streaming snapshot codec implementation for CPMap service with LWW delta collapsing.
 */
public final class CPMapSnapshotCodec implements CPServiceSnapshotCodec<Map<String, Map<Data, Data>>, DeltaEntry> {

    public static final short SERVICE_ID = 100;
    public static final short CODEC_VERSION = 1;
    public static final double WORKSPACE_MULTIPLIER = 1.5;
    private static final int MIN_HEADER_BYTES = 4;
    private static final int HEAP_DATA_HEADER_BYTES = 8;
    private static final int MIN_STRING_PAYLOAD_OFFSET = 12;



    @Override
    public short serviceId() {
        return SERVICE_ID;
    }

    @Override
    public short codecVersion() {
        return CODEC_VERSION;
    }

    @Override
    public double conservativeWorkspaceMultiplier() {
        return WORKSPACE_MULTIPLIER;
    }

    @Override
    public long maxRestoreWorkspaceBytes(long declaredBytes) {
        return (long) (declaredBytes * conservativeWorkspaceMultiplier());
    }

    @Override
    public Map<String, Map<Data, Data>> createEmptyBase() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, Map<Data, Data>> materializeBase(
            Map<String, Map<Data, Data>> existingBase, Iterator<DeltaEntry> deltas) {
        Map<String, Map<Data, Data>> newBase = new HashMap<>();
        for (Map.Entry<String, Map<Data, Data>> entry : existingBase.entrySet()) {
            newBase.put(entry.getKey(), new HashMap<>(entry.getValue()));
        }

        List<DeltaEntry> orderedDeltas = new ArrayList<>();
        while (deltas.hasNext()) {
            DeltaEntry d = deltas.next();
            if (d.serviceId() == SERVICE_ID) {
                orderedDeltas.add(d);
            }
        }
        orderedDeltas.sort(DeltaEntry.RAFT_ORDER);

        for (DeltaEntry d : orderedDeltas) {
            String mapName = getMapName(d.objectId());
            Map<Data, Data> map = newBase.computeIfAbsent(mapName, k -> new HashMap<>());

            if (d.opCode() == DeltaEntry.OP_PUT) {
                map.put(d.key(), d.value());
            } else if (d.opCode() == DeltaEntry.OP_REMOVE) {
                map.remove(d.key());
            }
        }

        Map<String, Map<Data, Data>> result = new HashMap<>();
        for (Map.Entry<String, Map<Data, Data>> entry : newBase.entrySet()) {
            result.put(entry.getKey(), Collections.unmodifiableMap(entry.getValue()));
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void applyDeltaToLive(Object liveContainer, DeltaEntry delta) {
        if (liveContainer instanceof Map) {
            Map<String, Map<Data, Data>> live = (Map<String, Map<Data, Data>>) liveContainer;
            String mapName = getMapName(delta.objectId());
            Map<Data, Data> map = live.computeIfAbsent(mapName, k -> new HashMap<>());
            if (delta.opCode() == DeltaEntry.OP_PUT) {
                map.put(delta.key(), delta.value());
            } else if (delta.opCode() == DeltaEntry.OP_REMOVE) {
                map.remove(delta.key());
            }
        }
    }

    @Override
    public Object swapLiveContainer(Object currentContainer, Object newContainer) {
        return newContainer;
    }

    @Override
    public void writeBase(Map<String, Map<Data, Data>> base, SnapshotFrameSink sink) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        Map<String, Map<Data, Data>> sortedMaps = new TreeMap<>(base);
        dos.writeInt(sortedMaps.size());

        for (Map.Entry<String, Map<Data, Data>> mapEntry : sortedMaps.entrySet()) {
            dos.writeUTF(mapEntry.getKey());
            Map<Data, Data> entries = mapEntry.getValue();
            dos.writeInt(entries.size());

            for (Map.Entry<Data, Data> kv : entries.entrySet()) {
                byte[] keyBytes = kv.getKey().toByteArray();
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                byte[] valBytes = kv.getValue().toByteArray();
                dos.writeInt(valBytes.length);
                dos.write(valBytes);
            }
        }
        dos.flush();

        byte[] allBytes = baos.toByteArray();
        int offset = 0;
        int remaining = allBytes.length;
        while (remaining > 0) {
            int toWrite = Math.min(remaining, SnapshotFrameSink.CHUNK_SIZE_BYTES);
            sink.writeChunk(allBytes, offset, toWrite);
            offset += toWrite;
            remaining -= toWrite;
        }
    }

    @Override
    public Map<String, Map<Data, Data>> readBase(
            SnapshotFrameSource source, long declaredBytes, ServiceWorkspace workspace) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] chunkBuf = new byte[SnapshotFrameSink.CHUNK_SIZE_BYTES];

        while (source.hasMoreChunks()) {
            int read = source.readChunk(chunkBuf, 0, chunkBuf.length);
            if (read > 0) {
                workspace.trackAllocation(read);
                baos.write(chunkBuf, 0, read);
            }
        }

        byte[] fullStream = baos.toByteArray();
        if (fullStream.length < MIN_HEADER_BYTES) {
            return Collections.emptyMap();
        }



        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(fullStream));
        int mapCount = dis.readInt();
        Map<String, Map<Data, Data>> restored = new HashMap<>();

        for (int i = 0; i < mapCount; i++) {
            String mapName = dis.readUTF();
            int entryCount = dis.readInt();
            Map<Data, Data> mapEntries = new HashMap<>(entryCount);

            for (int j = 0; j < entryCount; j++) {
                int keyLen = dis.readInt();
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                Data key = new HeapData(keyBytes);

                int valLen = dis.readInt();
                byte[] valBytes = new byte[valLen];
                dis.readFully(valBytes);
                Data val = new HeapData(valBytes);

                mapEntries.put(key, val);
            }
            restored.put(mapName, Collections.unmodifiableMap(mapEntries));
        }

        return Collections.unmodifiableMap(restored);
    }

    private String getMapName(Data objectId) {
        if (objectId == null) {
            return "default";
        }
        byte[] bytes = objectId.toByteArray();
        if (bytes.length > HEAP_DATA_HEADER_BYTES) {
            try {
                java.io.DataInputStream dis = new java.io.DataInputStream(
                        new java.io.ByteArrayInputStream(bytes, HEAP_DATA_HEADER_BYTES,
                                bytes.length - HEAP_DATA_HEADER_BYTES));
                int strLen = dis.readInt();
                if (strLen > 0 && strLen <= bytes.length - MIN_STRING_PAYLOAD_OFFSET) {
                    byte[] strBytes = new byte[strLen];
                    dis.readFully(strBytes);
                    return new String(strBytes, java.nio.charset.StandardCharsets.UTF_8);
                }
            } catch (Exception ignored) {
                // Fall back to raw string decoding
            }
            return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }


}
