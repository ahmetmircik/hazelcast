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

package com.hazelcast.cp.internal.datastructures.snapshot.v2;

import com.hazelcast.cp.internal.datastructures.snapshot.v2.codecs.CPMapSnapshotCodec;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.DeltaEntry;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.ServiceWorkspace;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSink;
import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.SnapshotFrameSource;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPMapSnapshotCodecTest {

    private static final byte[] VALID_KEY_1 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    private static final byte[] VALID_VAL_1 = new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    private static final byte[] VALID_VAL_2 = new byte[]{21, 22, 23, 24, 25, 26, 27, 28, 29, 30};
    private static final byte[] VALID_OBJ_ID = new byte[]{31, 32, 33, 34, 35, 36, 37, 38, 39, 40};

    @Test
    public void testMaterializeBaseWithLastWriteWins() {
        CPMapSnapshotCodec codec = new CPMapSnapshotCodec();
        Map<String, Map<Data, Data>> emptyBase = codec.createEmptyBase();

        Data objId = new HeapData(VALID_OBJ_ID);
        Data key1 = new HeapData(VALID_KEY_1);
        Data val1 = new HeapData(VALID_VAL_1);
        Data val2 = new HeapData(VALID_VAL_2);

        DeltaEntry d1 = new DeltaEntry(codec.serviceId(), objId, 1L, DeltaEntry.OP_PUT, key1, val1, 1L, 0);
        DeltaEntry d2 = new DeltaEntry(codec.serviceId(), objId, 1L, DeltaEntry.OP_PUT, key1, val2, 2L, 0);

        Map<String, Map<Data, Data>> base = codec.materializeBase(emptyBase, List.of(d1, d2).iterator());
        assertNotNull(base);
        assertEquals(1, base.size());

        Map<Data, Data> map = base.values().iterator().next();
        assertEquals(val2, map.get(key1));

        DeltaEntry d3 = new DeltaEntry(codec.serviceId(), objId, 1L, DeltaEntry.OP_REMOVE, key1, null, 3L, 0);
        Map<String, Map<Data, Data>> baseAfterRemove = codec.materializeBase(base, List.of(d3).iterator());
        Map<Data, Data> mapAfterRemove = baseAfterRemove.values().iterator().next();
        assertNull(mapAfterRemove.get(key1));
    }

    @Test
    public void testStreamingFrameRoundTrip() throws IOException {
        CPMapSnapshotCodec codec = new CPMapSnapshotCodec();

        Data objId = new HeapData(VALID_OBJ_ID);
        Data key = new HeapData(VALID_KEY_1);
        Data val = new HeapData(VALID_VAL_1);

        DeltaEntry d = new DeltaEntry(codec.serviceId(), objId, 1L, DeltaEntry.OP_PUT, key, val, 1L, 0);
        Map<String, Map<Data, Data>> base = codec.materializeBase(codec.createEmptyBase(), List.of(d).iterator());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SnapshotFrameSink sink = new SnapshotFrameSink() {
            private long written;
            @Override
            public void writeChunk(byte[] buffer, int offset, int length) throws IOException {
                baos.write(buffer, offset, length);
                written += length;
            }
            @Override
            public long totalBytesWritten() {
                return written;
            }
        };

        codec.writeBase(base, sink);

        byte[] serializedData = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);

        SnapshotFrameSource source = new SnapshotFrameSource() {
            @Override
            public int readChunk(byte[] buffer, int offset, int length) throws IOException {
                return bais.read(buffer, offset, length);
            }
            @Override
            public boolean hasMoreChunks() {
                return bais.available() > 0;
            }
        };

        ServiceWorkspace workspace = new ServiceWorkspace(codec.serviceId(), 1024 * 1024);
        Map<String, Map<Data, Data>> restored = codec.readBase(source, serializedData.length, workspace);

        assertNotNull(restored);
        assertEquals(base.size(), restored.size());
        assertEquals(val, restored.values().iterator().next().get(key));
    }
}
