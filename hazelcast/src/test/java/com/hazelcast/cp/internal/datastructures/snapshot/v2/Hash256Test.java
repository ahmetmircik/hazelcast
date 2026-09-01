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

import com.hazelcast.cp.internal.datastructures.snapshot.v2.core.Hash256;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Hash256Test {

    @Test
    public void testImmutabilityAndDefensiveCloning() {
        byte[] original = new byte[32];
        Arrays.fill(original, (byte) 0xAB);

        Hash256 hash = new Hash256(original);
        original[0] = 0x12;

        assertFalse("Hash256 must clone input defensively", hash.bytes()[0] == 0x12);
        assertEquals((byte) 0xAB, hash.bytes()[0]);

        byte[] exported = hash.bytes();
        exported[0] = 0x34;
        assertEquals((byte) 0xAB, hash.bytes()[0]);
    }

    @Test
    public void testConstantTimeEquality() {
        byte[] b1 = new byte[32];
        byte[] b2 = new byte[32];
        Arrays.fill(b1, (byte) 0x42);
        Arrays.fill(b2, (byte) 0x42);

        Hash256 h1 = new Hash256(b1);
        Hash256 h2 = new Hash256(b2);

        assertTrue(h1.constantTimeEquals(h2));
        assertEquals(h1, h2);
        assertEquals(h1.hashCode(), h2.hashCode());

        b2[31] = (byte) 0x99;
        Hash256 h3 = new Hash256(b2);
        assertFalse(h1.constantTimeEquals(h3));
        assertFalse(h1.equals(h3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLengthRejected() {
        new Hash256(new byte[16]);
    }
}
