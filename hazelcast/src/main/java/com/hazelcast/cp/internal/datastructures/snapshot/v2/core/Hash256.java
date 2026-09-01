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

package com.hazelcast.cp.internal.datastructures.snapshot.v2.core;

import java.io.Serializable;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;

/**
 * Truly immutable 32-byte SHA-256 hash value class with constant-time equality comparison.
 */
public final class Hash256 implements Serializable {

    public static final int HASH_LENGTH = 32;
    private static final long serialVersionUID = 1L;

    private final byte[] bytes;

    public Hash256(byte[] bytes) {
        if (bytes == null || bytes.length != HASH_LENGTH) {
            throw new IllegalArgumentException("Hash256 requires exactly " + HASH_LENGTH + " bytes");
        }
        this.bytes = bytes.clone();
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    public byte[] asBytes() {
        return bytes.clone();
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public boolean constantTimeEquals(Hash256 other) {
        if (other == null) {
            return false;
        }
        return MessageDigest.isEqual(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Hash256 hash256 = (Hash256) o;
        return constantTimeEquals(hash256);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return HexFormat.of().formatHex(bytes);
    }
}
