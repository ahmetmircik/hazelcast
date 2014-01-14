package com.hazelcast.disk.core;

import com.hazelcast.disk.helper.MurmurHash3;
import com.hazelcast.disk.helper.SipHashInline;

/**
 * @author: ahmetmircik
 * Date: 12/23/13
 */
public interface Hasher<Key, Hash> {

    Hash hash(Key key);

    public static final class DataHasher implements Hasher<byte[], Integer> {
        @Override
        public Integer hash(byte[] bytes) {
            return MurmurHash3.murmurhash3x8632(bytes, 0, bytes.length, 271);
        }
    }

    public static final class DataHasher2 implements Hasher<byte[], Long> {
        @Override
        public Long hash(byte[] bytes) {
            return SipHashInline.hash24(bytes);
        }
    }

    ;

}