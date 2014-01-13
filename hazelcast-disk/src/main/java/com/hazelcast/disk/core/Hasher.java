package com.hazelcast.disk.core;

import com.hazelcast.disk.helper.MurmurHash3;
import com.hazelcast.nio.serialization.Data;

/**
 * @author: ahmetmircik
 * Date: 12/23/13
 */
public interface Hasher<Key,Hash> {
     Hash hash(Key key);

     static final Hasher<Data,Integer> DATA_HASHER = new Hasher<Data, Integer>() {

         @Override
         public Integer hash(Data data) {
//             return SipHashInline.hash24(data.getBuffer());
             return  MurmurHash3.murmurhash3x8632(data.getBuffer(), 0, data.getBuffer().length, 271);
         }
     };
}
