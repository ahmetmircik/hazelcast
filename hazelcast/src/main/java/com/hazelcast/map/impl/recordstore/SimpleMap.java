/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

public class SimpleMap<K, V> {

    private static final int DEFAULT_CAPACITY = 256;
    private static final float DEFAULT_LOAD_FACTOR = 0.55f;

    private int size = 0;
    private int resizeThreshold;
    private float loadFactor = DEFAULT_LOAD_FACTOR;

    private Object[] keyValuePairs;

    public SimpleMap() {
        capacity(findNextPositivePowerOfTwo(Math.max(DEFAULT_CAPACITY, DEFAULT_CAPACITY)));
    }

    public Object get(K key) {
        return getMapped(key);
    }

    private V getMapped(final Object key) {
        final Object[] entries = this.keyValuePairs;
        final int mask = entries.length - 1;
        int index = evenHash(key.hashCode(), mask);

        Object value = null;
        while (entries[index + 1] != null) {
            if (entries[index] == key || entries[index].equals(key)) {
                value = entries[index + 1];
                break;
            }

            index = next(index, mask);
        }

        return (V) value;
    }

    public Object put(K key, V value) {
        final Object[] entries = this.keyValuePairs;
        final int mask = entries.length - 1;
        int index = evenHash(key.hashCode(), mask);
        Object oldValue = null;

        while (entries[index + 1] != null) {
            if (entries[index] == key || entries[index].equals(key)) {
                oldValue = entries[index + 1];
                break;
            }

            index = next(index, mask);
        }

        if (oldValue == null) {
            ++size;
            entries[index] = key;
        }

        entries[index + 1] = value;

        increaseCapacity();
        return oldValue;
    }

    public Object remove(K key) {
        final Object[] entries = this.keyValuePairs;
        final int mask = entries.length - 1;
        int keyIndex = evenHash(key.hashCode(), mask);

        Object oldValue = null;
        while (entries[keyIndex + 1] != null) {
            if (entries[keyIndex] == key || entries[keyIndex].equals(key)) {
                oldValue = entries[keyIndex + 1];
                entries[keyIndex] = null;
                entries[keyIndex + 1] = null;
                size--;

                compactChain(keyIndex);

                break;
            }
            keyIndex = next(keyIndex, mask);
        }

        return oldValue;
    }

    private void compactChain(int deleteKeyIndex) {
        final Object[] keyValuePairs = this.keyValuePairs;
        final int mask = keyValuePairs.length - 1;
        int keyIndex = deleteKeyIndex;

        while (true) {
            keyIndex = next(keyIndex, mask);
            if (keyValuePairs[keyIndex + 1] == null) {
                break;
            }

            final int hash = evenHash(keyValuePairs[keyIndex].hashCode(), mask);

            if ((keyIndex < hash && (hash <= deleteKeyIndex || deleteKeyIndex <= keyIndex)) ||
                    (hash <= deleteKeyIndex && deleteKeyIndex <= keyIndex)) {
                keyValuePairs[deleteKeyIndex] = keyValuePairs[keyIndex];
                keyValuePairs[deleteKeyIndex + 1] = keyValuePairs[keyIndex + 1];

                keyValuePairs[keyIndex] = null;
                keyValuePairs[keyIndex + 1] = null;
                deleteKeyIndex = keyIndex;
            }
        }
    }

    public int size() {
        return size;
    }

    private static int findNextPositivePowerOfTwo(final int value) {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }

    private void capacity(final int newCapacity) {
        final int entriesLength = newCapacity * 2;
        if (entriesLength < 0) {
            throw new IllegalStateException("max capacity reached at size=" + size);
        }

        resizeThreshold = (int) (newCapacity * loadFactor);
        keyValuePairs = new Object[entriesLength];
    }

    private static int evenHash(final int value, final int mask) {
        final int hash = (value << 1) - (value << 8);

        return hash & mask;
    }

    private void increaseCapacity() {
        if (size > resizeThreshold) {
            // keyValuePairs.length = 2 * capacity
            final int newCapacity = keyValuePairs.length;
            rehash(newCapacity);
        }
    }

    private void rehash(final int newCapacity) {
        final Object[] oldEntries = keyValuePairs;
        final int length = keyValuePairs.length;

        capacity(newCapacity);

        final Object[] newEntries = keyValuePairs;
        final int mask = keyValuePairs.length - 1;

        for (int keyIndex = 0; keyIndex < length; keyIndex += 2) {
            final Object value = oldEntries[keyIndex + 1];
            if (value != null) {
                final Object key = oldEntries[keyIndex];
                int index = evenHash(key.hashCode(), mask);

                while (newEntries[index + 1] != null) {
                    index = next(index, mask);
                }

                newEntries[index] = key;
                newEntries[index + 1] = value;
            }
        }
    }

    private static int next(final int index, final int mask) {
        return (index + 2) & mask;
    }
}