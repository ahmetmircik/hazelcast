/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.benchmark.serialization;

import java.util.Random;

abstract class Factory {
    final Random rand = new Random();

    protected Object createAndSetValues(int id) {
        final long offset = rand.nextLong();
        final double multiplier = rand.nextDouble();

        SampleObject object = create(id);
        object.shortVal = (short) offset;
        object.floatVal = (float) (multiplier * offset);

        byte[] byteArr = new byte[4096];
        rand.nextBytes(byteArr);
        object.byteArr = byteArr;

        long[] longArr = new long[3000];
        for (int i = 0; i < longArr.length; i++) {
            longArr[i] = i + offset;
        }
        object.longArr = longArr;

        double[] dblArr = new double[3000];
        for (int i = 0; i < dblArr.length; i++) {
            dblArr[i] = multiplier * (i + offset);
        }
        object.dblArr = dblArr;

        object.str = offset + " sample " + object.floatVal + " string " + object.intVal + " object";

        return object;
    }

    abstract SampleObject create(int intVal);
}
