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
