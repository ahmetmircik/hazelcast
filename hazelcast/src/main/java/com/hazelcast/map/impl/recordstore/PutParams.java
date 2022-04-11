/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

public final class PutParams {

    private PutParams() {
    }

    public static final PutParams PUT_PARAMS = new PutParams()
            .setPutVanilla(true)
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(false);

    public static final PutParams PUT_IF_ABSENT_PARAMS = new PutParams()
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(true)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(false);


    public static final PutParams SET_PARAMS = new PutParams()
            .setPutVanilla(true)
            .setLoad(false)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(false);

    public static final PutParams PUT_TRANSIENT_PARAMS = new PutParams()
            .setPutVanilla(true)
            .setLoad(false)
            .setStore(false)
            .setPutIfAbsent(false)
            .setPutIfExists(false)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(false)
            .setPutTransient(true);


    public static final PutParams REPLACE_PARAMS = new PutParams()
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(true)
            .setPutIfEqual(false)
            .setPutFromLoad(false)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(false);

    public static final PutParams REPLACE_IF_SAME_PARAMS = new PutParams()
            .setLoad(true)
            .setStore(true)
            .setPutIfAbsent(false)
            .setPutIfExists(true)
            .setPutIfEqual(true)
            .setPutFromLoad(false)
            .setSetTtl(false)
            .setCheckIfLoaded(true)
            .setCountAsAccess(true)
            .setBackup(false);

    // params
    private boolean load;
    private boolean store;
    private boolean putVanilla;
    private boolean putIfAbsent;
    private boolean putIfExists;
    private boolean putIfEqual;
    private boolean putFromLoad;
    private boolean putTransient;
    private boolean setTtl;
    private boolean checkIfLoaded;
    private boolean countAsAccess;
    private boolean backup;

    public PutParams setLoad(boolean load) {
        this.load = load;
        return this;
    }

    public PutParams setStore(boolean store) {
        this.store = store;
        return this;
    }

    public PutParams setPutVanilla(boolean putVanilla) {
        this.putVanilla = putVanilla;
        return this;
    }

    public PutParams setPutIfAbsent(boolean putIfAbsent) {
        this.putIfAbsent = putIfAbsent;
        return this;
    }

    public PutParams setPutIfExists(boolean putIfExists) {
        this.putIfExists = putIfExists;
        return this;
    }

    public PutParams setPutIfEqual(boolean putIfEqual) {
        this.putIfEqual = putIfEqual;
        return this;
    }

    public PutParams setPutFromLoad(boolean putFromLoad) {
        this.putFromLoad = putFromLoad;
        return this;
    }

    public PutParams setPutTransient(boolean putTransient) {
        this.putTransient = putTransient;
        return this;
    }

    public PutParams setSetTtl(boolean setTtl) {
        this.setTtl = setTtl;
        return this;
    }

    public PutParams setCheckIfLoaded(boolean checkIfLoaded) {
        this.checkIfLoaded = checkIfLoaded;
        return this;
    }

    public PutParams setCountAsAccess(boolean countAsAccess) {
        this.countAsAccess = countAsAccess;
        return this;
    }

    public PutParams setBackup(boolean backup) {
        this.backup = backup;
        return this;
    }

    public boolean isLoad() {
        return load;
    }

    public boolean isStore() {
        return store;
    }

    public boolean isPutIfAbsent() {
        return putIfAbsent;
    }

    public boolean isPutVanilla() {
        return putVanilla;
    }

    public boolean isPutIfExists() {
        return putIfExists;
    }

    public boolean isPutIfEqual() {
        return putIfEqual;
    }

    public boolean isPutFromLoad() {
        return putFromLoad;
    }

    public boolean isPutTransient() {
        return putTransient;
    }

    public boolean isSetTtl() {
        return setTtl;
    }

    public boolean isCheckIfLoaded() {
        return checkIfLoaded;
    }

    public boolean isCountAsAccess() {
        return countAsAccess;
    }

    public boolean isBackup() {
        return backup;
    }
}
