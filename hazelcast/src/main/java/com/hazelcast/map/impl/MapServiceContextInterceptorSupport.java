/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.map.MapInterceptor;

/**
 * Helper interceptor methods for {@link MapServiceContext}.
 */
public interface MapServiceContextInterceptorSupport {

    Object interceptGet(MapContainer mapContainer, Object value);

    void interceptAfterGet(MapContainer mapContainer, Object value);

    Object interceptPut(MapContainer mapContainer, Object oldValue, Object newValue);

    void interceptAfterPut(MapContainer mapContainer, Object newValue);

    Object interceptRemove(MapContainer mapContainer, Object value);

    void interceptAfterRemove(MapContainer mapContainer, Object value);

    String generateInterceptorId(String mapName, MapInterceptor interceptor);

    void addInterceptor(String id, String mapName, MapInterceptor interceptor);

    void removeInterceptor(String mapName, String id);

    boolean hasInterceptor(MapContainer mapContainer);
}
