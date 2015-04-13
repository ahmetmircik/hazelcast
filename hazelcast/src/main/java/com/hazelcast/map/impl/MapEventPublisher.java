package com.hazelcast.map.impl;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

/**
 * Helper methods for publishing events.
 *
 * @see MapEventPublisherSupport
 */
public interface MapEventPublisher {

    void publishWanReplicationUpdate(String mapName, EntryView entryView);

    void publishWanReplicationRemove(String mapName, Data key, long removeTime);

    void publishMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected);

    void publishEvent(Address caller, String mapName, EntryEventType eventType,
                      Data dataKey, Data dataOldValue, Data dataValue);

    void publishEvent(Address caller, String mapName, EntryEventType eventType, boolean synthetic,
                      Data dataKey, Data dataOldValue, Data dataValue);

    void publishMapPartitionLostEvent(Address caller, String mapName, int partitionId);

    /**
     * Only gives a hint which indicates that a map-wide operation has just been executed on this partition.
     * This method should not publish an event.
     */
    void hintMapEvent(Address caller, String mapName, EntryEventType eventType, int numberOfEntriesAffected, int partitionId);
}
