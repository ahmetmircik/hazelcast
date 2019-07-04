//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapMessageType;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.serialization.Data;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

@SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public final class MapPutAllCodec2 {
    public static final MapMessageType REQUEST_TYPE;
    public static final int RESPONSE_TYPE = 100;

    public MapPutAllCodec2() {
    }

    public static ClientMessage encodeRequest(String name, List<Data> entries) {
        int requiredDataSize = MapPutAllCodec2.RequestParameters.calculateDataSize(name, entries);
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(REQUEST_TYPE.id());
        clientMessage.setRetryable(false);
        clientMessage.setAcquiresResource(false);
        clientMessage.setOperationName("Map.putAll");
        clientMessage.set(name);
        clientMessage.set(entries.size() / 2);

        for (int i = 0; i < entries.size(); i += 2) {
            Data key = entries.get(i);
            Data value = entries.get(i + 1);

            clientMessage.set(key);
            clientMessage.set(value);
        }

        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static MapPutAllCodec2.RequestParameters decodeRequest(ClientMessage clientMessage) {
        MapPutAllCodec2.RequestParameters parameters = new MapPutAllCodec2.RequestParameters();
        String name = null;
        name = clientMessage.getStringUtf8();
        parameters.name = name;
        List<Entry<Data, Data>> entries = null;
        int entries_size = clientMessage.getInt();
        entries = new ArrayList(entries_size);

        for (int entries_index = 0; entries_index < entries_size; ++entries_index) {
            Data entries_item_key = clientMessage.getData();
            Data entries_item_val = clientMessage.getData();
            Entry<Data, Data> entries_item = new SimpleEntry(entries_item_key, entries_item_val);
            entries.add(entries_item);
        }

        parameters.entries = entries;
        return parameters;
    }

    public static ClientMessage encodeResponse() {
        int requiredDataSize = MapPutAllCodec2.ResponseParameters.calculateDataSize();
        ClientMessage clientMessage = ClientMessage.createForEncode(requiredDataSize);
        clientMessage.setMessageType(100);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    public static MapPutAllCodec2.ResponseParameters decodeResponse(ClientMessage clientMessage) {
        MapPutAllCodec2.ResponseParameters parameters = new MapPutAllCodec2.ResponseParameters();
        return parameters;
    }

    static {
        REQUEST_TYPE = MapMessageType.MAP_PUTALL;
    }

    public static class ResponseParameters {
        public ResponseParameters() {
        }

        public static int calculateDataSize() {
            int dataSize = ClientMessage.HEADER_SIZE;
            return dataSize;
        }
    }

    public static class RequestParameters {
        public static final MapMessageType TYPE;
        public String name;
        public List<Entry<Data, Data>> entries;

        public RequestParameters() {
        }

        public static int calculateDataSize(String name, Collection<Data> entries) {
            int dataSize = ClientMessage.HEADER_SIZE;
            dataSize += ParameterUtil.calculateDataSize(name);
            dataSize += 4;

            for (Data data : entries) {
                dataSize += ParameterUtil.calculateDataSize(data);
            }

            return dataSize;
        }

        static {
            TYPE = MapPutAllCodec2.REQUEST_TYPE;
        }
    }
}
